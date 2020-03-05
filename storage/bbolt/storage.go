package bbolt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/fxamacker/cbor"
	log "github.com/go-kit/kit/log"
	"github.com/golang/geo/s2"
	"go.etcd.io/bbolt"

	"github.com/akhenakh/insideout"
)

var (
	featureStoragePool = sync.Pool{
		New: func() interface{} {
			return &insideout.FeatureStorage{}
		},
	}
)

// Storage cold storage
type Storage struct {
	*bbolt.DB
	logger        log.Logger
	minCoverLevel int
}

// NewStorage returns a cold storage using leveldb
func NewStorage(path string, logger log.Logger) (*Storage, func() error, error) {
	// Creating DB
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	return &Storage{
		DB:     db,
		logger: logger,
	}, db.Close, nil
}

// NewROStorage returns a read only storage using leveldb
func NewROStorage(path string, logger log.Logger) (*Storage, func() error, error) {
	// Creating DB
	db, err := bbolt.Open(path, 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open DB for reading at %s: %w", path, err)
	}

	s := &Storage{
		DB:     db,
		logger: logger,
	}

	infos, err := s.LoadIndexInfos()
	if err != nil {
		return nil, nil, err
	}
	s.minCoverLevel = infos.MinCoverLevel

	return s, db.Close, nil
}

// LoadFeature loads one feature from the DB
func (s *Storage) LoadFeature(id uint32) (*insideout.Feature, error) {
	fs := &insideout.FeatureStorage{}
	err := s.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("feature"))
		k := insideout.FeatureKey(id)
		v := b.Get(k)
		if v == nil {
			return fmt.Errorf("feature id not found: %d", id)
		}

		dec := cbor.NewDecoder(bytes.NewReader(v))
		return dec.Decode(fs)
	})
	if err != nil {
		return nil, err
	}

	loops := make([]*s2.Loop, len(fs.LoopsBytes))
	for i := 0; i < len(loops); i++ {
		l := &s2.Loop{}
		if err = l.Decode(bytes.NewReader(fs.LoopsBytes[i])); err != nil {
			return nil, err
		}
		loops[i] = l
	}
	f := &insideout.Feature{
		Loops:      loops,
		Properties: fs.Properties,
	}

	return f, nil
}

// LoadAllFeatures loads FeatureStorage from DB into idx
// only useful to fill in memory shapeindex
func (s *Storage) LoadAllFeatures(add func(*insideout.FeatureStorage, uint32) error) error {
	err := s.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte("feature")).Cursor()
		prefix := []byte{insideout.FeaturePrefix()}
		for key, value := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = c.Next() {
			id := binary.BigEndian.Uint32(key[1:])

			dec := cbor.NewDecoder(bytes.NewReader(value))
			fs := featureStoragePool.Get().(*insideout.FeatureStorage)
			if err := dec.Decode(fs); err != nil {
				featureStoragePool.Put(fs)
				return err
			}

			if err := add(fs, id); err != nil {
				featureStoragePool.Put(fs)
				return err
			}
			featureStoragePool.Put(fs)
		}
		return nil
	})
	return err
}

// LoadFeaturesCells loads CellsStorage from DB into idx
// only useful to fill in memory tree indexes
func (s *Storage) LoadFeaturesCells(add func([]s2.CellUnion, []s2.CellUnion, uint32)) error {
	err := s.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte("cell")).Cursor()
		prefix := []byte{insideout.CellPrefix()}

		for key, value := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = c.Next() {
			// read back FeatureStorage
			id := binary.BigEndian.Uint32(key[1:])
			dec := cbor.NewDecoder(bytes.NewReader(value))
			cs := &insideout.CellsStorage{}
			if err := dec.Decode(cs); err != nil {
				return err
			}

			add(cs.CellsIn, cs.CellsOut, id)
		}
		return nil
	})
	return err
}

// LoadMapInfos loads map infos from the DB if any
func (s *Storage) LoadMapInfos() (*insideout.MapInfos, bool, error) {
	var mapInfos *insideout.MapInfos
	err := s.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("map"))
		value := b.Get(insideout.MapKey())
		if value == nil {
			return nil
		}
		mapInfos = &insideout.MapInfos{}
		dec := cbor.NewDecoder(bytes.NewReader(value))
		if err := dec.Decode(mapInfos); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	if mapInfos == nil {
		return nil, false, nil
	}

	return mapInfos, true, nil
}

// LoadIndexInfos loads index infos from the DB
func (s *Storage) LoadIndexInfos() (*insideout.IndexInfos, error) {
	infos := &insideout.IndexInfos{}

	err := s.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("info"))
		value := b.Get(insideout.InfoKey())
		if value == nil {
			return errors.New("can't find infos entries, invalid DB")
		}
		dec := cbor.NewDecoder(bytes.NewReader(value))
		if err := dec.Decode(infos); err != nil {
			return err
		}
		return nil
	})

	return infos, err
}

// LoadCellStorage loads cell storage from
func (s *Storage) LoadCellStorage(id uint32) (*insideout.CellsStorage, error) {
	// get the s2 cells from the index
	cs := &insideout.CellsStorage{}
	err := s.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("cell"))
		v := b.Get(insideout.CellKey(id))
		dec := cbor.NewDecoder(bytes.NewReader(v))
		if err := dec.Decode(cs); err != nil {
			return err
		}
		return nil
	})

	return cs, err
}

func (s *Storage) StabDB(lat, lng float64, StopOnInsideFound bool) (insideout.IndexResponse, error) {
	var idxResp insideout.IndexResponse

	ll := s2.LatLngFromDegrees(lat, lng)
	p := s2.PointFromLatLng(ll)
	c := s2.CellIDFromLatLng(ll)
	cLookup := s2.CellFromPoint(p).ID().Parent(s.minCoverLevel)
	mi := make(map[insideout.FeatureIndexResponse]struct{})

	startKey, stopKey := insideout.InsideRangeKeys(cLookup)
	err := s.View(func(tx *bbolt.Tx) error {
		curs := tx.Bucket([]byte("cell")).Cursor()
		for k, v := curs.Seek(startKey); k != nil && bytes.Compare(k, stopKey) <= 0; k, v = curs.Next() {

			cr := s2.CellID(binary.BigEndian.Uint64(k[1:]))
			if !cr.Contains(c) {
				continue
			}
			// read back the feature id and polygon index uint32 + uint16
			for i := 0; i < len(v); i += 4 + 2 {
				res := insideout.FeatureIndexResponse{}
				res.ID = binary.BigEndian.Uint32(v[i : i+4])
				res.Pos = binary.BigEndian.Uint16(v[i+4:])
				mi[res] = struct{}{}
				if StopOnInsideFound {
					idxResp.IDsInside = append(idxResp.IDsInside, res)
					return nil
				}
			}
		}
		return nil
	})
	if err != nil {
		return idxResp, err
	}

	if len(idxResp.IDsInside) > 0 && StopOnInsideFound {
		return idxResp, nil
	}

	// dedup
	for res := range mi {
		idxResp.IDsInside = append(idxResp.IDsInside, res)
	}

	startKey, stopKey = insideout.OutsideRangeKeys(cLookup)
	mo := make(map[insideout.FeatureIndexResponse]struct{})

	err = s.View(func(tx *bbolt.Tx) error {
		curs := tx.Bucket([]byte("cell")).Cursor()
		for k, v := curs.Seek(startKey); k != nil && bytes.Compare(k, stopKey) <= 0; k, v = curs.Next() {
			cr := s2.CellID(binary.BigEndian.Uint64(k[1:]))
			if !cr.Contains(c) {
				continue
			}
			// read back the feature id and polygon index uint32 + uint16
			for i := 0; i < len(v); i += 4 + 2 {
				res := insideout.FeatureIndexResponse{}
				res.ID = binary.BigEndian.Uint32(v[i : i+4])
				res.Pos = binary.BigEndian.Uint16(v[i+4:])
				// remove any answer matching inside
				if _, ok := mi[res]; !ok {
					mo[res] = struct{}{}
				}
			}
		}
		return nil
	})
	if err != nil {
		return idxResp, err
	}

	// dedup
	for res := range mo {
		idxResp.IDsMayBeInside = append(idxResp.IDsMayBeInside, res)
	}

	return idxResp, nil
}
