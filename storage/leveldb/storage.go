package leveldb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/fxamacker/cbor"
	log "github.com/go-kit/kit/log"
	"github.com/golang/geo/s2"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

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
	*leveldb.DB
	logger        log.Logger
	minCoverLevel int
}

// NewStorage returns a cold storage using leveldb
func NewStorage(path string, logger log.Logger) (*Storage, func() error, error) {
	// Creating DB
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}
	db, err := leveldb.OpenFile(path, o)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to created DB at %s: %w", path, err)
	}

	s := &Storage{
		DB:     db,
		logger: logger,
	}

	return s, db.Close, nil
}

// NewROStorage returns a read only storage using leveldb
func NewROStorage(path string, logger log.Logger) (*Storage, func() error, error) {
	// Creating DB
	o := &opt.Options{
		Filter:   filter.NewBloomFilter(10),
		ReadOnly: true,
	}
	db, err := leveldb.OpenFile(path, o)
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
	k := insideout.FeatureKey(id)
	v, err := s.Get(k, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, fmt.Errorf("feature id not found: %d", id)
		}
		return nil, err
	}
	dec := cbor.NewDecoder(bytes.NewReader(v))
	fs := &insideout.FeatureStorage{}
	if err = dec.Decode(fs); err != nil {
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
	iter := s.NewIterator(util.BytesPrefix([]byte{insideout.FeaturePrefix()}), &opt.ReadOptions{
		DontFillCache: true,
	})
	for iter.Next() {
		// read back FeatureStorage
		key := iter.Key()
		id := binary.BigEndian.Uint32(key[1:])
		v := iter.Value()
		dec := cbor.NewDecoder(bytes.NewReader(v))
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
	iter.Release()
	return iter.Error()
}

// LoadFeaturesCells loads CellsStorage from DB into idx
// only useful to fill in memory tree indexes
func (s *Storage) LoadFeaturesCells(add func([]s2.CellUnion, []s2.CellUnion, uint32)) error {
	iter := s.NewIterator(util.BytesPrefix([]byte{insideout.CellPrefix()}), &opt.ReadOptions{
		DontFillCache: true,
	})
	for iter.Next() {
		// read back FeatureStorage
		key := iter.Key()
		id := binary.BigEndian.Uint32(key[1:])
		v := iter.Value()
		dec := cbor.NewDecoder(bytes.NewReader(v))
		cs := &insideout.CellsStorage{}
		if err := dec.Decode(cs); err != nil {
			return err
		}

		add(cs.CellsIn, cs.CellsOut, id)
	}
	iter.Release()
	return iter.Error()
}

// LoadMapInfos loads map infos from the DB if any
func (s *Storage) LoadMapInfos() (*insideout.MapInfos, bool, error) {
	v, err := s.Get(insideout.MapKey(), &opt.ReadOptions{
		DontFillCache: true,
	})
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	dec := cbor.NewDecoder(bytes.NewReader(v))
	mapInfos := &insideout.MapInfos{}
	if err = dec.Decode(mapInfos); err != nil {
		return nil, false, err
	}

	return mapInfos, true, nil
}

// LoadIndexInfos loads index infos from the DB
func (s *Storage) LoadIndexInfos() (*insideout.IndexInfos, error) {
	v, err := s.Get(insideout.InfoKey(), &opt.ReadOptions{
		DontFillCache: true,
	})
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, errors.New("can't find infos entries, invalid DB")
		}
		return nil, err
	}
	dec := cbor.NewDecoder(bytes.NewReader(v))
	infos := &insideout.IndexInfos{}
	if err = dec.Decode(infos); err != nil {
		return nil, err
	}

	return infos, nil
}

// LoadCellStorage loads cell storage from
func (s *Storage) LoadCellStorage(id uint32) (*insideout.CellsStorage, error) {
	// get the s2 cells from the index
	v, err := s.Get(insideout.CellKey(id), nil)
	if err != nil {
		return nil, err
	}

	dec := cbor.NewDecoder(bytes.NewReader(v))
	cs := &insideout.CellsStorage{}
	if err := dec.Decode(cs); err != nil {
		return nil, err
	}
	return cs, nil
}

func (s *Storage) StabDB(lat, lng float64, StopOnInsideFound bool) (insideout.IndexResponse, error) {
	var idxResp insideout.IndexResponse

	ll := s2.LatLngFromDegrees(lat, lng)
	p := s2.PointFromLatLng(ll)
	c := s2.CellIDFromLatLng(ll)
	cLookup := s2.CellFromPoint(p).ID().Parent(s.minCoverLevel)

	startKey, stopKey := insideout.InsideRangeKeys(cLookup)
	iter := s.NewIterator(&util.Range{Start: startKey, Limit: stopKey}, &opt.ReadOptions{
		DontFillCache: true,
	})
	defer iter.Release()

	mi := make(map[insideout.FeatureIndexResponse]struct{})

	for iter.Next() {
		k := iter.Key()
		cr := s2.CellID(binary.BigEndian.Uint64(k[1:]))
		if !cr.Contains(c) {
			continue
		}
		v := iter.Value()
		// read back the feature id and polygon index uint32 + uint16
		for i := 0; i < len(v); i += 4 + 2 {
			res := insideout.FeatureIndexResponse{}
			res.ID = binary.BigEndian.Uint32(v[i : i+4])
			res.Pos = binary.BigEndian.Uint16(v[i+4:])
			mi[res] = struct{}{}
			if StopOnInsideFound {
				idxResp.IDsInside = append(idxResp.IDsInside, res)
				return idxResp, nil
			}
		}
	}

	// dedup
	for res := range mi {
		idxResp.IDsInside = append(idxResp.IDsInside, res)
	}

	if err := iter.Error(); err != nil {
		return idxResp, err
	}

	startKey, stopKey = insideout.OutsideRangeKeys(cLookup)
	oiter := s.NewIterator(&util.Range{Start: startKey, Limit: stopKey}, &opt.ReadOptions{
		DontFillCache: true,
	})
	defer oiter.Release()

	mo := make(map[insideout.FeatureIndexResponse]struct{})

	for oiter.Next() {
		k := oiter.Key()
		cr := s2.CellID(binary.BigEndian.Uint64(k[1:]))
		if !cr.Contains(c) {
			continue
		}
		v := oiter.Value()
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

	// dedup
	for res := range mo {
		idxResp.IDsMayBeInside = append(idxResp.IDsMayBeInside, res)
	}

	if err := oiter.Error(); err != nil {
		return idxResp, err
	}

	return idxResp, nil
}
