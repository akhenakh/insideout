package pogreb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/akrylysov/pogreb"
	"github.com/fxamacker/cbor"
	log "github.com/go-kit/kit/log"
	"github.com/golang/geo/s2"
	"github.com/syndtr/goleveldb/leveldb"

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
	*pogreb.DB
	logger log.Logger
}

// NewStorage returns a cold storage using pogreb
func NewStorage(path string, logger log.Logger) (*Storage, func() error, error) {
	// Creating DB
	db, err := pogreb.Open(path, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to created DB at %s: %w", path, err)
	}

	return &Storage{
		DB:     db,
		logger: logger,
	}, db.Close, nil
}

// NewROStorage returns a read only storage using pogreb
func NewROStorage(path string, logger log.Logger) (*Storage, func() error, error) {
	// Creating DB
	db, err := pogreb.Open(path, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open DB for reading at %s: %w", path, err)
	}

	return &Storage{
		DB:     db,
		logger: logger,
	}, db.Close, nil
}

// LoadFeature loads one feature from the DB
func (s *Storage) LoadFeature(id uint32) (*insideout.Feature, error) {
	k := insideout.FeatureKey(id)
	v, err := s.Get(k)
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
	it := s.Items()
	for {
		// read back FeatureStorage
		key, val, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return err
			}
			break
		}
		// we only want keys starting with feature prefix
		if key[0] != insideout.FeaturePrefix() {
			continue
		}
		id := binary.BigEndian.Uint32(key[1:])
		dec := cbor.NewDecoder(bytes.NewReader(val))
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
}

// LoadFeaturesCells loads CellsStorage from DB into idx
// only useful to fill in memory tree indexes
func (s *Storage) LoadFeaturesCells(add func([]s2.CellUnion, []s2.CellUnion, uint32)) error {
	it := s.Items()
	for {
		// read back FeatureStorage
		key, val, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return err
			}
			break
		}
		// we only want keys starting with feature prefix
		if key[0] != insideout.CellPrefix() {
			continue
		}
		id := binary.BigEndian.Uint32(key[1:])
		dec := cbor.NewDecoder(bytes.NewReader(val))
		cs := &insideout.CellsStorage{}
		if err := dec.Decode(cs); err != nil {
			return err
		}

		add(cs.CellsIn, cs.CellsOut, id)
	}
	return nil
}

// LoadMapInfos loads map infos from the DB if any
func (s *Storage) LoadMapInfos() (*insideout.MapInfos, bool, error) {
	v, err := s.Get(insideout.MapKey())
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
	v, err := s.Get(insideout.InfoKey())
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
	v, err := s.Get(insideout.CellKey(id))
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

	return idxResp, errors.New("not implemented")
}
