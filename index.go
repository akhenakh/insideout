package insideout

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fxamacker/cbor"
	"github.com/go-kit/kit/log"
	"github.com/golang/geo/s2"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	featureStoragePool = sync.Pool{
		New: func() interface{} {
			return &FeatureStorage{}
		},
	}
)

// Index offers different strategy indexers to speed up queries
type Index interface {
	// Add si to the index using the id
	Add(si *FeatureStorage, id uint32) error
	// Stab returns ids of polygon we are inside and polygons we may be inside
	Stab(lat, lng float64) IndexResponse
}

// IndexResponse a response to find back a feature from an index
type IndexResponse struct {
	IDsInside      []FeatureIndexResponse
	IDsMayBeInside []FeatureIndexResponse
}

// FeatureIndexResponse a feature response to find back a feature from an index
type FeatureIndexResponse struct {
	// id of the feature
	ID uint32
	// index of the polygon (in case of multipolygon)
	Pos uint16
}

// FeatureStorage on disk storage of the feature
type FeatureStorage struct {
	Properties map[string]interface{}

	// Next entries are arrays since a multipolygon may contains multiple loop

	// Cells inside cover
	CellsIn []s2.CellUnion

	// Cells outside cover
	CellsOut []s2.CellUnion

	// LoopsBytes encoded with s2 Loop encoder
	LoopsBytes [][]byte
}

// Storage cold storage
type Storage struct {
	*leveldb.DB
	logger log.Logger
}

// Feature representation in memory
type Feature struct {
	Loops      []*s2.Loop
	Properties map[string]interface{}
}

// IndexInfos used to store informations about the index in DB
type IndexInfos struct {
	Filename       string
	IndexTime      time.Time
	IndexerVersion string
	FeatureCount   uint32
}

// NewStorage returns a cold storage using leveldb
func NewLevelDBStorage(path string, logger log.Logger) (*Storage, func() error, error) {
	// Creating DB
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}
	db, err := leveldb.OpenFile(path, o)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to created DB at %s: %w", path, err)
	}

	return &Storage{
		DB:     db,
		logger: logger,
	}, db.Close, nil
}

// LoadFeature loads one feature from the DB
func (s *Storage) LoadFeature(id uint32) (*Feature, error) {
	k := FeatureKey(id)
	v, err := s.Get(k, nil)
	if err != nil {
		return nil, err
	}
	dec := cbor.NewDecoder(bytes.NewReader(v))
	fs := featureStoragePool.Get().(*FeatureStorage)
	defer featureStoragePool.Put(fs)
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
	f := &Feature{
		Loops:      loops,
		Properties: fs.Properties,
	}

	return f, nil
}

// LoadAllFeatures loads all FeatureStorage from DB into idx
// only useful to fill indexes
func (s *Storage) LoadAllFeatures(idx Index) error {
	iter := s.NewIterator(util.BytesPrefix([]byte{byte(featurePrefix)}), &opt.ReadOptions{
		DontFillCache: true,
	})
	for iter.Next() {
		// read back FeatureStorage
		key := iter.Key()
		id := binary.BigEndian.Uint32(key[1:])
		v := iter.Value()
		dec := cbor.NewDecoder(bytes.NewReader(v))
		fs := featureStoragePool.Get().(*FeatureStorage)
		if err := dec.Decode(fs); err != nil {
			featureStoragePool.Put(fs)
			return err
		}

		if err := idx.Add(fs, id); err != nil {
			featureStoragePool.Put(fs)
			return err
		}
		featureStoragePool.Put(fs)
	}
	iter.Release()
	return iter.Error()
}

// LoadIndexInfos loads index infos from the DB
func (s *Storage) LoadIndexInfos() (*IndexInfos, error) {
	v, err := s.Get(InfoKey(), &opt.ReadOptions{
		DontFillCache: true,
	})
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, errors.New("can't find infos entries, invalid DB")
		}
		return nil, err
	}
	dec := cbor.NewDecoder(bytes.NewReader(v))
	infos := &IndexInfos{}
	if err = dec.Decode(infos); err != nil {
		return nil, err
	}

	return infos, nil
}

func (infos *IndexInfos) String() string {
	return fmt.Sprintf("Filename: %s\nIndexTime: %s\nIndexerVersion: %s\nFeatureCount %d\n",
		infos.Filename,
		infos.IndexTime,
		infos.IndexerVersion,
		infos.FeatureCount,
	)
}
