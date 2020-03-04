package badger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/fxamacker/cbor"
	log "github.com/go-kit/kit/log"
	"github.com/golang/geo/s2"

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
	*badger.DB
	logger log.Logger
}

// NewStorage returns a cold storage using leveldb
func NewStorage(path string, logger log.Logger) (*Storage, func() error, error) {
	// Creating DB
	opts := badger.LSMOnlyOptions(path)
	db, err := badger.Open(opts)
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
	opts := badger.LSMOnlyOptions(path)
	opts.ValueLogLoadingMode = options.FileIO
	opts.ReadOnly = true
	//opts.KeepL0InMemory = false
	opts.Logger = nil
	db, err := badger.Open(opts)
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
	fs := &insideout.FeatureStorage{}
	err := s.View(func(txn *badger.Txn) error {
		k := insideout.FeatureKey(id)
		item, err := txn.Get(k)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("feature id not found: %d", id)
			}
			return err
		}
		err = item.Value(func(v []byte) error {
			dec := cbor.NewDecoder(bytes.NewReader(v))
			return dec.Decode(fs)
		})
		if err != nil {
			return err
		}
		return nil
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
	err := s.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte{insideout.FeaturePrefix()}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			id := binary.BigEndian.Uint32(key[1:])

			err := item.Value(func(v []byte) error {
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
				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	})

	return err
}

// LoadFeaturesCells loads CellsStorage from DB into idx
// only useful to fill in memory tree indexes
func (s *Storage) LoadFeaturesCells(add func(*insideout.CellsStorage, uint32)) error {
	err := s.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{insideout.CellPrefix()}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			// read back FeatureStorage
			id := binary.BigEndian.Uint32(key[1:])
			if err := item.Value(func(v []byte) error {
				dec := cbor.NewDecoder(bytes.NewReader(v))
				cs := &insideout.CellsStorage{}
				if err := dec.Decode(cs); err != nil {
					return err
				}

				add(cs, id)
				return nil
			}); err != nil {
				return err
			}

		}
		return nil
	})
	return err
}

// LoadMapInfos loads map infos from the DB if any
func (s *Storage) LoadMapInfos() (*insideout.MapInfos, bool, error) {
	var b []byte
	err := s.View(func(txn *badger.Txn) error {
		item, err := txn.Get(insideout.MapKey())
		if err != nil {
			return err
		}
		b, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	dec := cbor.NewDecoder(bytes.NewReader(b))
	mapInfos := &insideout.MapInfos{}
	if err = dec.Decode(mapInfos); err != nil {
		return nil, false, err
	}

	return mapInfos, true, nil
}

// LoadIndexInfos loads index infos from the DB
func (s *Storage) LoadIndexInfos() (*insideout.IndexInfos, error) {
	var b []byte
	err := s.View(func(txn *badger.Txn) error {
		item, err := txn.Get(insideout.InfoKey())
		if err != nil {
			return err
		}
		b, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, errors.New("can't find infos entries, invalid DB")
		}
		return nil, err
	}

	dec := cbor.NewDecoder(bytes.NewReader(b))
	infos := &insideout.IndexInfos{}
	if err = dec.Decode(infos); err != nil {
		return nil, err
	}

	return infos, nil
}

// LoadCellStorage loads cell storage from
func (s *Storage) LoadCellStorage(id uint32) (*insideout.CellsStorage, error) {
	// get the s2 cells from the index
	var b []byte
	err := s.View(func(txn *badger.Txn) error {
		item, err := txn.Get(insideout.CellKey(id))
		if err != nil {
			return err
		}
		b, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	dec := cbor.NewDecoder(bytes.NewReader(b))
	cs := &insideout.CellsStorage{}
	if err := dec.Decode(cs); err != nil {
		return nil, err
	}
	return cs, nil
}
