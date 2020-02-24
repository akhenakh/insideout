package insideout

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fxamacker/cbor"
	"github.com/go-kit/kit/log"
	"github.com/golang/geo/s2"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// FeatureStorage on disk storage of the feature
type FeatureStorage struct {
	Properties map[string]interface{}

	// Next entries are arrays since a multipolygon may contains multiple loop
	// LoopsBytes encoded with s2 Loop encoder
	LoopsBytes [][]byte
}

// CellsStorage are used to store indexed cells
// for use with the treeindex
type CellsStorage struct {
	// Cells inside cover
	CellsIn []s2.CellUnion

	// Cells outside cover
	CellsOut []s2.CellUnion
}

// Storage cold storage
type Storage struct {
	*leveldb.DB
	logger log.Logger
}

// IndexInfos used to store information about the index in DB
type IndexInfos struct {
	Filename       string
	IndexTime      time.Time
	IndexerVersion string
	FeatureCount   uint32
	MinCoverLevel  int
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
		if err == leveldb.ErrNotFound {
			return nil, fmt.Errorf("feature id not found: %d", id)
		}
		return nil, err
	}
	dec := cbor.NewDecoder(bytes.NewReader(v))
	fs := &FeatureStorage{}
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

// LoadAllFeatures loads FeatureStorage from DB into idx
// only useful to fill in memory shapeindex
func (s *Storage) LoadAllFeatures(add func(*FeatureStorage, uint32) error) error {
	iter := s.NewIterator(util.BytesPrefix([]byte{featurePrefix}), &opt.ReadOptions{
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
func (s *Storage) LoadFeaturesCells(add func(*CellsStorage, uint32)) error {
	iter := s.NewIterator(util.BytesPrefix([]byte{cellPrefix}), &opt.ReadOptions{
		DontFillCache: true,
	})
	for iter.Next() {
		// read back FeatureStorage
		key := iter.Key()
		id := binary.BigEndian.Uint32(key[1:])
		v := iter.Value()
		dec := cbor.NewDecoder(bytes.NewReader(v))
		cs := &CellsStorage{}
		if err := dec.Decode(cs); err != nil {
			return err
		}

		add(cs, id)
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

// TilesHandler serves the mbtiles at /api/debug/tiles/11/618/722.pbf
func (s *Storage) TilesHandler(w http.ResponseWriter, req *http.Request) {
	sp := strings.Split(req.URL.Path, "/")

	if len(sp) != 6 {
		http.Error(w, "Invalid query", http.StatusBadRequest)
		return
	}
	z, _ := strconv.Atoi(sp[3])
	x, _ := strconv.Atoi(sp[4])
	y, _ := strconv.Atoi(strings.Trim(sp[5], ".pbf"))

	data, err := s.ReadTileData(uint8(z), uint64(x), uint64(1<<uint(z)-y-1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(data) == 0 {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "gzip")
	_, _ = w.Write(data)
}

// ReadTileData returns []bytes from a tile
func (s *Storage) ReadTileData(z uint8, x uint64, y uint64) ([]byte, error) {
	k := []byte(fmt.Sprintf("%c%d/%d/%d", TilesURLPrefix, z, x, y))
	v, err := s.Get(k, nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	tk := []byte{TilesPrefix}
	tk = append(tk, v...)
	v, err = s.Get(tk, nil)
	if err == leveldb.ErrNotFound {
		return nil, errors.New("can't find blob at existing entry")
	}
	return v, err
}

func (infos *IndexInfos) String() string {
	return fmt.Sprintf("Filename: %s\nIndexTime: %s\nIndexerVersion: %s\nFeatureCount %d\n",
		infos.Filename,
		infos.IndexTime,
		infos.IndexerVersion,
		infos.FeatureCount,
	)
}
