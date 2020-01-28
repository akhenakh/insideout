package insideout

import (
	"bytes"
	"encoding/binary"
	"log"

	"github.com/fxamacker/cbor"
	"github.com/golang/geo/s2"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Index offers different strategy indexers to speed up queries
type Index interface {
	// Add si to the index using the id
	Add(si *SIndex, id uint32) error
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

// SIndex on disk index
type SIndex struct {
	Properties map[string]interface{}

	// Next entries are arrays since a multipolygon may contains multiple loop

	// Cells inside cover
	CellsIn []s2.CellUnion

	// Cells outside cover
	CellsOut []s2.CellUnion

	// LoopsBytes encoded with s2 Loop encoder
	LoopsBytes [][]byte
}

// Feature as in memory
type Feature struct {
	Loops      []*s2.Loop
	Properties map[string]interface{}
}

// LoadFeature loads one feature from the DB
func LoadFeature(db *leveldb.DB, id uint32) (*Feature, error) {
	k := FeatureKey(id)
	v, err := db.Get(k, nil)
	if err != nil {
		return nil, err
	}
	dec := cbor.NewDecoder(bytes.NewReader(v))
	idx := &SIndex{}
	if err = dec.Decode(idx); err != nil {
		log.Fatal(err)
	}

	loops := make([]*s2.Loop, len(idx.LoopsBytes))
	for i := 0; i < len(loops); i++ {
		l := &s2.Loop{}
		if err = l.Decode(bytes.NewReader(idx.LoopsBytes[i])); err != nil {
			return nil, err
		}
		loops[i] = l
	}
	f := &Feature{
		Loops:      loops,
		Properties: idx.Properties,
	}

	return f, nil
}

// LoadAllSIndex loads all SIndex from DB, only useful to fill indexes
func LoadAllSIndex(db *leveldb.DB, addFunc func(si *SIndex, id uint32)) error {
	iter := db.NewIterator(util.BytesPrefix([]byte{byte(featurePrefix)}), &opt.ReadOptions{
		DontFillCache: true,
	})
	for iter.Next() {
		// read back SIndex
		key := iter.Key()
		id := binary.BigEndian.Uint32(key[1:])
		v := iter.Value()
		dec := cbor.NewDecoder(bytes.NewReader(v))
		si := &SIndex{}
		if err := dec.Decode(si); err != nil {
			return err
		}

		addFunc(si, id)
	}
	iter.Release()
	return iter.Error()
}
