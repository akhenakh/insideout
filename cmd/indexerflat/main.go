package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"

	"github.com/akhenakh/oureadb/index/geodata"
	"github.com/fxamacker/cbor"
	"github.com/golang/geo/s2"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/twpayne/go-geom/encoding/geojson"
)

type SIndex struct {
	Properties map[string]interface{}
	LoopBytes  []byte
}

const (
	maxCells = 20000
	level    = 15
)

func main() {
	var fc geojson.FeatureCollection

	file, err := os.Open("../../communes.geojson")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&fc)
	if err != nil {
		log.Fatal(err)
	}

	icoverer := &s2.RegionCoverer{MaxLevel: level, MinLevel: level, MaxCells: maxCells}
	ocoverer := &s2.RegionCoverer{MaxLevel: level, MinLevel: level, MaxCells: maxCells}

	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}
	db, err := leveldb.OpenFile("out.db", o)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	gd := &geodata.GeoData{}

	// // Start indefinite length array encoding.
	// if err := enc.StartIndefiniteArray(); err != nil {
	// 	log.Fatal(err)
	// }

	var count uint32
	for _, f := range fc.Features {
		b := new(bytes.Buffer)
		enc := cbor.NewEncoder(b, cbor.CanonicalEncOptions())

		err = geodata.GeoJSONFeatureToGeoData(f, gd)
		if err != nil {
			log.Println(err, f.Properties)
			continue
		}
		cui, err := gd.InteriorCover(icoverer)
		if err != nil {
			log.Println(err, f.Properties)
			continue
		}

		batch := new(leveldb.Batch)

		cuo, err := gd.Cover(ocoverer)
		if err != nil {
			log.Println(err, f.Properties)
			continue
		}

		var outside s2.CellUnion
		for _, c := range cuo {
			if !cui.ContainsCellID(c) {
				outside = append(outside, c)
			}
		}

		// index interior cover
		for _, c := range cui {
			v := make([]byte, 4)
			binary.BigEndian.PutUint32(v, count)
			// append to existing if any
			ev, err := db.Get(InsideKey(c), nil)
			if err != nil {
				if err != leveldb.ErrNotFound {
					log.Fatal(err)
				}
			}
			v = append(v, ev...)
			batch.Put(InsideKey(c), v)
		}

		// index outside cover
		for _, c := range outside {
			v := make([]byte, 4)
			binary.BigEndian.PutUint32(v, count)
			// append to existing if any
			ev, err := db.Get(OutsideKey(c), nil)
			if err != nil {
				if err != leveldb.ErrNotFound {
					log.Fatal(err)
				}
			}
			v = append(v, ev...)
			batch.Put(OutsideKey(c), v)
		}

		err = db.Write(batch, nil)
		if err != nil {
			log.Fatal(err)
		}

		if len(cui) > maxCells || len(outside) > maxCells {
			log.Println("cover too big", f, len(cui))
			continue
		}

		log.Println(f.Properties, len(cui), len(outside))
		lb := new(bytes.Buffer)
		l := geodata.LoopFromCoordinates(f.Geometry.FlatCoords())
		err = l.Encode(lb)
		if err != nil {
			log.Fatal(err)
		}
		i := &SIndex{Properties: f.Properties, LoopBytes: lb.Bytes()}

		// Encode array element.
		if err := enc.Encode(i); err != nil {
			log.Fatal(err)
		}

		// store feature
		err = db.Put(FeatureKey(count), b.Bytes(), nil)
		if err != nil {
			log.Fatal(err)
		}

		count++
	}

	// if err := enc.EndIndefinite(); err != nil {
	// 	log.Fatal(err)
	// }

	err = db.CompactRange(util.Range{})
	if err != nil {
		log.Fatal(err)
	}

}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func InsideKey(c s2.CellID) []byte {
	k := make([]byte, 1+8)
	k[0] = 'I'
	binary.BigEndian.PutUint64(k[1:], uint64(c))
	return k
}

func OutsideKey(c s2.CellID) []byte {
	k := make([]byte, 1+8)
	k[0] = 'O'
	binary.BigEndian.PutUint64(k[1:], uint64(c))
	return k
}

func FeatureKey(id uint32) []byte {
	k := make([]byte, 1+4)
	k[0] = 'F'
	binary.BigEndian.PutUint32(k[1:], id)
	return k
}
