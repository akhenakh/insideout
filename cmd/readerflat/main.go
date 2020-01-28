package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"github.com/bluele/gcache"
	"github.com/fxamacker/cbor"
	"github.com/golang/geo/s2"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	level = 15
)

type SIndex struct {
	Properties map[string]interface{}
	LoopBytes  []byte
}

type Feature struct {
	*s2.Loop
	Properties map[string]interface{}
}

func loadFeature(db *leveldb.DB, id uint32) (*Feature, error) {
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

	l := &s2.Loop{}
	if err = l.Decode(bytes.NewReader(idx.LoopBytes)); err != nil {
		return nil, err
	}
	f := &Feature{
		Loop:       l,
		Properties: idx.Properties,
	}

	return f, nil
}

func main() {

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	o := &opt.Options{
		Filter:   filter.NewBloomFilter(10),
		ReadOnly: true,
	}

	db, err := leveldb.OpenFile("../../out.db", o)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// cache
	gc := gcache.New(100).ARC().LoaderFunc(func(key interface{}) (interface{}, error) {
		id := key.(uint32)
		return loadFeature(db, id)
	}).Build()
	/*
		// load loops
		iter := db.NewIterator(util.BytesPrefix([]byte("F")), &opt.ReadOptions{
			DontFillCache: true,
		})
		for iter.Next() {
			// read back feature
			key := iter.Key()
			id := binary.BigEndian.Uint32(key[1:])
			value := iter.Value()
			dec := cbor.NewDecoder(bytes.NewReader(value))
			idx := &SIndex{}
			if err = dec.Decode(idx); err != nil {
				log.Fatal(err)
			}

			l := &s2.Loop{}
			if err = l.Decode(bytes.NewReader(idx.LoopBytes)); err != nil {
				log.Fatal(err)
			}
			mprop[id] = &Feature{
				Loop:       l,
				Properties: idx.Properties,
			}

		}
		iter.Release()
		err = iter.Error()
	*/

	lat := 47.8
	t := time.Now()

	pipCount := 0

	for i := 0; i < 10_000; i++ {
		p := s2.PointFromLatLng(s2.LatLngFromDegrees(lat-(float64(i)/2_000), 2.2))

		c := s2.CellFromPoint(p).ID()
		c = c.Parent(level)

		v, err := db.Get(InsideKey(c), nil)
		if err != nil {
			if err != leveldb.ErrNotFound {
				log.Fatal(err)
			}
		}
		// we are inside
		if v != nil {
			continue
		}

		v, err = db.Get(OutsideKey(c), nil)
		if err != nil {
			if err == leveldb.ErrNotFound {
				fmt.Println("no solution outside empty")
				continue
			}
		}

		if len(v) > 0 {
			var pipres []uint32
			for i := 0; i < len(v); i += 4 {
				// do pip
				id := binary.BigEndian.Uint32(v[i:])
				li, err := gc.Get(id)
				if err != nil {
					log.Fatal(err)
				}
				l := li.(*Feature)

				//fmt.Println("LOOP TESTING", id, c)
				pipCount++
				if l.ContainsPoint(p) {
					pipres = append(pipres, id)
				}
			}
			if len(pipres) == 0 {
				fmt.Println("no solution", c)
				continue
			}
			//fmt.Println("from PIP", pipres)
		}
	}

	fmt.Println(time.Since(t), "pip", pipCount)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)

	time.Sleep(2 * time.Minute)
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
