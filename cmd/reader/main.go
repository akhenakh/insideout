package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/bluele/gcache"
	"github.com/golang/geo/s2"
	"github.com/namsral/flag"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/akhenakh/insideout"
	"github.com/akhenakh/insideout/index/shapeindex"
	"github.com/akhenakh/insideout/index/treeindex"
)

var (
	version = "no version from LDFLAGS"

	cacheCount = flag.Int("cacheCount", 100, "Features count to cache")
	dbPath     = flag.String("dbPath", "out.db", "Database path")

	stopOnFirstFound = flag.Bool("stopOnFirstFound", false, "Stop in first feature found")
	strategy         = flag.String("strategy", "insidetree", "Strategy to use: insidetree|shapeindex|disk")
)

func main() {
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	o := &opt.Options{
		Filter:   filter.NewBloomFilter(10),
		ReadOnly: true,
	}

	db, err := leveldb.OpenFile(*dbPath, o)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// cache
	gc := gcache.New(*cacheCount).ARC().LoaderFunc(func(key interface{}) (interface{}, error) {
		id := key.(uint32)
		return insideout.LoadFeature(db, id)
	}).Build()

	var idx insideout.Index

	switch *strategy {
	case "insidetree":
		idx = treeindex.New(treeindex.Options{StopOnInsideFound: *stopOnFirstFound})

		f := func(si *insideout.SIndex, id uint32) {
			err := idx.Add(si, id)
			if err != nil {
				log.Fatal(err)
			}
		}
		err := insideout.LoadAllSIndex(db, f)
		if err != nil {
			log.Fatal(err)
		}
	case "shapeindex":
		idx = shapeindex.New()

		f := func(si *insideout.SIndex, id uint32) {
			err := idx.Add(si, id)
			if err != nil {
				log.Fatal(err)
			}
		}
		err := insideout.LoadAllSIndex(db, f)
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("unknown strategy ", *strategy)
	}

	idxResp := idx.Stab(48.8, 2.2)
	for _, fid := range idxResp.IDsInside {
		fi, err := gc.Get(fid.ID)
		if err != nil {
			log.Fatal(err)
		}
		f := fi.(*insideout.Feature)
		log.Println("Found inside", f.Properties, "loop #", fid.Pos)
	}

	for _, fid := range idxResp.IDsMayBeInside {
		fi, err := gc.Get(fid.ID)
		if err != nil {
			log.Fatal(err)
		}
		f := fi.(*insideout.Feature)
		log.Println("Testing PIP outside", f.Properties, "loop #", fid.Pos)
		l := f.Loops[fid.Pos]
		if l.ContainsPoint(s2.PointFromLatLng(s2.LatLngFromDegrees(48.8, 2.2))) {
			log.Println("Found in PIP outside", f.Properties, "loop #", fid.Pos)
		}
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)

	//time.Sleep(2 * time.Minute)

}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
