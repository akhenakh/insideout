package main

import (
	"fmt"
	stdlog "log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	"github.com/bluele/gcache"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/geo/s2"
	"github.com/namsral/flag"

	"github.com/akhenakh/insideout"
	"github.com/akhenakh/insideout/index/shapeindex"
	"github.com/akhenakh/insideout/index/treeindex"
)

const appName = "reader"

var (
	version = "no version from LDFLAGS"

	cacheCount = flag.Int("cacheCount", 100, "Features count to cache")
	dbPath     = flag.String("dbPath", "out.db", "Database path")

	stopOnFirstFound = flag.Bool("stopOnFirstFound", false, "Stop in first feature found")
	strategy         = flag.String("strategy", "insidetree", "Strategy to use: insidetree|shapeindex|disk")
)

func main() {
	flag.Parse()

	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger, "caller", log.Caller(5), "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "app", appName)
	logger = level.NewFilter(logger, level.AllowAll())

	stdlog.SetOutput(log.NewStdlibAdapter(logger))

	level.Info(logger).Log("msg", "Starting app", "version", version)

	go func() {
		stdlog.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	storage, clean, err := insideout.NewLevelDBStorage(*dbPath, logger)
	if err != nil {
		level.Error(logger).Log("msg", "failed to open storage", "error", err, "db_path", *dbPath)
		os.Exit(2)
	}
	defer clean()

	infos, err := storage.LoadIndexInfos()
	if err != nil {
		level.Error(logger).Log("msg", "failed to read infos", "error", err)
		os.Exit(2)
	}

	level.Info(logger).Log("msg", "read index_infos", "feature_count", infos.FeatureCount)

	// cache
	gc := gcache.New(*cacheCount).ARC().LoaderFunc(func(key interface{}) (interface{}, error) {
		id := key.(uint32)
		return storage.LoadFeature(id)
	}).Build()

	var idx insideout.Index

	switch *strategy {
	case "insidetree":
		idx = treeindex.New(treeindex.Options{StopOnInsideFound: *stopOnFirstFound})
	case "shapeindex":
		idx = shapeindex.New()
	default:
		level.Error(logger).Log("msg", "unknown strategy", "error", err, "strategy", *strategy)
		os.Exit(2)
	}

	err = storage.LoadAllFeatures(idx)
	if err != nil {
		level.Error(logger).Log("msg", "failed to load feature from storage", "error", err, "strategy", *strategy)
		os.Exit(2)
	}

	idxResp := idx.Stab(47.8469, 5.4031)
	for _, fid := range idxResp.IDsInside {
		fi, err := gc.Get(fid.ID)
		if err != nil {
			stdlog.Fatal(err)
		}
		f := fi.(*insideout.Feature)
		stdlog.Println("Found inside ID", fid.ID, f.Properties, "loop #", fid.Pos)
	}

	for _, fid := range idxResp.IDsMayBeInside {
		fi, err := gc.Get(fid.ID)
		if err != nil {
			stdlog.Fatal(err)
		}
		f := fi.(*insideout.Feature)
		stdlog.Println("Testing PIP outside ID", fid.ID, f.Properties, "loop #", fid.Pos)
		l := f.Loops[fid.Pos]
		if l.ContainsPoint(s2.PointFromLatLng(s2.LatLngFromDegrees(47.8469, 5.4031))) {
			stdlog.Println("Found in PIP outside ID", fid.ID, f.Properties, "loop #", fid.Pos)
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
