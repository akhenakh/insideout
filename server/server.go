package server

import (
	"os"

	"github.com/bluele/gcache"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/geo/s2"
	"google.golang.org/grpc/health"

	"github.com/akhenakh/insideout"
	"github.com/akhenakh/insideout/index/shapeindex"
	"github.com/akhenakh/insideout/index/treeindex"
)

// Server exposes indexes services
type Server struct {
	storage      *insideout.Storage
	logger       log.Logger
	cache        gcache.Cache
	healthServer *health.Server
	idx          insideout.Index
}

type Options struct {
	StopOnFirstFound bool
	CacheCount       int
	Strategy         string
}

func New(storage *insideout.Storage, logger log.Logger, healthServer *health.Server, opts Options) *Server {
	logger = log.With(logger, "component", "server")

	var idx insideout.Index

	switch opts.Strategy {
	case insideout.InsideTreeStrategy:
		treeidx := treeindex.New(treeindex.Options{StopOnInsideFound: opts.StopOnFirstFound})
		err := storage.LoadFeaturesCells(treeidx.Add)
		if err != nil {
			level.Error(logger).Log("msg", "failed to load cells from storage", "error", err, "strategy", opts.Strategy)
			os.Exit(2)
		}
		idx = treeidx
	case insideout.ShapeIndex:
		shapeidx := shapeindex.New()
		err := storage.LoadAllFeatures(shapeidx.Add)
		if err != nil {
			level.Error(logger).Log("msg", "failed to load feature from storage", "error", err, "strategy", opts.Strategy)
			os.Exit(2)
		}
		idx = shapeidx
	}

	// cache
	gc := gcache.New(opts.CacheCount).ARC().LoaderFunc(func(key interface{}) (interface{}, error) {
		id := key.(uint32)
		return storage.LoadFeature(id)
	}).Build()

	return &Server{
		storage:      storage,
		logger:       logger,
		cache:        gc,
		healthServer: healthServer,
		idx:          idx,
	}
}

// Stab returns features containing lat lng
func (s *Server) Stab(lat, lng float64) ([]*insideout.Feature, error) {
	var res []*insideout.Feature
	idxResp := s.idx.Stab(lat, lng)
	for _, fid := range idxResp.IDsInside {
		fi, err := s.cache.Get(fid.ID)
		if err != nil {
			return nil, err
		}
		f := fi.(*insideout.Feature)
		level.Debug(s.logger).Log("msg", "Found inside feature", "fid", fid.ID, "properties", f.Properties, "loop #", fid.Pos)
		res = append(res, f)
	}

	for _, fid := range idxResp.IDsMayBeInside {
		fi, err := s.cache.Get(fid.ID)
		if err != nil {
			return nil, err
		}
		f := fi.(*insideout.Feature)
		l := f.Loops[fid.Pos]
		if l.ContainsPoint(s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))) {
			level.Debug(s.logger).Log("msg", "Found outside + PIP feature", "fid", fid.ID, "properties", f.Properties, "loop #", fid.Pos)
			res = append(res, f)
		}
	}
	return res, nil
}
