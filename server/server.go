package server

import (
	"context"
	"fmt"
	"sort"

	"github.com/dgraph-io/ristretto"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/geo/s2"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/opentracing/opentracing-go"
	slog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/status"

	"github.com/akhenakh/insideout"
	"github.com/akhenakh/insideout/gen/go/insidesvc/v1"
	"github.com/akhenakh/insideout/index/dbindex"
	"github.com/akhenakh/insideout/index/shapeindex"
	"github.com/akhenakh/insideout/index/treeindex"
)

var (
	errorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "insided_server",
		Name:      "error_total",
		Help:      "The total number of errors occurring",
	})

	featureHitCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "insided_server",
		Name:      "feature_cache_hit_total",
		Help:      "Features cache hits",
	})

	featureMissCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "insided_server",
		Name:      "feature_miss_hit_total",
		Help:      "Features miss hits",
	})
)

// Server exposes indexes services.
type Server struct {
	storage      insideout.Store
	logger       log.Logger
	cache        *ristretto.Cache
	healthServer *health.Server
	idx          insideout.Index
}

type Options struct {
	StopOnFirstFound bool
	CacheCount       int
	Strategy         string
}

// New returns a Server.
func New(storage insideout.Store, logger log.Logger, healthServer *health.Server,
	opts Options) (*Server, error) {
	logger = log.With(logger, "component", "server")

	var idx insideout.Index

	switch opts.Strategy {
	case insideout.InsideTreeStrategy:
		treeidx := treeindex.New(treeindex.Options{StopOnInsideFound: opts.StopOnFirstFound})

		err := storage.LoadFeaturesCells(treeidx.Add)
		if err != nil {
			level.Error(logger).Log("msg", "failed to load cells from storage", "error", err, "strategy", opts.Strategy)

			return nil, fmt.Errorf("ailed to load cells from storage: %w", err)
		}

		idx = treeidx
	case insideout.ShapeIndexStrategy:
		shapeidx := shapeindex.New()

		err := storage.LoadAllFeatures(shapeidx.Add)
		if err != nil {
			level.Error(logger).Log("msg", "failed to load feature from storage", "error", err, "strategy", opts.Strategy)

			return nil, fmt.Errorf("failed to load feature from storage: %w", err)
		}

		idx = shapeidx
	case insideout.DBStrategy:
		dbidx := dbindex.New(storage, dbindex.Options{StopOnInsideFound: opts.StopOnFirstFound})
		idx = dbidx
	}

	s := &Server{
		storage:      storage,
		logger:       logger,
		healthServer: healthServer,
		idx:          idx,
	}

	// cache
	if opts.CacheCount > 0 {
		cache, err := ristretto.NewCache(&ristretto.Config{
			NumCounters: int64(opts.CacheCount) * 10, // number of keys to track frequency
			MaxCost:     int64(opts.CacheCount),      // maximum cost of cache
			BufferItems: 64,                          // number of keys per Get buffer.
		})
		if err != nil {
			return nil, fmt.Errorf("cache error: %w", err)
		}

		s.cache = cache
	}

	return s, nil
}

// feature fetch feature from cache or from storage.
func (s *Server) feature(id uint32) (*insideout.Feature, error) {
	if s.cache == nil {
		return s.storage.LoadFeature(id)
	}

	fi, found := s.cache.Get(id)
	if !found {
		lf, err := s.storage.LoadFeature(id)
		if err != nil {
			return nil, fmt.Errorf("error loading feature: %w", err)
		}

		s.cache.Set(id, lf, 1)
		featureMissCounter.Inc()

		return lf, nil
	}

	featureHitCounter.Inc()

	return fi.(*insideout.Feature), nil
}

// Within query exposed via gRPC.
func (s *Server) Within(
	ctx context.Context, req *insidesvc.WithinRequest,
) (resp *insidesvc.WithinResponse, terr error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "Within")
	defer span.Finish()

	defer s.handleError(terr, span)

	idxResp, err := s.idx.Stab(req.Lat, req.Lng)
	if err != nil {
		return nil, fmt.Errorf("stabbing error: %w", err)
	}

	level.Debug(s.logger).Log("msg", "querying within",
		"lat", req.Lat,
		"lng", req.Lng,
		"idx_resp", idxResp,
	)

	span.LogFields(
		slog.Float64("lat", req.Lat),
		slog.Float64("lng", req.Lng),
	)

	var fresps []*insidesvc.FeatureResponse

	for _, fid := range idxResp.IDsInside {
		f, err := s.feature(fid.ID)
		if err != nil {
			return nil, err
		}

		level.Debug(s.logger).Log("msg", "Found inside feature",
			"fid", fid.ID,
			"properties", f.Properties,
			"loop #", fid.Pos)

		feature := &insidesvc.Feature{}

		if !req.RemoveGeometries {
			l := f.Loops[fid.Pos]
			feature.Geometry = &insidesvc.Geometry{
				Type:        insidesvc.Geometry_TYPE_POLYGON,
				Coordinates: insideout.CoordinatesFromLoops(l),
			}
		}

		// TODO: filter properties
		prop, err := insideout.PropertiesToValues(f)
		if err != nil {
			return nil, fmt.Errorf("can't transfor property to value: %w", err)
		}

		feature.Properties = prop
		feature.Properties[insidesvc.LoopIndexProperty] = &structpb.Value{
			Kind: &structpb.Value_NumberValue{NumberValue: float64(fid.Pos)},
		}
		feature.Properties[insidesvc.FeatureIDProperty] = &structpb.Value{
			Kind: &structpb.Value_NumberValue{NumberValue: float64(fid.ID)},
		}

		fresp := &insidesvc.FeatureResponse{
			Id:      fid.ID,
			Feature: feature,
		}
		fresps = append(fresps, fresp)
	}

	p := s2.PointFromLatLng(s2.LatLngFromDegrees(req.Lat, req.Lng))

	for _, fid := range idxResp.IDsMayBeInside {
		f, err := s.feature(fid.ID)
		if err != nil {
			return nil, err
		}

		level.Debug(s.logger).Log("msg", "Found maybe inside feature",
			"fid", fid.ID,
			"properties", f.Properties,
			"loop #", fid.Pos)

		l := f.Loops[fid.Pos]
		if !l.ContainsPoint(p) {
			continue
		}

		level.Debug(s.logger).Log("msg", "Found maybe inside feature PIP valid",
			"fid", fid.ID,
			"properties", f.Properties,
			"loop #", fid.Pos)

		feature := &insidesvc.Feature{}

		if !req.RemoveGeometries {
			feature.Geometry = &insidesvc.Geometry{
				Type:        insidesvc.Geometry_TYPE_POLYGON,
				Coordinates: insideout.CoordinatesFromLoops(l),
			}
		}

		prop, err := insideout.PropertiesToValues(f)
		if err != nil {
			return nil, err
		}

		feature.Properties = prop
		feature.Properties[insidesvc.LoopIndexProperty] = &structpb.Value{
			Kind: &structpb.Value_NumberValue{NumberValue: float64(fid.Pos)},
		}
		feature.Properties[insidesvc.FeatureIDProperty] = &structpb.Value{
			Kind: &structpb.Value_NumberValue{NumberValue: float64(fid.ID)},
		}

		fresp := &insidesvc.FeatureResponse{
			Id:      fid.ID,
			Feature: feature,
		}
		fresps = append(fresps, fresp)
	}

	// sort features by "admin_level"
	sort.SliceStable(fresps, func(i, j int) bool {
		return fresps[i].Feature.Properties["admin_level"].GetNumberValue() <
			fresps[j].Feature.Properties["admin_level"].GetNumberValue()
	})

	level.Debug(s.logger).Log("msg", "result stab",
		"lat", req.Lat,
		"lng", req.Lng,
		"features_count", len(fresps))

	resp = &insidesvc.WithinResponse{
		Point: &insidesvc.Point{
			Lat: req.Lat,
			Lng: req.Lng,
		},
		Responses: fresps,
	}

	return resp, nil
}

func (s *Server) Get(ctx context.Context, req *insidesvc.GetRequest) (resp *insidesvc.GetResponse, terr error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "Get")
	defer span.Finish()

	defer s.handleError(terr, span)

	span.LogFields(
		slog.Uint32("feature_id", req.Id),
		slog.Uint32("loop_index", req.LoopIndex),
	)

	f, err := s.feature(req.Id)
	if err != nil {
		return nil, err
	}

	if f == nil {
		return nil, status.Error(codes.NotFound, "can't found feature")
	}

	if req.LoopIndex >= uint32(len(f.Loops)) {
		return nil, status.Error(codes.NotFound, "loop index out of range")
	}

	l := f.Loops[req.LoopIndex]

	prop, err := insideout.PropertiesToValues(f)
	if err != nil {
		return nil, err
	}

	feature := &insidesvc.Feature{
		Geometry: &insidesvc.Geometry{
			Type:        insidesvc.Geometry_TYPE_POLYGON,
			Coordinates: insideout.CoordinatesFromLoops(l),
		},
		Properties: prop,
	}

	feature.Properties[insidesvc.LoopIndexProperty] = &structpb.Value{
		Kind: &structpb.Value_NumberValue{NumberValue: float64(req.LoopIndex)},
	}
	feature.Properties[insidesvc.FeatureIDProperty] = &structpb.Value{
		Kind: &structpb.Value_NumberValue{NumberValue: float64(req.Id)},
	}
	resp.Id = req.Id
	resp.Feature = feature

	return resp, nil
}

// Stab returns features containing lat lng.
func (s *Server) IndexStab(lat, lng float64) ([]*insideout.Feature, error) {
	var res []*insideout.Feature

	idxResp, err := s.idx.Stab(lat, lng)
	if err != nil {
		return nil, fmt.Errorf("stabbing error: %w", err)
	}

	for _, fid := range idxResp.IDsInside {
		f, err := s.feature(fid.ID)
		if err != nil {
			return nil, err
		}

		level.Debug(s.logger).Log("msg", "Found inside feature",
			"fid", fid.ID,
			"properties", f.Properties,
			"loop #", fid.Pos)

		res = append(res, f)
	}

	for _, fid := range idxResp.IDsMayBeInside {
		f, err := s.feature(fid.ID)
		if err != nil {
			return nil, err
		}

		l := f.Loops[fid.Pos]
		if l.ContainsPoint(s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))) {
			level.Debug(s.logger).Log("msg", "Found outside + PIP feature",
				"fid", fid.ID,
				"properties", f.Properties,
				"loop #", fid.Pos)

			res = append(res, f)
		}
	}

	return res, nil
}

func (s *Server) handleError(terr error, span opentracing.Span) {
	if terr != nil {
		// do not log not found as error
		if status, ok := status.FromError(terr); ok && status.Code() == codes.NotFound {
			level.Debug(s.logger).Log("error", terr)

			return
		}

		errorCounter.Inc()
		span.LogFields(
			slog.String("error", terr.Error()),
		)
		span.SetTag("error", true)

		level.Error(s.logger).Log("error", terr)
	}
}
