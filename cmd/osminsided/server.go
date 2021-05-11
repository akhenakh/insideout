package main

import (
	"context"
	"fmt"

	"github.com/dgraph-io/ristretto"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/geo/s2"
	"github.com/opentracing/opentracing-go"
	slog "github.com/opentracing/opentracing-go/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/status"

	"github.com/akhenakh/insideout"
	"github.com/akhenakh/insideout/gen/go/osminsidesvc/v1"
	"github.com/akhenakh/insideout/index/osmtreeidx"
)

// Server exposes indexes services.
type Server struct {
	logger       log.Logger
	healthServer *health.Server
	idx          insideout.OSMIndex
	cache        *ristretto.Cache
	storage      insideout.OSMStore
}

func NewServer(
	ctx context.Context,
	logger log.Logger,
	storage insideout.OSMStore,
	healthServer *health.Server,
) (*Server, error) {
	treeidx := osmtreeidx.New(osmtreeidx.Options{StopOnInsideFound: false})

	err := storage.LoadFeaturesCells(treeidx.Add)
	if err != nil {
		level.Error(logger).Log("msg", "failed to load cells from storage", "error", err)

		return nil, fmt.Errorf("ailed to load cells from storage: %w", err)
	}

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e4,     // number of keys to track frequency
		MaxCost:     1 << 27, // 128M
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		return nil, fmt.Errorf("cache error: %w", err)
	}

	return &Server{
		idx:          treeidx,
		logger:       logger,
		healthServer: healthServer,
		cache:        cache,
		storage:      storage,
	}, nil
}

// Within query exposed via gRPC.
func (s *Server) Within(
	ctx context.Context, req *osminsidesvc.WithinRequest,
) (resp *osminsidesvc.WithinResponse, terr error) {
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

	resp = &osminsidesvc.WithinResponse{}

	for _, fid := range idxResp.IDsInside {
		resp.Fids = append(resp.Fids, fid.ID)
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

		resp.Fids = append(resp.Fids, fid.ID)
	}

	return resp, nil
}

// feature fetch feature from cache or from storage.
func (s *Server) feature(id int64) (*insideout.Feature, error) {
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
