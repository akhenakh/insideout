package server

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"strconv"

	"github.com/bluele/gcache"
	"github.com/fxamacker/cbor"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/geo/s2"
	"github.com/golang/protobuf/jsonpb"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/gorilla/mux"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/status"

	"github.com/akhenakh/insideout"
	"github.com/akhenakh/insideout/index/dbindex"
	"github.com/akhenakh/insideout/index/shapeindex"
	"github.com/akhenakh/insideout/index/treeindex"
	"github.com/akhenakh/insideout/insidesvc"
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
	case insideout.ShapeIndexStrategy:
		shapeidx := shapeindex.New()
		err := storage.LoadAllFeatures(shapeidx.Add)
		if err != nil {
			level.Error(logger).Log("msg", "failed to load feature from storage", "error", err, "strategy", opts.Strategy)
			os.Exit(2)
		}
		idx = shapeidx
	case insideout.DBStrategy:
		dbidx, err := dbindex.New(storage, dbindex.Options{StopOnInsideFound: opts.StopOnFirstFound})
		if err != nil {
			level.Error(logger).Log("msg", "failed to read storage", "error", err, "strategy", opts.Strategy)
			os.Exit(2)
		}
		idx = dbidx
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

// Within query exposed via gRPC
func (s *Server) Within(ctx context.Context, req *insidesvc.WithinRequest) (*insidesvc.WithinResponse, error) {
	idxResp, err := s.idx.Stab(req.Lat, req.Lng)
	if err != nil {
		return nil, err
	}

	level.Debug(s.logger).Log("msg", "querying within",
		"lat", req.Lat,
		"lng", req.Lng,
		"idx_resp", idxResp,
	)

	var fresps []*insidesvc.FeatureResponse

	for _, fid := range idxResp.IDsInside {
		fi, err := s.cache.Get(fid.ID)
		if err != nil {
			return nil, err
		}

		f := fi.(*insideout.Feature)
		level.Debug(s.logger).Log("msg", "Found inside feature",
			"fid", fid.ID,
			"properties", f.Properties,
			"loop #", fid.Pos)

		feature := &insidesvc.Feature{}

		if !req.RemoveGeometries {
			l := f.Loops[fid.Pos]
			feature.Geometry = &insidesvc.Geometry{
				Type:        insidesvc.Geometry_POLYGON,
				Coordinates: insideout.CoordinatesFromLoops(l),
			}
		}

		//TODO: filter properties
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

	p := s2.PointFromLatLng(s2.LatLngFromDegrees(req.Lat, req.Lng))
	for _, fid := range idxResp.IDsMayBeInside {
		fi, err := s.cache.Get(fid.ID)
		if err != nil {
			return nil, err
		}

		f := fi.(*insideout.Feature)
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
				Type:        insidesvc.Geometry_POLYGON,
				Coordinates: insideout.CoordinatesFromLoops(l),
			}
		}

		//TODO: filter properties
		prop, err := insideout.PropertiesToValues(f)
		if err != nil {
			return nil, err
		}
		feature.Properties = prop

		fresp := &insidesvc.FeatureResponse{
			Id:      fid.ID,
			Feature: feature,
		}
		fresps = append(fresps, fresp)
	}

	level.Info(s.logger).Log("msg", "result stab",
		"lat", req.Lat,
		"lng", req.Lng,
		"features_count", len(fresps))

	resp := &insidesvc.WithinResponse{
		Point: &insidesvc.Point{
			Lat: req.Lat,
			Lng: req.Lng,
		},
		Responses: fresps,
	}

	return resp, nil
}

// DebugGetHandler HTTP 1.1 Handler to debug a feature
func (s *Server) DebugGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	fid, err := strconv.ParseUint(vars["fid"], 10, 32)
	if err != nil {
		http.Error(w, "invalid parameter fid", 400)
		return
	}
	lidx, err := strconv.ParseUint(vars["loop_index"], 10, 16)
	if err != nil {
		http.Error(w, "invalid parameter fid", 400)
		return
	}

	ctx := r.Context()

	f, err := s.Get(ctx, &insidesvc.GetRequest{
		Id:        uint32(fid),
		LoopIndex: uint32(lidx),
	})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// get the s2 cells from the index
	v, err := s.storage.Get(insideout.CellKey(uint32(fid)), nil)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	dec := cbor.NewDecoder(bytes.NewReader(v))
	cs := &insideout.CellsStorage{}
	if err := dec.Decode(cs); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	f.Properties[insidesvc.CellsInProperty] = &structpb.Value{
		Kind: &structpb.Value_StringValue{
			StringValue: insideout.CellUnionToToken(cs.CellsIn[lidx]),
		},
	}

	f.Properties[insidesvc.CellsOutProperty] = &structpb.Value{
		Kind: &structpb.Value_StringValue{
			StringValue: insideout.CellUnionToToken(cs.CellsOut[lidx]),
		},
	}
	w.Header().Set("Content-Type", "application/json")

	m := jsonpb.Marshaler{}
	err = m.Marshal(w, f)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

// WithinHandler HTTP 1.1 Handler to query within returns GeoJSON
func (s *Server) WithinHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	lat, err := strconv.ParseFloat(vars["lat"], 64)
	if err != nil {
		http.Error(w, "invalid parameter lat", 400)
		return
	}
	lng, err := strconv.ParseFloat(vars["lng"], 64)
	if err != nil {
		http.Error(w, "invalid parameter lat", 400)
		return
	}

	ctx := r.Context()

	resp, err := s.Within(ctx, &insidesvc.WithinRequest{
		Lat: lat,
		Lng: lng,
	})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	fc := &geojson.FeatureCollection{}
	for _, fres := range resp.Responses {
		f := &geojson.Feature{}
		ng := geom.NewPolygonFlat(geom.XY, fres.Feature.Geometry.Coordinates, []int{len(fres.Feature.Geometry.Coordinates)})
		f.Geometry = ng
		f.Properties = insideout.ValueToProperties(fres.Feature.Properties)
		fc.Features = append(fc.Features, f)
	}

	w.Header().Set("Content-Type", "application/json")
	json, err := fc.MarshalJSON()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Write(json)
}

func (s *Server) Get(ctx context.Context, req *insidesvc.GetRequest) (*insidesvc.Feature, error) {
	fi, err := s.cache.Get(req.Id)
	if err != nil {
		return nil, err
	}

	if fi == nil {
		return nil, status.Error(codes.NotFound, "can't found feature")
	}

	f := fi.(*insideout.Feature)

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
			Type:        insidesvc.Geometry_POLYGON,
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

	return feature, nil
}

// Stab returns features containing lat lng
func (s *Server) IndexStab(lat, lng float64) ([]*insideout.Feature, error) {
	var res []*insideout.Feature
	idxResp, err := s.idx.Stab(lat, lng)
	if err != nil {
		return nil, err
	}
	for _, fid := range idxResp.IDsInside {
		fi, err := s.cache.Get(fid.ID)
		if err != nil {
			return nil, err
		}
		f := fi.(*insideout.Feature)
		level.Debug(s.logger).Log("msg", "Found inside feature",
			"fid", fid.ID,
			"properties", f.Properties,
			"loop #", fid.Pos)
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
			level.Debug(s.logger).Log("msg", "Found outside + PIP feature",
				"fid", fid.ID,
				"properties", f.Properties,
				"loop #", fid.Pos)
			res = append(res, f)
		}
	}
	return res, nil
}
