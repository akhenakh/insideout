package server

import (
	"net/http"
	"strconv"

	"github.com/golang/protobuf/jsonpb"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/gorilla/mux"
	"github.com/opentracing/opentracing-go"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"

	"github.com/akhenakh/insideout"
	"github.com/akhenakh/insideout/insidesvc"
)

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
	cs, err := s.storage.LoadCellStorage(uint32(fid))
	if err != nil {
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
	ctx := r.Context()

	span, ctx := opentracing.StartSpanFromContext(ctx, "WithinHandler")
	defer span.Finish()

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

	resp, err := s.Within(ctx, &insidesvc.WithinRequest{
		Lat: lat,
		Lng: lng,
	})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	fc := &geojson.FeatureCollection{}
	if len(resp.Responses) == 0 {
		http.Error(w, "{\"msg\": \"no features found at this location\"}", 404)
		return
	}
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
