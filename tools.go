package insideout

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/golang/geo/s2"
	spb "github.com/golang/protobuf/ptypes/struct"
	"github.com/pkg/errors"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
)

const (
	insidePrefix  byte = 'I'
	outsidePrefix byte = 'O'
	featurePrefix byte = 'F'
	cellPrefix    byte = 'C'
	infoKey       byte = 'i'
	mapKey        byte = 'm'
	// reserved T & t for tiles
	TilesURLPrefix byte = 't'
	TilesPrefix    byte = 'T'

	InsideTreeStrategy = "insidetree"
	DBStrategy         = "db"
	ShapeIndexStrategy = "shapeindex"
)

// GeoJSONCoverCellUnion generates an s2 cover normalized
func GeoJSONCoverCellUnion(f *geojson.Feature, coverer *s2.RegionCoverer, interior bool) ([]s2.CellUnion, error) {
	if f.Geometry == nil {
		return nil, errors.New("invalid geometry")
	}
	var cu []s2.CellUnion

	switch rg := f.Geometry.(type) {
	case *geom.Polygon:
		// only supports outer ring
		cup, err := coverPolygon(rg.FlatCoords(), coverer, interior)
		if err != nil {
			return nil, errors.Wrap(err, "can't cover polygon")
		}
		cu = append(cu, cup)
	case *geom.MultiPolygon:
		for i := 0; i < rg.NumPolygons(); i++ {
			p := rg.Polygon(i)
			cup, err := coverPolygon(p.FlatCoords(), coverer, interior)
			if err != nil {
				return nil, errors.Wrap(err, "can't cover polygon")
			}
			cu = append(cu, cup)
		}

	default:
		return nil, errors.New("unsupported data type")
	}

	return cu, nil
}

// GeoJSONEncodeLoops encodes all MultiPolygons and Polygons as loops []byte
func GeoJSONEncodeLoops(f *geojson.Feature) ([][]byte, error) {
	if f.Geometry == nil {
		return nil, errors.New("invalid geometry")
	}
	var b [][]byte

	switch rg := f.Geometry.(type) {
	case *geom.Polygon:
		// only supports outer ring
		lb := new(bytes.Buffer)
		l := LoopFromCoordinates(rg.FlatCoords())
		err := l.Encode(lb)
		if err != nil {
			return nil, errors.Wrap(err, "can't encode polygon")
		}
		b = append(b, lb.Bytes())

	case *geom.MultiPolygon:
		for i := 0; i < rg.NumPolygons(); i++ {
			lb := new(bytes.Buffer)
			p := rg.Polygon(i)
			l := LoopFromCoordinates(p.FlatCoords())
			err := l.Encode(lb)
			if err != nil {
				return nil, errors.Wrap(err, "can't encode polygon")
			}
			b = append(b, lb.Bytes())
		}

	default:
		return nil, errors.New("unsupported data type")
	}

	return b, nil
}

// coverPolygon returns an s2 cover from a list of lng, lat forming a closed polygon
func coverPolygon(c []float64, coverer *s2.RegionCoverer, interior bool) (s2.CellUnion, error) {
	if len(c) < 6 {
		return nil, errors.New("invalid polygons not enough coordinates for a closed polygon")
	}
	if len(c)%2 != 0 {
		return nil, errors.New("invalid polygons odd coordinates number")
	}
	l := LoopFromCoordinates(c)
	if l.IsEmpty() || l.IsFull() || l.ContainsOrigin() {
		return nil, errors.New("invalid polygons")
	}
	if interior {
		return coverer.InteriorCovering(l), nil
	}
	return coverer.Covering(l), nil
}

// LoopFromCoordinates creates a LoopFence from a list of lng lat
func LoopFromCoordinates(c []float64) *s2.Loop {
	if len(c)%2 != 0 || len(c) < 2*3 {
		return nil
	}
	points := make([]s2.Point, len(c)/2)

	for i := 0; i < len(c); i += 2 {
		points[i/2] = s2.PointFromLatLng(s2.LatLngFromDegrees(c[i+1], c[i]))
	}

	if points[0] == points[len(points)-1] {
		// remove last item if same as 1st
		points = append(points[:len(points)-1], points[len(points)-1+1:]...)
	}

	loop := s2.LoopFromPoints(points)
	return loop
}

// CoordinatesFromLoops returns []float64 as lng lat adding 1st as last suitable for GeoJSON
func CoordinatesFromLoops(l *s2.Loop) []float64 {
	points := l.Vertices()
	coords := make([]float64, len(points)*2+2)

	for i := 0; i < len(points); i++ {
		ll := s2.LatLngFromPoint(points[i])
		coords[i*2] = ll.Lng.Degrees()
		coords[i*2+1] = ll.Lat.Degrees()
	}
	coords[len(points)*2] = coords[0]
	coords[len(points)*2+1] = coords[1]

	return coords
}

func InsideKey(c s2.CellID) []byte {
	k := make([]byte, 1+8)
	k[0] = insidePrefix
	binary.BigEndian.PutUint64(k[1:], uint64(c))
	return k
}

// InsideRangeKeys returns the min and max range keys for c
func InsideRangeKeys(c s2.CellID) ([]byte, []byte) {
	mink := make([]byte, 1+8)
	mink[0] = insidePrefix
	binary.BigEndian.PutUint64(mink[1:], uint64(c.RangeMin()))
	maxk := make([]byte, 1+8)
	maxk[0] = insidePrefix
	binary.BigEndian.PutUint64(maxk[1:], uint64(c.RangeMax()))
	return mink, maxk
}

func OutsideKey(c s2.CellID) []byte {
	k := make([]byte, 1+8)
	k[0] = outsidePrefix
	binary.BigEndian.PutUint64(k[1:], uint64(c))
	return k
}

// OutsideRangeKeys returns the min and max range keys for c
func OutsideRangeKeys(c s2.CellID) ([]byte, []byte) {
	mink := make([]byte, 1+8)
	mink[0] = outsidePrefix
	binary.BigEndian.PutUint64(mink[1:], uint64(c.RangeMin()))
	maxk := make([]byte, 1+8)
	maxk[0] = outsidePrefix
	binary.BigEndian.PutUint64(maxk[1:], uint64(c.RangeMax()))
	return mink, maxk
}

// FeatureKey returns the key for the id
func FeatureKey(id uint32) []byte {
	k := make([]byte, 1+4)
	k[0] = featurePrefix
	binary.BigEndian.PutUint32(k[1:], id)
	return k
}

// CellKey returns the key for the cell id
func CellKey(id uint32) []byte {
	k := make([]byte, 1+4)
	k[0] = cellPrefix
	binary.BigEndian.PutUint32(k[1:], id)
	return k
}

// InfoKey returns the key for the info entry
func InfoKey() []byte {
	return []byte{infoKey}
}

// MapKey returns the key for the map entry
func MapKey() []byte {
	return []byte{mapKey}
}

// PropertiesToValues converts feature's properties to protobuf Value
func PropertiesToValues(f *Feature) (map[string]*spb.Value, error) {
	m := make(map[string]*spb.Value)
	for k, vi := range f.Properties {
		switch tv := vi.(type) {
		case bool:
			m[k] = &spb.Value{Kind: &spb.Value_BoolValue{BoolValue: tv}}
		case int:
			m[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: float64(tv)}}
		case string:
			m[k] = &spb.Value{Kind: &spb.Value_StringValue{StringValue: tv}}
		case float64:
			m[k] = &spb.Value{Kind: &spb.Value_NumberValue{NumberValue: tv}}
		case nil:
			// pass
		default:
			return nil, fmt.Errorf("GeoJSON property %s unsupported type %T", k, tv)
		}
	}

	return m, nil
}

// ValueToProperties converts a protobuf Value map to its JSON serializable map equivalent
func ValueToProperties(src map[string]*spb.Value) map[string]interface{} {
	res := make(map[string]interface{})

	for k, v := range src {
		switch x := v.Kind.(type) {
		case *spb.Value_NumberValue:
			res[k] = x.NumberValue
		case *spb.Value_StringValue:
			res[k] = x.StringValue
		case *spb.Value_BoolValue:
			res[k] = x.BoolValue
		}
	}
	return res
}

// CellUnionToTokens a cell union to a token string list
func CellUnionToTokens(cu s2.CellUnion) []string {
	res := make([]string, len(cu))

	for i, c := range cu {
		res[i] = c.ToToken()
	}
	return res
}

// CellUnionToToken return a comma separated list of tokens
func CellUnionToToken(cu s2.CellUnion) string {
	l := CellUnionToTokens(cu)
	return strings.Join(l, ",")
}
