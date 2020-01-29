package insideout

import (
	"bytes"
	"encoding/binary"

	"github.com/golang/geo/s2"
	"github.com/pkg/errors"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
)

const (
	insidePrefix  = 'I'
	outsidePrefix = 'O'
	featurePrefix = 'F'
	cellPrefix    = 'C'
	infoKey       = 'i'

	InsideTreeStrategy = "insidetree"
	DBStrategy         = "db"
	ShapeIndex         = "shapeindex"
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

func InsideKey(c s2.CellID) []byte {
	k := make([]byte, 1+8)
	k[0] = insidePrefix
	binary.BigEndian.PutUint64(k[1:], uint64(c))
	return k
}

func OutsideKey(c s2.CellID) []byte {
	k := make([]byte, 1+8)
	k[0] = outsidePrefix
	binary.BigEndian.PutUint64(k[1:], uint64(c))
	return k
}

func FeatureKey(id uint32) []byte {
	k := make([]byte, 1+4)
	k[0] = featurePrefix
	binary.BigEndian.PutUint32(k[1:], id)
	return k
}

func CellKey(id uint32) []byte {
	k := make([]byte, 1+4)
	k[0] = cellPrefix
	binary.BigEndian.PutUint32(k[1:], id)
	return k
}

func InfoKey() []byte {
	return []byte{infoKey}
}
