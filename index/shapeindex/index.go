package shapeindex

import (
	"bytes"
	"errors"
	"sync"

	"github.com/golang/geo/s2"

	"github.com/akhenakh/insideout"
)

// Index using s2.ShapeIndexStrategy
type Index struct {
	sync.Mutex
	*s2.ShapeIndex
	*s2.ContainsPointQuery
}

type indexedLoop struct {
	*s2.Loop
	insideout.FeatureIndexResponse
}

func New() *Index {
	return &Index{
		ShapeIndex: s2.NewShapeIndex(),
	}
}

func (idx *Index) Add(si *insideout.FeatureStorage, id uint32) error {
	idx.Lock()
	defer idx.Unlock()
	for i := 0; i < len(si.LoopsBytes); i++ {
		l := &s2.Loop{}
		if err := l.Decode(bytes.NewReader(si.LoopsBytes[i])); err != nil {
			return err
		}

		il := indexedLoop{
			Loop: l,
			FeatureIndexResponse: insideout.FeatureIndexResponse{
				ID:  id,
				Pos: uint16(i),
			},
		}

		idx.ShapeIndex.Add(il)
	}
	return nil
}

// Stab returns polygon's ids we are inside and polygon's ids we may be inside
// in case of this index we are always in
func (idx *Index) Stab(lat, lng float64) (insideout.IndexResponse, error) {
	idx.Lock()
	defer idx.Unlock()
	p := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))

	var idxResp insideout.IndexResponse

	if idx.ContainsPointQuery == nil {
		idx.ContainsPointQuery = s2.NewContainsPointQuery(idx.ShapeIndex, s2.VertexModelOpen)
	}

	shapes := idx.ContainsPointQuery.ContainingShapes(p)

	for _, shape := range shapes {
		il, ok := shape.(indexedLoop)
		if !ok {
			return idxResp, errors.New("invalid type read from db")
		}

		idxResp.IDsInside = append(idxResp.IDsInside, il.FeatureIndexResponse)
	}

	return idxResp, nil
}
