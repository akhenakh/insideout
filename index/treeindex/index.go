package treeindex

import (
	"github.com/akhenakh/insidetree"
	"github.com/golang/geo/s2"

	"github.com/akhenakh/insideout"
)

// Index using insidetree
type Index struct {
	itree *insidetree.Tree
	otree *insidetree.Tree

	opts Options
}

// Options for the insidetree Index
type Options struct {
	// StopOnInside, if you know your data does not overlap (eg countries) set it to true
	// so it won't go looking further and response faster
	StopOnInsideFound bool
}

func New(opts Options) *Index {
	return &Index{
		itree: insidetree.NewTree(),
		otree: insidetree.NewTree(),
		opts:  opts,
	}
}

func (idx *Index) Add(si *insideout.FeatureStorage, id uint32) error {
	for i, cu := range si.CellsIn {
		for _, c := range cu {
			idx.itree.Index(c, insideout.FeatureIndexResponse{
				ID:  id,
				Pos: uint16(i),
			})
		}
	}
	for i, cu := range si.CellsOut {
		for _, c := range cu {
			idx.otree.Index(c, insideout.FeatureIndexResponse{
				ID:  id,
				Pos: uint16(i),
			})
		}
	}

	return nil
}

// Stab returns polygon's ids we are inside and polygon's ids we may be inside
func (idx *Index) Stab(lat, lng float64) insideout.IndexResponse {
	var idxResp insideout.IndexResponse

	p := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))

	c := s2.CellFromPoint(p).ID()
	res := idx.itree.Stab(c)
	for _, r := range res {
		fres := r.(insideout.FeatureIndexResponse)
		idxResp.IDsInside = append(idxResp.IDsInside, fres)
	}

	if idx.opts.StopOnInsideFound && len(res) > 0 {
		return idxResp
	}

	res = idx.otree.Stab(c)
	if len(res) == 0 {
		return idxResp
	}

	for _, r := range res {
		fres := r.(insideout.FeatureIndexResponse)
		idxResp.IDsMayBeInside = append(idxResp.IDsMayBeInside, fres)
	}
	return idxResp
}
