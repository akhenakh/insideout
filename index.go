package insideout

import (
	"github.com/golang/geo/s2"
)

// Index offers different strategy indexers to speed up queries.
type Index interface {
	// Stab returns ids of polygon we are inside and polygons we may be inside
	Stab(lat, lng float64) (IndexResponse, error)
}

// IndexResponse a response to find back a feature from an index.
type IndexResponse struct {
	IDsInside      []FeatureIndexResponse
	IDsMayBeInside []FeatureIndexResponse
}

// FeatureIndexResponse a feature response to find back a feature from an index.
type FeatureIndexResponse struct {
	// id of the feature
	ID uint32
	// index of the polygon (in case of multipolygon)
	Pos uint16
}

// Feature representation in memory
type Feature struct {
	Loops      []*s2.Loop
	Properties map[string]interface{}
}
