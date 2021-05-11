package insideout

import (
	"github.com/golang/geo/s2"
)

// Index offers different strategy indexers to speed up queries.
type Index interface {
	// Stab returns ids of polygon we are inside and polygons we may be inside
	Stab(lat, lng float64) (IndexResponse, error)
}

// Index offers different strategy indexers to speed up queries.
type OSMIndex interface {
	// Stab returns ids of polygon we are inside and polygons we may be inside
	Stab(lat, lng float64) (OSMIndexResponse, error)
}

// IndexResponse a response to find back a feature from an index.
type IndexResponse struct {
	IDsInside      []FeatureIndexResponse
	IDsMayBeInside []FeatureIndexResponse
}

// OSMIndexResponse a response to find back a feature from an index.
type OSMIndexResponse struct {
	IDsInside      []OSMFeatureIndexResponse
	IDsMayBeInside []OSMFeatureIndexResponse
}

// FeatureIndexResponse a feature response to find back a feature from an index.
type FeatureIndexResponse struct {
	// id of the feature
	ID uint32
	// index of the polygon (in case of multipolygon)
	Pos uint16
}

// OSMFeatureIndexResponse a feature response to find back a feature from an index.
type OSMFeatureIndexResponse struct {
	// id of the feature
	ID int64
	// index of the polygon (in case of multipolygon)
	Pos uint16
}

// Feature representation in memory.
type Feature struct {
	Loops      []*s2.Loop
	Properties map[string]interface{}
}
