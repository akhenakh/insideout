package dbindex

import (
	"github.com/akhenakh/insideout"
)

// Index using dbindex
type Index struct {
	storage insideout.Store

	opts Options
}

// Options for the dbindex
type Options struct {
	// StopOnInside, if you know your data does not overlap (eg countries) set it to true
	// so it won't go looking further and response faster
	StopOnInsideFound bool
}

func New(storage insideout.Store, opts Options) *Index {
	return &Index{
		storage: storage,
		opts:    opts,
	}
}

// Stab returns polygon's ids containing lat lng and polygon's ids that may be
func (idx *Index) Stab(lat, lng float64) (insideout.IndexResponse, error) {
	return idx.storage.StabDB(lat, lng, idx.opts.StopOnInsideFound)
}
