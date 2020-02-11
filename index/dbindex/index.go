package dbindex

import (
	"encoding/binary"

	"github.com/golang/geo/s2"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/akhenakh/insideout"
)

// Index using dbindex
type Index struct {
	storage *insideout.Storage

	opts Options

	minCoverLevel int
}

// Options for the dbindex
type Options struct {
	// StopOnInside, if you know your data does not overlap (eg countries) set it to true
	// so it won't go looking further and response faster
	StopOnInsideFound bool
}

func New(storage *insideout.Storage, opts Options) (*Index, error) {
	infos, err := storage.LoadIndexInfos()
	if err != nil {
		return nil, err
	}

	return &Index{
		storage:       storage,
		opts:          opts,
		minCoverLevel: infos.MinCoverLevel,
	}, nil
}

// Stab returns polygon's ids containing lat lng and polygon's ids that may be
func (idx *Index) Stab(lat, lng float64) (insideout.IndexResponse, error) {
	var idxResp insideout.IndexResponse

	ll := s2.LatLngFromDegrees(lat, lng)
	p := s2.PointFromLatLng(ll)
	c := s2.CellIDFromLatLng(ll)
	cLookup := s2.CellFromPoint(p).ID().Parent(idx.minCoverLevel)

	startKey, stopKey := insideout.InsideRangeKeys(cLookup)
	iter := idx.storage.NewIterator(&util.Range{Start: startKey, Limit: stopKey}, &opt.ReadOptions{
		DontFillCache: true,
	})
	defer iter.Release()

	mi := make(map[insideout.FeatureIndexResponse]struct{})

	for iter.Next() {
		k := iter.Key()
		cr := s2.CellID(binary.BigEndian.Uint64(k[1:]))
		if !cr.Contains(c) {
			continue
		}
		v := iter.Value()
		// read back the feature id and polygon index uint32 + uint16
		for i := 0; i < len(v); i += 4 + 2 {
			res := insideout.FeatureIndexResponse{}
			res.ID = binary.BigEndian.Uint32(v[i:])
			res.Pos = binary.BigEndian.Uint16(v[i+4:])
			mi[res] = struct{}{}
			if idx.opts.StopOnInsideFound {
				idxResp.IDsInside = append(idxResp.IDsInside, res)
				return idxResp, nil
			}
		}
	}

	// dedup
	for res := range mi {
		idxResp.IDsInside = append(idxResp.IDsInside, res)
	}

	if err := iter.Error(); err != nil {
		return idxResp, err
	}

	startKey, stopKey = insideout.OutsideRangeKeys(cLookup)
	oiter := idx.storage.NewIterator(&util.Range{Start: startKey, Limit: stopKey}, &opt.ReadOptions{
		DontFillCache: true,
	})
	defer oiter.Release()

	mo := make(map[insideout.FeatureIndexResponse]struct{})

	for oiter.Next() {
		k := oiter.Key()
		cr := s2.CellID(binary.BigEndian.Uint64(k[1:]))
		if !cr.Contains(c) {
			continue
		}
		v := oiter.Value()
		// read back the feature id and polygon index uint32 + uint16
		for i := 0; i < len(v); i += 4 + 2 {
			res := insideout.FeatureIndexResponse{}
			res.ID = binary.BigEndian.Uint32(v[i:])
			res.Pos = binary.BigEndian.Uint16(v[i+4:])
			// remove any answer matching inside
			if _, ok := mi[res]; !ok {
				mo[res] = struct{}{}
			}
		}
	}

	// dedup
	for res := range mo {
		idxResp.IDsMayBeInside = append(idxResp.IDsMayBeInside, res)
	}

	if err := oiter.Error(); err != nil {
		return idxResp, err
	}

	return idxResp, nil
}
