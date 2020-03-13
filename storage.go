package insideout

import (
	"fmt"
	"time"

	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom/encoding/geojson"
)

type Store interface {
	LoadFeature(id uint32) (*Feature, error)
	LoadAllFeatures(add func(*FeatureStorage, uint32) error) error
	LoadFeaturesCells(add func([]s2.CellUnion, []s2.CellUnion, uint32)) error
	LoadCellStorage(id uint32) (*CellsStorage, error)
	LoadIndexInfos() (*IndexInfos, error)
	StabDB(lat, lng float64, StopOnInsideFound bool) (IndexResponse, error)
	Index(fc geojson.FeatureCollection, icoverer *s2.RegionCoverer, ocoverer *s2.RegionCoverer,
		warningCellsCover int, fileName, version string) error
}

// FeatureStorage on disk storage of the feature
type FeatureStorage struct {
	Properties map[string]interface{} `cbor:"1,keyasint,omitempty"`

	// Next entries are arrays since a multipolygon may contains multiple loop
	// LoopsBytes encoded with s2 Loop encoder
	LoopsBytes [][]byte `cbor:"2,keyasint,omitempty"`
}

// CellsStorage are used to store indexed cells
// for use with the treeindex
type CellsStorage struct {
	// Cells inside cover
	CellsIn []s2.CellUnion `cbor:"1,keyasint,omitempty"`

	// Cells outside cover
	CellsOut []s2.CellUnion `cbor:"2,keyasint,omitempty"`
}

// IndexInfos used to store information about the index in DB
type IndexInfos struct {
	Filename       string    `cbor:"1,keyasint,omitempty"`
	IndexTime      time.Time `cbor:"2,keyasint,omitempty"`
	IndexerVersion string    `cbor:"3,keyasint,omitempty"`
	FeatureCount   uint32    `cbor:"4,keyasint,omitempty"`
	MinCoverLevel  int       `cbor:"5,keyasint,omitempty"`
}

func (infos *IndexInfos) String() string {
	return fmt.Sprintf("Filename: %s\nIndexTime: %s\nIndexerVersion: %s\nFeatureCount %d\n",
		infos.Filename,
		infos.IndexTime,
		infos.IndexerVersion,
		infos.FeatureCount,
	)
}
