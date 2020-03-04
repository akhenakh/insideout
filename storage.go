package insideout

import (
	"fmt"
	"net/http"
	"time"

	"github.com/golang/geo/s2"
)

type Store interface {
	LoadFeature(id uint32) (*Feature, error)
	LoadAllFeatures(add func(*FeatureStorage, uint32) error) error
	LoadFeaturesCells(add func(*CellsStorage, uint32)) error
	LoadCellStorage(id uint32) (*CellsStorage, error)
	LoadIndexInfos() (*IndexInfos, error)
	LoadMapInfos() (*MapInfos, bool, error)
}

type TileStore interface {
	TilesHandler(w http.ResponseWriter, req *http.Request)
	ReadTileData(z uint8, x uint64, y uint64) ([]byte, error)
}

// FeatureStorage on disk storage of the feature
type FeatureStorage struct {
	Properties map[string]interface{}

	// Next entries are arrays since a multipolygon may contains multiple loop
	// LoopsBytes encoded with s2 Loop encoder
	LoopsBytes [][]byte
}

// CellsStorage are used to store indexed cells
// for use with the treeindex
type CellsStorage struct {
	// Cells inside cover
	CellsIn []s2.CellUnion

	// Cells outside cover
	CellsOut []s2.CellUnion
}

// IndexInfos used to store information about the index in DB
type IndexInfos struct {
	Filename       string
	IndexTime      time.Time
	IndexerVersion string
	FeatureCount   uint32
	MinCoverLevel  int
}

// MapInfos used to store information about the map if any in DB
type MapInfos struct {
	CenterLat, CenterLng float64
	MaxZoom              int
}

func (infos *IndexInfos) String() string {
	return fmt.Sprintf("Filename: %s\nIndexTime: %s\nIndexerVersion: %s\nFeatureCount %d\n",
		infos.Filename,
		infos.IndexTime,
		infos.IndexerVersion,
		infos.FeatureCount,
	)
}
