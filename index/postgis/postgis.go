package postgis

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"

	"github.com/akhenakh/insideout"
)

// Index using postgis
type Index struct {
	*sql.DB
}

func New(hostname, username, password, dbName string) (*Index, error) {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=disable",
		username, password, dbName, hostname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return &Index{
		DB: db,
	}, nil
}

// Stab returns polygon's ids we are inside and polygon's ids we may be inside
// in case of this index we are always in
func (idx *Index) Stab(lat, lng float64) (insideout.IndexResponse, error) {
	var idxResp insideout.IndexResponse

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	q := fmt.Sprintf(`SELECT ogc_fid FROM polytest
			WHERE ST_Contains(wkb_geometry,
				ST_Transform(ST_GeomFromText('POINT(%f %f)', 4326), 4326)
			)`, lng, lat)

	rows, err := idx.QueryContext(ctx, q)
	if err != nil {
		return idxResp, err
	}
	for rows.Next() {
		var ogcFID int
		if err := rows.Scan(&ogcFID); err != nil {
			return idxResp, err
		}
		res := insideout.FeatureIndexResponse{}
		res.ID = uint32(ogcFID)
		idxResp.IDsInside = append(idxResp.IDsInside, res)
	}

	return idxResp, nil
}
