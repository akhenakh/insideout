package postgis

import (
	"context"
	"fmt"
	"time"

	log "github.com/go-kit/kit/log"
	"github.com/jackc/pgx/v4/log/kitlogadapter"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/akhenakh/insideout"
)

// Index using postgis.
type Index struct {
	*pgxpool.Pool
}

func New(ctx context.Context, logger log.Logger, dbURL string) (*Index, error) {
	poolConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("can't parse db url %s error: %w", dbURL, err)
	}

	pgxlogger := kitlogadapter.NewLogger(log.With(logger, "caller", log.Caller(5)))

	poolConfig.ConnConfig.Logger = pgxlogger

	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create pool: %w", err)
	}

	return &Index{Pool: pool}, nil
}

// Stab returns polygon's ids we are inside and polygon's ids we may be inside
// in case of this index we are always in.
func (idx *Index) Stab(lat, lng float64) (insideout.IndexResponse, error) {
	var idxResp insideout.IndexResponse

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	q := fmt.Sprintf(`SELECT ogc_fid FROM france
			WHERE ST_Contains(wkb_geometry,
				ST_Transform(ST_GeomFromText('POINT(%f %f)', 4326), 4326)
			)`, lng, lat)

	rows, err := idx.Query(ctx, q)
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
