package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/akhenakh/insideout"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rcrowley/go-metrics"
)

var (
	dbURL  = flag.String("dbURL", "localhost", "database URL use with postgis index only")
	latMin = flag.Float64("latMin", 49.10, "Lat min")
	lngMin = flag.Float64("lngMin", -1.10, "Lng min")
	latMax = flag.Float64("latMax", 46.63, "Lat max")
	lngMax = flag.Float64("lngMax", 5.5, "Lng max")
)

func main() {
	flag.Parse()

	poolConfig, err := pgxpool.ParseConfig(*dbURL)
	if err != nil {
		log.Fatal(err)
	}

	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		log.Fatal(err)
	}

	// catch termination
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	defer signal.Stop(interrupt)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		tm := metrics.NewTimer()

		for {
			t := time.Now()
			lat := *latMin + rand.Float64()*(*latMax-*latMin) // nolint: gosec
			lng := *lngMin + rand.Float64()*(*lngMax-*lngMin) // nolint: gosec

			tm.UpdateSince(t)

			_, err := Stab(ctx, pool, lat, lng)
			if err != nil {
				break
			}
		}

		msg := fmt.Sprintf("count %d rate mean %.0f/s rate1 %.0f/s 99p %.0f\n",
			tm.Count(), tm.RateMean(), tm.Rate1(), tm.Percentile(99.0))
		fmt.Println(msg)
	}()

	select {
	case <-interrupt:
		cancel()

		break
	case <-ctx.Done():
		break
	}

	wg.Wait()
}

// Stab returns polygon's ids we are inside and polygon's ids we may be inside
// in case of this index we are always in.
func Stab(ctx context.Context, pool *pgxpool.Pool, lat, lng float64) (insideout.IndexResponse, error) {
	var idxResp insideout.IndexResponse

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	q := fmt.Sprintf(`SELECT ogc_fid FROM france
			WHERE ST_Contains(wkb_geometry,
				ST_Transform(ST_GeomFromText('POINT(%f %f)', 4326), 4326)
			)`, lng, lat)

	rows, err := pool.Query(ctx, q)
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
