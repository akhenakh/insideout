package main

import (
	"context"
	"database/sql"
	"fmt"
	stdlog "log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	//_ "net/http/pprof"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	_ "github.com/lib/pq"
	"github.com/namsral/flag"
	"github.com/rcrowley/go-metrics"

	"github.com/akhenakh/insideout/loglevel"
)

/*
SELECT * FROM polytest
    WHERE ST_Contains(wkb_geometry,
    	ST_Transform(
    		ST_GeomFromText('POINT(2.2 48.8)', 4326), 4326
    	)
    )
*/

const appName = "loadtesterpg"

var (
	logLevel = flag.String("logLevel", "INFO", "DEBUG|INFO|WARN|ERROR")

	dbHost = flag.String("dbHost", "localhost", "database hostname")
	dbUser = flag.String("dbUser", "testgis", "database username")
	dbPass = flag.String("dbPass", "testgis", "database password")
	dbName = flag.String("dbName", "testgis", "database name")

	latMin = flag.Float64("latMin", 49.10, "Lat min")
	lngMin = flag.Float64("lngMin", -1.10, "Lng min")
	latMax = flag.Float64("latMax", 46.63, "Lat max")
	lngMax = flag.Float64("lngMax", 5.5, "Lng max")
)

func main() {
	flag.Parse()

	// pprof
	// go func() {
	// 	stdlog.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger, "caller", log.Caller(5), "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "app", appName)
	logger = loglevel.NewLevelFilterFromString(logger, *logLevel)

	stdlog.SetOutput(log.NewStdlibAdapter(logger))

	rand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())

	// catch termination
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tm := metrics.NewTimer()

		connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=disable",
			*dbUser, *dbPass, *dbName, *dbHost)

		db, err := sql.Open("postgres", connStr)
		if err != nil {
			level.Error(logger).Log("msg", "error opening", "error", err)
			os.Exit(2)
		}
		for {
			select {
			case <-ctx.Done():
				break
			default:
			}
			ctx, rcancel := context.WithTimeout(ctx, 200*time.Millisecond)

			t := time.Now()
			lat := *latMin + rand.Float64()*(*latMax-*latMin)
			lng := *lngMin + rand.Float64()*(*lngMax-*lngMin)
			q := fmt.Sprintf(`SELECT insee, nom, wikipedia, surf_ha FROM polytest
			WHERE ST_Contains(wkb_geometry,
				ST_Transform(ST_GeomFromText('POINT(%f %f)', 4326), 4326)
			)`, lng, lat)
			rows, err := db.QueryContext(ctx, q)
			if err != nil {
				level.Error(logger).Log("msg", "sql query error", "error", err)
				rcancel()
				break
			}

			for rows.Next() {
				var insee, nom, wikipedia, surf_ha string
				if err := rows.Scan(&insee, &nom, &wikipedia, &surf_ha); err != nil {
					level.Error(logger).Log("msg", "sql row scan error", "error", err)
					rcancel()
					break
				}
				level.Debug(logger).Log(
					"msg", "found feature",
					"insee", insee,
					"name", nom,
					"lat", lat,
					"lng", lng,
				)
			}
			tm.UpdateSince(t)
			rcancel()
		}

		fmt.Printf("count %d rate mean %.0f/s rate1 %.0f/s 99p %.0f\n",
			tm.Count(), tm.RateMean(), tm.Rate1(), tm.Percentile(99.0))
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
