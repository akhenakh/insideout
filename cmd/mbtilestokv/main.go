package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"

	"github.com/fxamacker/cbor"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	_ "github.com/mattn/go-sqlite3"
	"github.com/namsral/flag"
	"go.etcd.io/bbolt"

	"github.com/akhenakh/insideout"
	"github.com/akhenakh/insideout/loglevel"
)

const appName = "mbtilestokv"

var (
	version  = "no version from LDFLAGS"
	logLevel = flag.String("logLevel", "INFO", "DEBUG|INFO|WARN|ERROR")

	centerLat = flag.Float64("centerLat", 48.8, "Latitude center used for the debug map")
	centerLng = flag.Float64("centerLng", 2.2, "Longitude center used for the debug map")
	maxZoom   = flag.Int("maxZoom", 9, "max zoom used for the debug map")

	tilesPath = flag.String("tilesPath", "./france9.mbtiles", "mbtiles file path")
	dbPath    = flag.String("dbPath", "./inside.db", "db path out")
)

func main() {
	flag.Parse()

	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger, "caller", log.Caller(5), "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "app", appName)
	logger = loglevel.NewLevelFilterFromString(logger, *logLevel)

	level.Info(logger).Log("msg", "starting converting tiles", "version", version)

	database, err := sql.Open("sqlite3", *tilesPath)
	if err != nil {
		level.Error(logger).Log("msg", "can't read mbtiles sqlite", "error", err)
		os.Exit(2)
	}

	db, err := bbolt.Open(*dbPath, 0600, nil)
	if err != nil {
		level.Error(logger).Log("msg", "can't open leveldb", "error", err)
		os.Exit(2)
	}
	defer db.Close()

	rows, err := database.Query("SELECT * FROM map")
	if err != nil {
		level.Error(logger).Log("msg", "can't read data from mbtiles sqlite", "error", err)
		os.Exit(2)
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(insideout.MapKey())
		if err != nil {
			return err
		}
		var zoom, column, row int
		var tileID, gridID, key string
		for rows.Next() {
			rows.Scan(&zoom, &column, &row, &tileID, &gridID)
			key = fmt.Sprintf("%c%d/%d/%d", insideout.TilesURLPrefix, zoom, column, row)
			if err = b.Put([]byte(key), []byte(tileID)); err != nil {
				return err
			}
		}

		rows, err = database.Query("SELECT * FROM images")
		if err != nil {
			return err
		}

		var tileData []byte
		for rows.Next() {
			rows.Scan(&tileData, &tileID)
			key = fmt.Sprintf("%c%s", insideout.TilesPrefix, tileID)
			if err = b.Put([]byte(key), tileData); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		level.Error(logger).Log("msg", "failed writing to DB", "error", err, "db_path", *dbPath)
		os.Exit(2)
	}

	infoBytes := new(bytes.Buffer)

	infos := &insideout.MapInfos{
		CenterLat: *centerLat,
		CenterLng: *centerLng,
		MaxZoom:   *maxZoom,
	}

	enc := cbor.NewEncoder(infoBytes, cbor.CanonicalEncOptions())
	if err := enc.Encode(infos); err != nil {
		level.Error(logger).Log("msg", "failed encoding MapInfos", "error", err)
		os.Exit(2)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(insideout.MapKey())

		return b.Put(insideout.MapKey(), infoBytes.Bytes())
	})
	if err != nil {
		level.Error(logger).Log("msg", "failed writing MapInfos to DB", "error", err, "db_path", *dbPath)
		os.Exit(2)
	}

}
