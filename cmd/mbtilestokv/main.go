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
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/akhenakh/insideout"
)

const appName = "mbtilestokv"

var (
	version = "no version from LDFLAGS"

	centerLat = flag.Float64("centerLat", 48.8, "Latitude center used for the debug map")
	centerLng = flag.Float64("centerLng", 2.2, "Longitude center used for the debug map")
	maxZoom   = flag.Int("maxZoom", 9, "max zoom used for the debug map")

	tilesPath = flag.String("tilesPath", "./france9.mbtiles", "mbtiles file path")
	dbPath    = flag.String("dbPath", "./out.db", "db path out")
)

func main() {
	flag.Parse()

	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger, "caller", log.Caller(5), "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "app", appName)
	logger = level.NewFilter(logger, level.AllowAll())

	database, err := sql.Open("sqlite3", *tilesPath)
	if err != nil {
		level.Error(logger).Log("msg", "can't read mbtiles sqlite", "error", err)
		os.Exit(2)
	}

	db, err := leveldb.OpenFile(*dbPath, nil)
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
	var zoom, column, row int
	var tileID, gridID, key string
	for rows.Next() {
		rows.Scan(&zoom, &column, &row, &tileID, &gridID)
		key = fmt.Sprintf("%c%d/%d/%d", insideout.TilesURLPrefix, zoom, column, row)
		if err = db.Put([]byte(key), []byte(tileID), nil); err != nil {
			level.Error(logger).Log("msg", "can't read data from mbtiles sqlite", "error", err)
			os.Exit(2)
		}
	}

	rows, err = database.Query("SELECT * FROM images")
	if err != nil {
		level.Error(logger).Log("msg", "can't read data from mbtiles sqlite", "error", err)
		os.Exit(2)
	}

	var tileData []byte
	for rows.Next() {
		rows.Scan(&tileData, &tileID)
		key = fmt.Sprintf("%c%s", insideout.TilesPrefix, tileID)
		if err = db.Put([]byte(key), tileData, nil); err != nil {
			level.Error(logger).Log("msg", "can't read data from mbtiles sqlite", "error", err)
			os.Exit(2)
		}
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

	if err := db.Put(insideout.MapKey(), infoBytes.Bytes(), nil); err != nil {
		level.Error(logger).Log("msg", "failed writing MapInfos to DB", "error", err, "db_path", *dbPath)
		os.Exit(2)
	}

	if err := db.CompactRange(util.Range{}); err != nil {
		level.Error(logger).Log("msg", "failed while compacting DB", "error", err, "db_path", *dbPath)
		os.Exit(2)
	}
}
