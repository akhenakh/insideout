package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/namsral/flag"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"

	_ "github.com/mattn/go-sqlite3"

	"github.com/akhenakh/insideout"
)

var (
	tilesPath = flag.String("tilesPath", "./france9.mbtiles", "mbtiles file path")
	dbPath    = flag.String("dbPath", "./out.db", "db path out")
)

func main() {
	flag.Parse()

	database, err := sql.Open("sqlite3", *tilesPath)
	if err != nil {
		log.Fatal(err)
	}

	db, err := leveldb.OpenFile(*dbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := database.Query("SELECT * FROM map")
	if err != nil {
		log.Fatal(err)
	}
	var zoom, column, row int
	var tileID, gridID, key string
	for rows.Next() {
		rows.Scan(&zoom, &column, &row, &tileID, &gridID)
		key = fmt.Sprintf("%c%d/%d/%d", insideout.TilesURLPrefix, zoom, column, row)
		if err = db.Put([]byte(key), []byte(tileID), nil); err != nil {
			log.Fatal(err)
		}
	}

	rows, err = database.Query("SELECT * FROM images")
	if err != nil {
		log.Fatal(err)
	}

	var tileData []byte
	for rows.Next() {
		rows.Scan(&tileData, &tileID)
		key = fmt.Sprintf("%c%s", insideout.TilesPrefix, tileID)
		if err = db.Put([]byte(key), tileData, nil); err != nil {
			log.Fatal(err)
		}
	}

	if err := db.CompactRange(util.Range{}); err != nil {
		log.Fatal(err)
	}
}
