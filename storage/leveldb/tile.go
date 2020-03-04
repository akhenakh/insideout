package leveldb

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/akhenakh/insideout"
)

// TilesHandler serves the mbtiles at /debug/tiles/11/618/722.pbf
func (s *Storage) TilesHandler(w http.ResponseWriter, req *http.Request) {
	sp := strings.Split(req.URL.Path, "/")

	if len(sp) != 6 {
		http.Error(w, "Invalid query", http.StatusBadRequest)
		return
	}
	z, _ := strconv.Atoi(sp[3])
	x, _ := strconv.Atoi(sp[4])
	y, _ := strconv.Atoi(strings.Trim(sp[5], ".pbf"))

	data, err := s.ReadTileData(uint8(z), uint64(x), uint64(1<<uint(z)-y-1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(data) == 0 {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "gzip")
	_, _ = w.Write(data)
}

// ReadTileData returns []bytes from a tile
func (s *Storage) ReadTileData(z uint8, x uint64, y uint64) ([]byte, error) {
	k := []byte(fmt.Sprintf("%c%d/%d/%d", insideout.TilesURLPrefix, z, x, y))
	v, err := s.Get(k, nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	tk := []byte{insideout.TilesPrefix}
	tk = append(tk, v...)
	v, err = s.Get(tk, nil)
	if err == leveldb.ErrNotFound {
		return nil, errors.New("can't find blob at existing entry")
	}
	return v, err
}
