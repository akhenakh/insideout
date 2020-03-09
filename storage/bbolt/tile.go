package bbolt

import (
	"errors"
	"fmt"

	"go.etcd.io/bbolt"

	"github.com/akhenakh/insideout"
)

// ReadTileData returns []bytes from a tile
func (s *Storage) ReadTileData(z uint8, x uint64, y uint64) ([]byte, error) {
	var v []byte
	err := s.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(insideout.MapKey())

		k := []byte(fmt.Sprintf("%c%d/%d/%d", insideout.TilesURLPrefix, z, x, y))
		v = b.Get(k)
		if v == nil {
			return nil
		}

		tk := []byte{insideout.TilesPrefix}
		tk = append(tk, v...)
		v = b.Get(tk)
		if v == nil {
			return errors.New("can't find blob at existing entry")
		}
		return nil
	})

	return v, err
}
