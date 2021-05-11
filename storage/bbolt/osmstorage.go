package bbolt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/fxamacker/cbor"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom/encoding/geojson"
	"go.etcd.io/bbolt"

	"github.com/akhenakh/insideout"
)

// OSMStorage cold storage.
type OSMStorage struct {
	*bbolt.DB
	logger        log.Logger
	minCoverLevel int
}

// NewOSMStorage returns a cold storage using bboltdb.
func NewOSMStorage(path string, logger log.Logger) (*OSMStorage, func() error, error) {
	// Creating DB
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("can't open database %w", err)
	}

	return &OSMStorage{
		DB:     db,
		logger: logger,
	}, db.Close, nil
}

// NewOSMROStorage returns a read only storage using bboltdb.
func NewOSMROStorage(path string, logger log.Logger) (*OSMStorage, func() error, error) {
	// Creating DB
	db, err := bbolt.Open(path, 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open DB for reading at %s: %w", path, err)
	}

	s := &OSMStorage{
		DB:     db,
		logger: logger,
	}

	infos, err := s.LoadIndexInfos()
	if err != nil {
		return nil, nil, err
	}

	s.minCoverLevel = infos.MinCoverLevel

	return s, db.Close, nil
}

// LoadIndexInfos loads index infos from the DB.
func (s *OSMStorage) LoadIndexInfos() (*insideout.IndexInfos, error) {
	infos := &insideout.IndexInfos{}

	err := s.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(insideout.InfoKey())
		value := b.Get(insideout.InfoKey())
		if value == nil {
			return OperationStorageError("can't find infos entries, invalid DB")
		}
		dec := cbor.NewDecoder(bytes.NewReader(value))

		return dec.Decode(infos)
	})

	return infos, err
}

// LoadFeature loads one feature from the DB.
func (s *OSMStorage) LoadFeature(id int64) (*insideout.Feature, error) {
	fs := &insideout.FeatureStorage{}

	err := s.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte{insideout.FeaturePrefix()})
		k := insideout.OSMFeatureKey(id)
		v := b.Get(k)
		if v == nil {
			return OperationStorageError(fmt.Sprintf("feature id not found: %d", id))
		}

		dec := cbor.NewDecoder(bytes.NewReader(v))

		return dec.Decode(fs)
	})
	if err != nil {
		return nil, fmt.Errorf("error loading feature %w", err)
	}

	loops := make([]*s2.Loop, len(fs.LoopsBytes))
	for i := 0; i < len(loops); i++ {
		l := &s2.Loop{}
		if err = l.Decode(bytes.NewReader(fs.LoopsBytes[i])); err != nil {
			return nil, err
		}

		loops[i] = l
	}

	f := &insideout.Feature{
		Loops:      loops,
		Properties: fs.Properties,
	}

	return f, nil
}

// LoadFeaturesCells loads CellsStorage from DB into idx
// only useful to fill in memory tree indexes.
func (s *OSMStorage) LoadFeaturesCells(add func([]s2.CellUnion, []s2.CellUnion, int64)) error {
	err := s.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte{insideout.CellPrefix()}).Cursor()
		prefix := []byte{insideout.CellPrefix()}

		for key, value := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = c.Next() {
			// read back FeatureStorage
			id := binary.BigEndian.Uint64(key[1:])
			dec := cbor.NewDecoder(bytes.NewReader(value))
			cs := &insideout.CellsStorage{}
			if err := dec.Decode(cs); err != nil {
				return err
			}

			add(cs.CellsIn, cs.CellsOut, int64(id))
		}

		return nil
	})

	return err
}

// LoadCellStorage loads cell storage from.
func (s *OSMStorage) LoadCellStorage(id int64) (*insideout.CellsStorage, error) {
	// get the s2 cells from the index
	cs := &insideout.CellsStorage{}
	err := s.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte{insideout.CellPrefix()})
		v := b.Get(insideout.OSMCellKey(id))
		dec := cbor.NewDecoder(bytes.NewReader(v))

		return dec.Decode(cs)
	})

	return cs, err
}

func (s *OSMStorage) Index(
	fc geojson.FeatureCollection,
	icoverer *s2.RegionCoverer,
	ocoverer *s2.RegionCoverer,
	warningCellsCover int,
	fileName,
	version string,
) error {
	logger := log.With(s.logger, "component", "indexer")

	err := s.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucket(insideout.InfoKey()); err != nil {
			return err
		}
		if _, err := tx.CreateBucket([]byte{insideout.FeaturePrefix()}); err != nil {
			return err
		}
		if _, err := tx.CreateBucket([]byte{insideout.CellPrefix()}); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("can't create bucket into DB: %w", err)
	}

	var count uint32

	for _, f := range fc.Features {
		f := f

		osmI, ok := f.Properties["osm_id"]
		if !ok {
			level.Warn(logger).Log("msg", "no osm_id", "error", err, "feature_properties", f.Properties)

			continue
		}

		osmIDF, ok := osmI.(float64)
		if !ok {
			level.Warn(logger).Log(
				"msg", "can't read back osm_id",
				"error", err,
				"osm_id", osmIDF,
				"feature_properties", f.Properties,
			)

			continue
		}

		osmID := int64(osmIDF)

		// cover inside
		cui, err := insideout.GeoJSONCoverCellUnion(f, icoverer, true)
		if err != nil {
			level.Warn(logger).Log("msg", "error covering inside", "error", err, "feature_properties", f.Properties)

			continue
		}

		// cover outside
		cuo, err := insideout.GeoJSONCoverCellUnion(f, ocoverer, false)
		if err != nil {
			level.Warn(logger).Log("msg", "error covering outside", "error", err, "feature_properties", f.Properties)

			continue
		}

		// store interior cover
		err = s.Update(func(tx *bbolt.Tx) error {
			for fi, cu := range cui {
				if warningCellsCover != 0 && len(cu) > warningCellsCover {
					level.Warn(logger).Log(
						"msg", fmt.Sprintf("inside cover too big %d cells, not indexing polygon #%d %s", len(cui), fi, f.Properties),
						"feature_properties", f.Properties,
					)

					continue
				}
				for _, c := range cu {
					// value is the feature id & the polygon index in a multipolygon: fi
					v := make([]byte, 10)
					binary.BigEndian.PutUint64(v, uint64(osmID))
					binary.BigEndian.PutUint16(v[8:], uint16(fi))
					// append to existing if any
					b := tx.Bucket([]byte{insideout.CellPrefix()})
					ev := b.Get(insideout.InsideKey(c))

					if ev != nil {
						v = append(v, ev...) //nolint: makezero
					}

					err = b.Put(insideout.InsideKey(c), v)
					if err != nil {
						return err
					}
				}
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("failed set inside cover into DB: %w", err)
		}

		// store outside cover
		err = s.Update(func(tx *bbolt.Tx) error {
			for fi, cu := range cuo {
				if warningCellsCover != 0 && len(cu) > warningCellsCover {
					level.Warn(logger).Log(
						"msg", fmt.Sprintf("outisde cover too big %d not indexing polygon #%d %s", len(cui), fi, f.Properties),
						"feature_properties", f.Properties,
					)

					continue
				}
				for _, c := range cu {
					// TODO: filter cells already indexed by inside cover

					// value is the feature id: current count, the polygon index in a multipolygon: fi
					v := make([]byte, 10)
					binary.BigEndian.PutUint64(v, uint64(osmID))
					binary.BigEndian.PutUint16(v[8:], uint16(fi))
					// append to existing if any
					b := tx.Bucket([]byte{insideout.CellPrefix()})
					ev := b.Get(insideout.OutsideKey(c))
					if ev != nil {
						v = append(v, ev...) //nolint: makezero
					}

					err = b.Put(insideout.OutsideKey(c), v)
					if err != nil {
						return err
					}
				}
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("failed set outside cover into DB: %w", err)
		}

		// store feature
		if err := s.writeOSMFeature(f, int64(osmID), cui, cuo); err != nil {
			return fmt.Errorf("can't store featrure into DB: %w", err)
		}

		level.Debug(s.logger).Log(
			"msg", "Stored feature",
		)

		count++
	}

	return s.writeInfos(icoverer, ocoverer, count, fileName, version)
}

func (s *OSMStorage) writeOSMFeature(f *geojson.Feature, id int64, cui, cuo []s2.CellUnion) error {
	// store feature
	lb, err := insideout.GeoJSONEncodeLoops(f)
	if err != nil {
		return fmt.Errorf("can't encode loop: %w", err)
	}

	b := new(bytes.Buffer)
	enc := cbor.NewEncoder(b, cbor.CanonicalEncOptions())

	// TODO: filter cuo cui[fi].ContainsCellID(c)
	fs := &insideout.FeatureStorage{Properties: f.Properties, LoopsBytes: lb}
	if err := enc.Encode(fs); err != nil {
		return fmt.Errorf("can't encode FeatureStorage: %w", err)
	}

	err = s.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte{insideout.FeaturePrefix()})
		err := bucket.Put(insideout.OSMFeatureKey(id), b.Bytes())
		if err != nil {
			return err
		}
		// store cells for tree
		b = new(bytes.Buffer)
		enc = cbor.NewEncoder(b, cbor.CanonicalEncOptions())
		cs := &insideout.CellsStorage{
			CellsIn:  cui,
			CellsOut: cuo,
		}

		if err := enc.Encode(cs); err != nil {
			return fmt.Errorf("can't encode CellsStorage: %w", err)
		}

		bucket = tx.Bucket([]byte{insideout.CellPrefix()})
		err = bucket.Put(insideout.OSMCellKey(id), b.Bytes())
		if err != nil {
			return err
		}

		level.Debug(s.logger).Log(
			"msg", "stored FeatureStorage",
			"loop_count", len(fs.LoopsBytes),
			"inside_loop_id", id,
		)

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed store feature into DB: %w", err)
	}

	return nil
}

func (s *OSMStorage) writeInfos(icoverer *s2.RegionCoverer, ocoverer *s2.RegionCoverer,
	fcount uint32, fileName, version string) error {
	infoBytes := new(bytes.Buffer)

	// Finding the lowest cover level
	minCoverLevel := ocoverer.MinLevel
	if icoverer.MinLevel < ocoverer.MinLevel {
		minCoverLevel = icoverer.MinLevel
	}

	infos := &insideout.IndexInfos{
		Filename:       fileName,
		IndexTime:      time.Now(),
		IndexerVersion: version,
		FeatureCount:   fcount,
		MinCoverLevel:  minCoverLevel,
	}

	enc := cbor.NewEncoder(infoBytes, cbor.CanonicalEncOptions())
	if err := enc.Encode(infos); err != nil {
		return fmt.Errorf("failed encoding IndexInfos: %w", err)
	}

	err := s.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(insideout.InfoKey())

		return b.Put(insideout.InfoKey(), infoBytes.Bytes())
	})
	if err != nil {
		return fmt.Errorf("failed encoding IndexInfos: %w", err)
	}

	return nil
}
