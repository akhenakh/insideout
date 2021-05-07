package bbolt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fxamacker/cbor"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom/encoding/geojson"
	"go.etcd.io/bbolt"

	"github.com/akhenakh/insideout"
)

var featureStoragePool = sync.Pool{
	New: func() interface{} {
		return &insideout.FeatureStorage{}
	},
}

// Storage cold storage.
type Storage struct {
	*bbolt.DB
	logger        log.Logger
	minCoverLevel int
}

var ErrStorage = errors.New("storage error")

func OperationStorageError(op string) error {
	return fmt.Errorf("OperationStorageError %w : %s", ErrStorage, op)
}

// NewStorage returns a cold storage using bboltdb.
func NewStorage(path string, logger log.Logger) (*Storage, func() error, error) {
	// Creating DB
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	return &Storage{
		DB:     db,
		logger: logger,
	}, db.Close, nil
}

// NewROStorage returns a read only storage using bboltdb.
func NewROStorage(path string, logger log.Logger) (*Storage, func() error, error) {
	// Creating DB
	db, err := bbolt.Open(path, 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open DB for reading at %s: %w", path, err)
	}

	s := &Storage{
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

// LoadFeature loads one feature from the DB.
func (s *Storage) LoadFeature(id uint32) (*insideout.Feature, error) {
	fs := &insideout.FeatureStorage{}

	err := s.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte{insideout.FeaturePrefix()})
		k := insideout.FeatureKey(id)
		v := b.Get(k)
		if v == nil {
			return OperationStorageError(fmt.Sprintf("feature id not found: %d", id))
		}

		dec := cbor.NewDecoder(bytes.NewReader(v))

		return dec.Decode(fs)
	})
	if err != nil {
		return nil, err
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

// LoadAllFeatures loads FeatureStorage from DB into idx
// only useful to fill in memory shapeindex.
func (s *Storage) LoadAllFeatures(add func(*insideout.FeatureStorage, uint32) error) error {
	err := s.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte{insideout.FeaturePrefix()}).Cursor()
		prefix := []byte{insideout.FeaturePrefix()}
		for key, value := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = c.Next() {
			id := binary.BigEndian.Uint32(key[1:])

			dec := cbor.NewDecoder(bytes.NewReader(value))
			fs, ok := featureStoragePool.Get().(*insideout.FeatureStorage)
			if !ok {
				return OperationStorageError("invalid data from db")
			}
			if err := dec.Decode(fs); err != nil {
				featureStoragePool.Put(fs)

				return err
			}

			if err := add(fs, id); err != nil {
				featureStoragePool.Put(fs)

				return err
			}
			featureStoragePool.Put(fs)
		}

		return nil
	})

	return err
}

// LoadFeaturesCells loads CellsStorage from DB into idx
// only useful to fill in memory tree indexes.
func (s *Storage) LoadFeaturesCells(add func([]s2.CellUnion, []s2.CellUnion, uint32)) error {
	err := s.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte{insideout.CellPrefix()}).Cursor()
		prefix := []byte{insideout.CellPrefix()}

		for key, value := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = c.Next() {
			// read back FeatureStorage
			id := binary.BigEndian.Uint32(key[1:])
			dec := cbor.NewDecoder(bytes.NewReader(value))
			cs := &insideout.CellsStorage{}
			if err := dec.Decode(cs); err != nil {
				return err
			}

			add(cs.CellsIn, cs.CellsOut, id)
		}

		return nil
	})

	return err
}

// LoadMapInfos loads map infos from the DB if any.
func (s *Storage) LoadMapInfos() (*insideout.MapInfos, bool, error) {
	var mapInfos *insideout.MapInfos

	err := s.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(insideout.MapKey())
		if b == nil {
			return nil
		}
		value := b.Get(insideout.MapKey())
		if value == nil {
			return nil
		}
		mapInfos = &insideout.MapInfos{}
		dec := cbor.NewDecoder(bytes.NewReader(value))

		return dec.Decode(mapInfos)
	})
	if err != nil {
		return nil, false, err
	}

	if mapInfos == nil {
		return nil, false, nil
	}

	return mapInfos, true, nil
}

// LoadIndexInfos loads index infos from the DB.
func (s *Storage) LoadIndexInfos() (*insideout.IndexInfos, error) {
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

// LoadCellStorage loads cell storage from.
func (s *Storage) LoadCellStorage(id uint32) (*insideout.CellsStorage, error) {
	// get the s2 cells from the index
	cs := &insideout.CellsStorage{}
	err := s.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte{insideout.CellPrefix()})
		v := b.Get(insideout.CellKey(id))
		dec := cbor.NewDecoder(bytes.NewReader(v))

		return dec.Decode(cs)
	})

	return cs, err
}

func (s *Storage) StabDB(lat, lng float64, stopOnInsideFound bool) (insideout.IndexResponse, error) {
	var idxResp insideout.IndexResponse

	ll := s2.LatLngFromDegrees(lat, lng)
	p := s2.PointFromLatLng(ll)
	c := s2.CellIDFromLatLng(ll)
	cLookup := s2.CellFromPoint(p).ID().Parent(s.minCoverLevel)
	mi := make(map[insideout.FeatureIndexResponse]struct{})
	startKey, stopKey := insideout.InsideRangeKeys(cLookup)

	err := s.View(func(tx *bbolt.Tx) error {
		curs := tx.Bucket([]byte{insideout.CellPrefix()}).Cursor()

		for k, v := curs.Seek(startKey); k != nil && bytes.Compare(k, stopKey) <= 0; k, v = curs.Next() {
			cr := s2.CellID(binary.BigEndian.Uint64(k[1:]))
			if !cr.Contains(c) {
				continue
			}
			// read back the feature id and polygon index uint32 + uint16
			for i := 0; i < len(v); i += 4 + 2 {
				res := insideout.FeatureIndexResponse{}
				res.ID = binary.BigEndian.Uint32(v[i : i+4])
				res.Pos = binary.BigEndian.Uint16(v[i+4:])
				mi[res] = struct{}{}
				if stopOnInsideFound {
					idxResp.IDsInside = append(idxResp.IDsInside, res)

					return nil
				}
			}
		}

		return nil
	})
	if err != nil {
		return idxResp, fmt.Errorf("while iterating over keys: %w", err)
	}

	if len(idxResp.IDsInside) > 0 && stopOnInsideFound {
		return idxResp, nil
	}

	// dedup
	for res := range mi {
		idxResp.IDsInside = append(idxResp.IDsInside, res)
	}

	startKey, stopKey = insideout.OutsideRangeKeys(cLookup)
	mo := make(map[insideout.FeatureIndexResponse]struct{})

	err = s.View(func(tx *bbolt.Tx) error {
		curs := tx.Bucket([]byte{insideout.CellPrefix()}).Cursor()
		for k, v := curs.Seek(startKey); k != nil && bytes.Compare(k, stopKey) <= 0; k, v = curs.Next() {
			cr := s2.CellID(binary.BigEndian.Uint64(k[1:]))
			if !cr.Contains(c) {
				continue
			}
			// read back the feature id and polygon index uint32 + uint16
			for i := 0; i < len(v); i += 4 + 2 {
				res := insideout.FeatureIndexResponse{}
				res.ID = binary.BigEndian.Uint32(v[i : i+4])
				res.Pos = binary.BigEndian.Uint16(v[i+4:])
				// remove any answer matching inside
				if _, ok := mi[res]; !ok {
					mo[res] = struct{}{}
				}
			}
		}

		return nil
	})
	if err != nil {
		return idxResp, err
	}

	// dedup
	for res := range mo {
		idxResp.IDsMayBeInside = append(idxResp.IDsMayBeInside, res)
	}

	return idxResp, nil
}

func (s *Storage) Index(fc geojson.FeatureCollection, icoverer *s2.RegionCoverer, ocoverer *s2.RegionCoverer,
	warningCellsCover int, fileName, version string) error {
	var count uint32

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

	for _, f := range fc.Features {
		f := f
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
					// value is the feature id: current count, the polygon index in a multipolygon: fi
					v := make([]byte, 6)
					binary.BigEndian.PutUint32(v, count)
					binary.BigEndian.PutUint16(v[4:], uint16(fi))
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
					v := make([]byte, 6)
					binary.BigEndian.PutUint32(v, count)
					binary.BigEndian.PutUint16(v[4:], uint16(fi))
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
		if err := s.writeFeature(f, count, cui, cuo); err != nil {
			return fmt.Errorf("can't store featrure into DB: %w", err)
		}

		level.Debug(s.logger).Log(
			"msg", "stored feature",
			"feature_properties", f.Properties,
		)

		count++
	}

	return s.writeInfos(icoverer, ocoverer, count, fileName, version)
}

func (s *Storage) writeFeature(f *geojson.Feature, id uint32, cui, cuo []s2.CellUnion) error {
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
		err := bucket.Put(insideout.FeatureKey(id), b.Bytes())
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
		err = bucket.Put(insideout.CellKey(id), b.Bytes())
		if err != nil {
			return err
		}

		level.Debug(s.logger).Log(
			"msg", "stored FeatureStorage",
			"feature_properties", f.Properties,
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

func (s *Storage) writeInfos(icoverer *s2.RegionCoverer, ocoverer *s2.RegionCoverer,
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
