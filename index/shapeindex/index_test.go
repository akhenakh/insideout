package shapeindex

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	log "github.com/go-kit/kit/log"
	"github.com/golang/geo/s2"
	"github.com/google/go-cmp/cmp"
	"github.com/twpayne/go-geom/encoding/geojson"

	"github.com/stretchr/testify/require"

	"github.com/akhenakh/insideout"
	"github.com/akhenakh/insideout/storage/bbolt"
)

func TestShapeIndex_Stab(t *testing.T) {
	shapeidx, clean := setup(t)
	defer clean()

	tests := []struct {
		name     string
		lat, lng float64
		want     insideout.IndexResponse
		wantErr  bool
	}{
		{
			"inside loop",
			47.3944602327291, -2.9924373872714556,
			insideout.IndexResponse{
				IDsInside: []insideout.FeatureIndexResponse{{
					ID:  0,
					Pos: 1,
				}},
				IDsMayBeInside: nil,
			},
			false,
		},
		{
			"outside loop",
			47.38297924900667, -2.961873380366456,
			insideout.IndexResponse{
				IDsInside:      nil,
				IDsMayBeInside: nil,
			},
			false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := shapeidx.Stab(tt.lat, tt.lng)
			if (err != nil) != tt.wantErr {
				t.Errorf("Stab() error = %v, wantErr %v", err, tt.wantErr)

				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("Stab() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func setup(t *testing.T) (*Index, func()) {
	t.Helper()

	logger := log.NewNopLogger()

	tmpFile, err := ioutil.TempFile(os.TempDir(), "insideout-test-")
	require.NoError(t, err)
	wstorage, wclose, err := bbolt.NewStorage(tmpFile.Name(), logger)
	require.NoError(t, err)

	var fc geojson.FeatureCollection

	file, err := os.Open("../testdata/poly.geojson")
	require.NoError(t, err)
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&fc)
	require.NoError(t, err)

	icoverer := &s2.RegionCoverer{
		MinLevel: 10,
		MaxLevel: 16,
		MaxCells: 24,
	}
	ocoverer := &s2.RegionCoverer{
		MinLevel: 10,
		MaxLevel: 15,
		MaxCells: 16,
	}

	err = wstorage.Index(fc, icoverer, ocoverer, 100, "poly.geojson", "unittest")
	require.NoError(t, err)

	err = wclose()
	require.NoError(t, err)

	// RO storage
	storage, bclose, err := bbolt.NewStorage(tmpFile.Name(), logger)
	require.NoError(t, err)

	shapeidx := New()
	err = storage.LoadAllFeatures(shapeidx.Add)
	require.NoError(t, err)

	return shapeidx, func() {
		bclose()
		os.Remove(tmpFile.Name())
	}
}
