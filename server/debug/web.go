// package debug expose tools to debug insideout indexed geo data
package debug

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
)

// S2CellQueryHandler returns a GeoJSON containing the cells passed in the query
// ?cells=TokenID,...
func S2CellQueryHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	sval := query.Get("cells")
	if sval == "" {
		http.Error(w, "invalid parameters", 400)
		return
	}
	cells := strings.Split(sval, ",")
	if len(cells) == 0 {
		http.Error(w, "invalid parameters", 400)
		return
	}

	cu := make(s2.CellUnion, len(cells))

	for i, cs := range cells {
		c := s2.CellIDFromToken(cs)
		cu[i] = c
	}

	w.Header().Set("Content-Type", "application/json")

	w.Write(CellUnionToGeoJSON(cu))
}

// CellUnionToGeoJSON helpers to display s2 cells on maps with GeoJSON
// exports cell union into its GeoJSON representation
func CellUnionToGeoJSON(cu s2.CellUnion) []byte {
	fc := geojson.FeatureCollection{}
	for _, cid := range cu {
		f := &geojson.Feature{}
		f.Properties = make(map[string]interface{})
		f.Properties["id"] = cid.ToToken()
		f.Properties["uid"] = strconv.FormatUint(uint64(cid), 10)
		f.Properties["str"] = cid.String()
		f.Properties["level"] = cid.Level()

		c := s2.CellFromCellID(cid)
		coords := make([]float64, 5*2)
		for i := 0; i < 4; i++ {
			p := c.Vertex(i)
			ll := s2.LatLngFromPoint(p)
			coords[i*2] = ll.Lng.Degrees()
			coords[i*2+1] = ll.Lat.Degrees()
		}
		// last is first
		coords[8], coords[9] = coords[0], coords[1]
		ng := geom.NewPolygonFlat(geom.XY, coords, []int{10})
		f.Geometry = ng
		fc.Features = append(fc.Features, f)
	}
	b, _ := fc.MarshalJSON()
	return b
}

// CellUnionToTokens a cell union to a token string list
func CellUnionToTokens(cu s2.CellUnion) []string {
	res := make([]string, len(cu))

	for i, c := range cu {
		res[i] = c.ToToken()
	}
	return res
}
