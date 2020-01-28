package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/akhenakh/oureadb/index/geodata"
	"github.com/fxamacker/cbor"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom/encoding/geojson"
)

type SIndex struct {
	Properties map[string]interface{}
	CellsIn    []s2.CellID
	CellsOut   []s2.CellID
	Coords     []float64
}

func main() {
	var fc geojson.FeatureCollection

	file, err := os.Open("../../GHSL_FUA_2019_b_single_fixed.geojson")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&fc)
	if err != nil {
		log.Fatal(err)
	}

	icoverer := &s2.RegionCoverer{MaxLevel: 20, MaxCells: 16}
	ocoverer := &s2.RegionCoverer{MaxLevel: 15, MaxCells: 2000}

	out, err := os.Create("out.data")
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	gd := &geodata.GeoData{}

	enc := cbor.NewEncoder(out, cbor.CanonicalEncOptions())
	// // Start indefinite length array encoding.
	// if err := enc.StartIndefiniteArray(); err != nil {
	// 	log.Fatal(err)
	// }

	for _, f := range fc.Features {
		err = geodata.GeoJSONFeatureToGeoData(f, gd)
		if err != nil {
			log.Println(err, f.Properties)
			continue
		}
		cui, err := gd.InteriorCover(icoverer)
		if err != nil {
			log.Println(err, f.Properties)
			continue
		}

		cuo, err := gd.Cover(ocoverer)
		if err != nil {
			log.Println(err, f.Properties)
			continue
		}

		//log.Println(cui, cuo, f.Properties)
		i := &SIndex{Properties: f.Properties, CellsIn: cui, CellsOut: cuo, Coords: f.Geometry.FlatCoords()}
		// Encode array element.
		if err := enc.Encode(i); err != nil {
			log.Fatal(err)
		}

	}

	// if err := enc.EndIndefinite(); err != nil {
	// 	log.Fatal(err)
	// }

}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
