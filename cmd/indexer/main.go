package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"

	"github.com/fxamacker/cbor"
	"github.com/golang/geo/s2"
	"github.com/namsral/flag"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/twpayne/go-geom/encoding/geojson"

	"github.com/akhenakh/insideout"
)

/*
00 		85011012.19 	km2 	  	7842 km 	7842 km
01 		21252753.05 	km2 	  	3921 km 	5004 km
02 	 	5313188.26 		km2 	  	1825 km 	2489 km
03 		1328297.07 		km2 	  	1130 km 	1310 km
04  	332074.27 		km2 	  	579 km 	636 km
05 	 	83018.57 		km2 	  	287 km 	315 km
06 	    20754.64 		km2 	  	143 km 	156 km
07 		5188.66 		km2 	  	72 km 	78 km
08 	 	1297.17 		km2 	  	36 km 	39 km
09 	 	324.29 			km2 	  	18 km 	20 km
10 	 	81.07 			km2 	  	9 km 	10 km
11 	 	20.27 			km2 	  	4 km 	5 km
12 	 	5.07 			km2 	  	2 km 	2 km
13 	 	1.27 			km2 	  	1123 m 	1225 m
14 		0.32 			km2 	  	562 m 	613 m
15 	 	79172.67 		m2 	  		281 m 	306 m
16 	 	19793.17 		m2 	  		140 m 	153 m
17 	 	4948.29 		m2 	  		70 m 	77 m
18 	 	1237.07 		m2 	  		35 m 	38 m
19 	 	309.27 			m2 	  		18 m 	19 m
20 	 	77.32 			m2 	  		9 m 	10 m
21 	 	19.33 			m2 	  		4 m 	5 m
22 	 	4.83 			m2 	  		2 m 	2 m
23 	 	1.21 			m2 	  		110 cm 	120 cm
24 		0.30 			m2 	  		55 cm 	60 cm
25 	 	755.05 			cm2 	  	27 cm 	30 cm
26 	 	188.76 			cm2 	  	14 cm 	15 cm
27 	 	47.19 			cm2 	  	7 cm 	7 cm
28 	 	11.80 			cm2 	  	3 cm 	4 cm
29 	 	2.95 			cm2 	  	17 mm 	18 mm
30 	 	0.74 			cm2 	  	8 mm 	9 mm
*/

var (
	version = "no version from LDFLAGS"

	insideMaxLevelCover  = flag.Int("insideMaxLevelCover", 16, "Max s2 level for inside cover")
	insideMinLevelCover  = flag.Int("insideMinLevelCover", 0, "Min s2 level for inside cover")
	insideMaxCellsCover  = flag.Int("insideMaxCellsCover", 16, "Max s2 Cells count for inside cover")
	outsideMaxLevelCover = flag.Int("outsideMaxLevelCover", 13, "Max s2 level for outside cover")
	outsideMinLevelCover = flag.Int("outsideMinLevelCover", 0, "Min s2 level for outside cover")
	outsideMaxCellsCover = flag.Int("outsideMaxCellsCover", 16, "Max s2 Cells count for outside cover")
	warningCellsCover    = flag.Int("warningCellsCover", 1000, "warning limit cover count")

	filePath = flag.String("filePath", "", "FeatureCollection GeoJSON file to index")
	dbPath   = flag.String("dbPath", "out.db", "Database path")
)

func main() {
	flag.Parse()

	var fc geojson.FeatureCollection

	// reading GeoJSON
	file, err := os.Open(*filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&fc)
	if err != nil {
		log.Fatal(err)
	}

	icoverer := &s2.RegionCoverer{MinLevel: *insideMinLevelCover, MaxLevel: *insideMaxLevelCover, MaxCells: *insideMaxCellsCover}
	ocoverer := &s2.RegionCoverer{MinLevel: *outsideMinLevelCover, MaxLevel: *outsideMaxLevelCover, MaxCells: *outsideMaxCellsCover}

	// Creating DB
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}
	db, err := leveldb.OpenFile(*dbPath, o)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var count uint32

	for _, f := range fc.Features {
		// cover inside
		cui, err := insideout.GeoJSONCoverCellUnion(f, icoverer, true)
		if err != nil {
			log.Println(err, f.Properties)
			continue
		}

		// cover outside
		cuo, err := insideout.GeoJSONCoverCellUnion(f, ocoverer, false)
		if err != nil {
			log.Println(err, f.Properties)
			continue
		}

		batch := new(leveldb.Batch)

		// store interior cover
		for fi, cu := range cui {
			if *warningCellsCover != 0 && len(cu) > *warningCellsCover {
				log.Printf("inside cover too big %d polygon #%d %s\n", len(cui), fi, f.Properties)
				continue
			}
			for _, c := range cu {
				// value is the feature index: count, the polygon index: fi
				v := make([]byte, 6)
				binary.BigEndian.PutUint32(v, count)
				binary.BigEndian.PutUint16(v, uint16(fi))
				// append to existing if any
				ev, err := db.Get(insideout.InsideKey(c), nil)
				if err != nil {
					if err != leveldb.ErrNotFound {
						log.Fatal(err)
					}
				}
				v = append(v, ev...)
				batch.Put(insideout.InsideKey(c), v)
			}
		}

		// store outside cover
		for fi, cu := range cuo {
			if *warningCellsCover != 0 && len(cu) > *warningCellsCover {
				log.Printf("outside cover too big %d polygon #%d %s\n", len(cui), fi, f.Properties)
				continue
			}
			for _, c := range cu {
				// filter cells already indexed by inside cover
				if cui[fi].ContainsCellID(c) {
					continue
				}

				// value is the feature index: count, the polygon index: fi
				v := make([]byte, 6)
				binary.BigEndian.PutUint32(v, count)
				binary.BigEndian.PutUint16(v, uint16(fi))
				// append to existing if any
				ev, err := db.Get(insideout.OutsideKey(c), nil)
				if err != nil {
					if err != leveldb.ErrNotFound {
						log.Fatal(err)
					}
				}
				v = append(v, ev...)
				batch.Put(insideout.OutsideKey(c), v)
			}
		}
		lb, err := insideout.GeoJSONEncodeLoops(f)
		if err != nil {
			log.Fatal(err)
		}

		b := new(bytes.Buffer)
		enc := cbor.NewEncoder(b, cbor.CanonicalEncOptions())

		i := &insideout.SIndex{Properties: f.Properties, LoopsBytes: lb, CellsIn: cui}
		if err := enc.Encode(i); err != nil {
			log.Fatal(err)
		}

		// store feature
		batch.Put(insideout.FeatureKey(count), b.Bytes())

		err = db.Write(batch, nil)
		if err != nil {
			log.Fatal(err)
		}

		//log.Println(f.Properties, len(cui), len(cuo))

		count++
	}

	log.Println("Indexed", count, "features")

	err = db.CompactRange(util.Range{})
	if err != nil {
		log.Fatal(err)
	}

}
