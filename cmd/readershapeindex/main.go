package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/akhenakh/oureadb/index/geodata"
	"github.com/fxamacker/cbor"
	"github.com/golang/geo/s2"
)

type SIndex struct {
	Properties map[string]interface{}
	CellsIn    []s2.CellID
	CellsOut   []s2.CellID
	Coords     []float64
}

type Feature struct {
	*s2.Loop
	Properties map[string]interface{}
	ID         int32
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// itree := insidetree.NewTree()
	// otree := insidetree.NewTree()
	sindex := s2.NewShapeIndex()
	//mprop := make(map[int32]*Feature)

	in, err := os.Open("../../out.data")
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()

	dec := cbor.NewDecoder(in)

	idx := &SIndex{}
	var current int32
	for {

		if err = dec.Decode(idx); err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}
		fmt.Println(idx.Properties)

		l := geodata.LoopFromCoordinates(idx.Coords)

		//f := &Feature{Properties: idx.Properties}
		f := &Feature{Properties: idx.Properties, Loop: l}
		sid := sindex.Add(f)
		//mprop[current] = f
		f.ID = sid

		// for _, c := range idx.CellsIn {
		// 	itree.Index(c, current)
		// }
		// for _, c := range idx.CellsOut {
		// 	otree.Index(c, current)
		// }

	}
	fmt.Println("loaded", current)

	// For info on each, see: https://golang.org/pkg/runtime/#MemStats

	lat := 47.8
	q := s2.NewContainsPointQuery(sindex, s2.VertexModelOpen)

	t := time.Now()

	pipCount := 0

	for i := 0; i < 10_000; i++ {
		p := s2.PointFromLatLng(s2.LatLngFromDegrees(lat-(float64(i)/2_000), 2.2))

		/*	c := s2.CellFromPoint(p).ID()
			res := itree.Stab(c)
			if len(res) > 0 {
				continue
			}

			res = otree.Stab(c)
			if len(res) == 0 {
				fmt.Println("no solution", c)
				continue
			}

			if len(res) > 0 {
				var pipres []int32
				for _, id := range res {
					// do pip
					idx := id.(int32)
					//s := sindex.Shape(idx)
					l := mprop[idx]
					//l := s.(*Feature)

					// fmt.Println("LOOP", idx.Properties, "TESTING", c, res, lat-(1.0/float64(i)), 2.2)
					if l.ContainsPoint(p) {
						pipres = append(pipres, id.(int32))
					}

				}
				if len(res) == 0 {
					fmt.Println("no solution", c)
					continue
				}
				fmt.Println("from PIP", pipres)
			} */
		pipCount++
		if shapes := q.ContainingShapes(p); len(shapes) == 0 {
			fmt.Println("not ContainingShapes")
		}
	}
	// 	c := s2.CellFromPoint(p).ID()
	// 	res := itree.Stab(c)
	// 	//
	// 	// for _, prop := range res {
	// 	// 	fmt.Println("found", prop)
	// 	// }
	// 	if len(res) == 0 {
	// 		res = otree.Stab(c)
	// 		if len(res) == 0 {
	// 			// fmt.Println("no solution", c)
	// 		}
	// 		if len(res) > 0 {
	// 			var pipres []uint
	// 			for _, id := range res {
	// 				// do pip
	// 				idx := mprop[id.(uint)]
	//
	// 				// fmt.Println("LOOP", idx.Properties, "TESTING", c, res, lat-(1.0/float64(i)), 2.2)
	// 				if idx.Loop != nil && idx.Loop.ContainsPoint(p) {
	// 					pipres = append(pipres, id.(uint))
	// 				}
	//
	// 			}
	// 			// fmt.Println("from PIP", pipres)
	// 		}
	//
	// 	} else {
	// 		// for _, id := range res {
	// 		// 	fmt.Println("from inside", id.(uint))
	// 		// }
	// 	}
	// }

	fmt.Println(time.Since(t), "pip", pipCount)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)

	//time.Sleep(2 * time.Minute)

}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
