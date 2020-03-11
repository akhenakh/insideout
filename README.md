Insideout
---------

Insideout is a suit of software dedicated to give you the best performances to accomplish one operation, query for the position within one or more polygons:

- is this location in a building ?
- in which city are we in ?
- what timezone ? 
- anything where a closed polygon describes a geographical region

This is the opensourced part of a project including ready to serve docker images with embedded pre indexed datasets.

## Strategy
Several strategies are available:

- On disk index (more reads) data can't be larger than memory
- Inside Tree in memory (fast when a location is inside inside cover), data can't be larger than memory only indexes are in memory
- full s2 index, fastest but huge memory consumption, wait for start since indexation is made on start

These 3 strategies give you enough choices to perform better according to your data.

## Indexer
Choose index and tune according to your data:  
Small sparse buildings should be indexed differently than cities also use `stopOnFirstFound` if you know only one polygon is encircling a position.

```
Usage of ./cmd/indexer/indexer:
  -dbPath="inside.db": Database path
  -filePath="": FeatureCollection GeoJSON file to index
  -insideMaxCellsCover=24: Max s2 Cells count for inside cover
  -insideMaxLevelCover=16: Max s2 level for inside cover
  -insideMinLevelCover=10: Min s2 level for inside cover
  -logLevel="INFO": DEBUG|INFO|WARN|ERROR
  -outsideMaxCellsCover=16: Max s2 Cells count for outside cover
  -outsideMaxLevelCover=15: Max s2 level for outside cover
  -outsideMinLevelCover=10: Min s2 level for outside cover
  -warningCellsCover=1000: warning limit cover count
```

## Insided

```
Usage of ./cmd/insided/insided:
  -cacheCount=200: Features count to cache, 0 to disable the cache
  -dbPath="inside.db": Database path
  -grpcPort=9200: gRPC API port
  -healthPort=6666: grpc health port
  -httpAPIPort=9201: http API port
  -httpMetricsPort=8088: http port
  -logLevel="INFO": DEBUG|INFO|WARN|ERROR
  -stopOnFirstFound=false: Stop in first feature found
  -strategy="db": Strategy to use: insidetree|shapeindex|db|postgis
```

## K/V Engines

Different engines have been tested: bbolt, pogreb, badger 1.6, goleveldb.

For Insideout particular load (read only random reads), bbolt is the best performer.

Test with loadtester 10s fr-communes using db engines & insidetree when available:

```
 ./insided -stopOnFirstFound=true -strategy=db -cacheCount=0 -dbPath=../leveldbindexer/inside.db -dbEngine=leveldb
count 31083 rate mean 3108/s rate1 3110/s 99p 980665
Alloc = 13 MiB  TotalAlloc = 3686 MiB   Sys = 71 MiB    NumGC = 321

./insided -stopOnFirstFound=true -strategy=db -cacheCount=0 -dbPath=../bboltindexer/inside.db -dbEngine=bbolt
count 42190 rate mean 4219/s rate1 4211/s 99p 4760278
Alloc = 1 MiB   TotalAlloc = 3479 MiB   Sys = 71 MiB    NumGC = 1635

./insided -stopOnFirstFound=true -strategy=insidetree -cacheCount=0 -dbPath=../bboltindexer/inside.db -dbEngine=bbolt 
count 42135 rate mean 4214/s rate1 4206/s 99p 2259642
Alloc = 208 MiB TotalAlloc = 3638 MiB   Sys = 411 MiB   NumGC = 29

./insided -stopOnFirstFound=true -strategy=insidetree -cacheCount=0 -dbPath=../leveldbindexer/inside.db -dbEngine=leveldb
count 41021 rate mean 4102/s rate1 4091/s 99p 13443368
Alloc = 390 MiB TotalAlloc = 3441 MiB   Sys = 480 MiB   NumGC = 22

./insided -stopOnFirstFound=true -strategy=insidetree -cacheCount=0 -dbPath=../badgerindexer/inside.db -dbEngine=badger
count 38936 rate mean 3894/s rate1 3874/s 99p 2599252
Alloc = 554 MiB TotalAlloc = 3988 MiB   Sys = 680 MiB   NumGC = 15

./insided -stopOnFirstFound=true -strategy=insidetree -cacheCount=0 -dbPath=../progrebindexer/inside.db -dbEngine=progreb
count 44853 rate mean 4485/s rate1 4476/s 99p 2374910
Alloc = 286 MiB TotalAlloc = 3954 MiB   Sys = 479 MiB   NumGC = 32
```

Pogreb is faster but does not supports prefix range and consumes a bit more than bbolt.

bbolt is more capable for this load.
