## Strategy

- full s2 index, really fast, huge memory consumption (communes france dataset)
- Inside Tree + in memory Loops
- On disk index

## K/V Engines
Several engines have been tested: bbolt, pogreb, badger 1.6, goleveldb.

For insideout particular load (read only random reads), bbolt is the best performer.

Test with fr-communes usind db engines & insidetree when available:

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

Pogreb is faster but does not supports prefix range and consume a bit more than bbolt.

bbolt is more capable for this load.