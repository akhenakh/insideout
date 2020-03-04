## Strategy

- full s2 index, really fast, huge memory consumption (communes france dataset)

  ```
   9.95088ms pip 10000
   Alloc = 954 MiB TotalAlloc = 2822 MiB   Sys = 1009 MiB  NumGC = 16
  ```
- Inside Tree + in memory Loops

  ```
  icoverer := &s2.RegionCoverer{MaxLevel: 20, MaxCells: 16}
  ocoverer := &s2.RegionCoverer{MaxLevel: 15, MaxCells: 8}
  41.929671ms pip 4548
  Alloc = 182 MiB TotalAlloc = 197 MiB    Sys = 205 MiB   NumGC = 7
  ```
- On disk index
  ```
  83.190952ms pip 3343
  Alloc = 130 MiB TotalAlloc = 400 MiB    Sys = 204 MiB   NumGC = 16
  ```
  
```
 ./insided -stopOnFirstFound=false -strategy=shapeindex  leveldb gcache
count 59623 rate mean 6165/s rate1 6227/s 99p 1056870
Alloc = 341 MiB TotalAlloc = 4828 MiB   Sys = 1350 MiB  NumGC = 33

./insided -stopOnFirstFound=false -strategy=db  leveldb gcache
count 31666 rate mean 2499/s rate1 2494/s 99p 1535310
Alloc = 24 MiB  TotalAlloc = 4393 MiB   Sys = 71 MiB    NumGC = 325

./insided -stopOnFirstFound=false -strategy=postgis  leveldb gcache
count 10401 rate mean 2766/s rate1 0/s 99p 812003
Alloc = 24 MiB  TotalAlloc = 935 MiB    Sys = 71 MiB    NumGC = 76

$ ./insided -stopOnFirstFound=false -strategy=insidetree  leveldb gcache
count 87912 rate mean 3731/s rate1 3617/s 99p 14598001
Alloc = 303 MiB TotalAlloc = 7997 MiB   Sys = 480 MiB   NumGC = 44

$ ./insided -stopOnFirstFound=false -strategy=db  leveldb ristretto
count 108059 rate mean 2520/s rate1 2512/s 99p 2161444
Alloc = 20 MiB  TotalAlloc = 15549 MiB  Sys = 71 MiB    NumGC = 1078

$ ./insided -stopOnFirstFound=false -strategy=insidetree  leveldb ristretto
count 78946 rate mean 3816/s rate1 3828/s 99p 14089667
Alloc = 412 MiB TotalAlloc = 11849 MiB  Sys = 480 MiB   NumGC = 62

```