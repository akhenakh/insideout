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
  
