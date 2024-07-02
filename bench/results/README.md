* Date: 2024-07-01.
* Machine: NixOS 22.11; Intel i7-12700K (3.6 GHz, 12 cores); Corsair VENGEANCE LPX DDR4 RAM 64GB (2 x 32GB) 3200MHz; Samsung 970 EVO Plus SSD 2TB (M.2 NVMe).
* Benchmark time: 4.56 hours.
* Notes:
    - Reading: `withReadOnlyTransaction` seems to have around a 30 ns/pair overhead; we haven’t investigated the reasons yet. (It might be related to `MonadBaseControl`.)
    - Writing: It turns out there is no clear performance benefit to chunking the upstream workload over using `chunkPairs/writeLMDBChunk`. (I.e., those intermediate sequences are not a bottleneck.)
    - Writing: `unsafe` FFI calls degrade performance for plain IO code; we haven’t investigated the reasons yet. (It might be related to GC getting blocked.) On the other hand, `unsafe` FFI calls improve performance by *hundreds* (as opposed to just tens, as for reading) of ns/pair for `streamly-lmdb`; we haven’t investigated the reasons yet. (It might be related to the periodic write transaction begin/commit or that for each transaction we spawn a bound thread.)
    - We don’t discuss the use of `unsafe` FFI calls in `../README.md` because using them for I/O is likely a “wrong” thing to do to begin with as they block GC and other threads. (This is also why we kept them only as an internal `streamly-lmdb` functionality.)
