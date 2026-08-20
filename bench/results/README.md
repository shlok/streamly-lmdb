* Date: 2026-08-20.
* Machine: NixOS 26.05; Intel i7-12700K (3.6 GHz, 12 cores); Corsair VENGEANCE LPX DDR4 RAM 64GB (2 x 32GB) 3200MHz; Samsung 970 EVO Plus SSD 2TB (M.2 NVMe).
* Benchmark time: 3.10 hours.
* Notes:
    - Older (2024-09-21): GHC 9.6.5 / Streamly 0.10.1 / NixOS 24.11.
    - Update/latest (2026-08-20): GHC 9.10.3 / Streamly 0.11.1 / NixOS 26.05.
    - Additional difference between older & latest: We removed the explicit GHC stream fusion-related compilation flags.
    - The total benchmark time was improved from 4.62 hours (older) to 3.10 hours (latest). We don’t know if this is due to GHC improvements, Streamly improvements, NixOS improvements, or something else.
    - Reading:
        - Older:`withReadOnlyTransaction` seemed to have around a 30 ns/pair overhead; we didn’t investigate the reasons. (It might have been related to `MonadBaseControl`.)
        - Update: This difference is gone.
    - Writing: It turns out there is no clear performance benefit to chunking the upstream workload over using `chunkPairs/writeLMDBChunk`. (I.e., those intermediate sequences are not a bottleneck.)
    - Writing:
        - Older: `unsafe` FFI calls degrade performance for plain IO code; we didn’t investigate the reasons. (It might have been related to GC getting blocked.)
        - Update: This is now opposite; `unsafe` FFI calls improve performance for plain IO code by hundreds of ns/pair. (We haven’t investigated the reasons yet.)
    - Writing: `unsafe` FFI calls improve performance by hundreds of ns/pair for `streamly-lmdb`; we haven’t investigated the reasons yet. (It might be related to the periodic write transaction begin/commit or that for each transaction we spawn a bound thread.)
    - We don’t discuss the use of `unsafe` FFI calls in `../README.md` because using them for I/O is likely a “wrong” thing to do to begin with as they block GC and other threads. (This is also why we kept them only as an internal `streamly-lmdb` functionality.)
