## 0.8.0

* Updated for Streamly 0.10.0.
* Added proper concurrency support, including safety upon asynchronous exceptions. (The previous versions were unreliable in concurrent settings—segfaults, deadlocks, etc.)
* A byproduct of the previous point: `writeLMDB` no longer maintains its own read-write transactions under the hood.
* Added the ability to use read-write transactions for `readLMDB`.
* Added the ability to stream unexpected key-value pairs in `writeLMDB` into a separate fold.
* Added `getLMDB` for getting a single key-value pair normally (without streaming).
* Added `writeLMDBChunk` for writing key-value pairs normally (without streaming).
* Added a `chunkPairs` convenience function that allows chunking a stream of key-value pairs by bytes or number of pairs.
* Improved documentation, esp. regarding caveats for long-lived transactions and various lower-level LMDB requirements.

## 0.7.0

* Added `readUnsafeFFI` and `writeUnsafeFFI` options.

## 0.6.0

* Updated for Streamly 0.9.0.

## 0.5.0

* Added the ability to close databases and environments.

## 0.4.0

* Added support for precreating read-only transactions.

## 0.3.0

* Updated for Streamly 0.8.0.

## 0.2.1

* Allow QuickCheck <2.15 and tasty <1.5.

## 0.2.0

* Added read options for backward reading and starting from a specific key.

## 0.1.0

* Added a write option that disallows overwriting but does not throw an error when attempting to write a key-value pair that already exists in the database.

## 0.0.1.1

* Fixed `install-includes` and `include-dirs` in the Cabal file.
* Added safety check for bound threads in `writeLMDB`.

## 0.0.1

* Initial release.
