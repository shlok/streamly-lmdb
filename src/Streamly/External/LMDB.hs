module Streamly.External.LMDB
  ( -- * Acknowledgments

    -- | The functionality for the limits and getting the environment and database, in particular the
    -- idea of specifying the read-only or read-write mode at the type level, was mostly obtained
    -- from the [lmdb-simple](https://hackage.haskell.org/package/lmdb-simple) library.

    -- * Warning

    -- | Unless you know what you’re doing, please do not use other mechanisms (in addition to the
    -- public functionality of this library) in the same Haskell program to interact with your LMDB
    -- databases. (If you really want to do this, you should heed all the low-level requirements we
    -- have linked to in the source code of this library, and in general understand how the LMDB C
    -- API works with @MDB_NOTLS@ enabled.)

    -- * Environments

    -- | With LMDB, one first creates a so-called “environment,” which one can think of as a file or
    -- directory on disk.
    Environment,
    openEnvironment,
    isReadOnlyEnvironment,
    closeEnvironment,

    -- ** Limits
    Limits (..),
    defaultLimits,

    -- * Databases #databases#

    -- | After creating an environment, one creates within it one or more databases.
    Database,
    getDatabase,
    closeDatabase,

    -- * Transactions #transactions#

    -- | In LMDB, there are two types of transactions: read-only transactions and read-write
    -- transactions. On a given environment, read-only transactions do not block other transactions
    -- and read-write transactions do not block read-only transactions, but read-write transactions
    -- are serialized and block other read-write transactions.
    --
    -- Read-only transactions attain a snapshot view of the environment; this view is not affected
    -- by newer read-write transactions.
    --
    -- /Warning/: Long-lived transactions are discouraged by LMDB, and it is your responsibility as
    -- a user of this library to avoid them as necessary. The reasons are twofold: (a) The first one
    -- we already mentioned: Read-write transactions block other read-write transactions. (b) The
    -- second is more insidious: Even though read-only transactions do not block read-write
    -- transactions, read-only transactions (since they attain a snapshot view of the environment)
    -- prevent the reuse of pages freed by newer read-write transactions, so the database can grow
    -- quickly.
    Transaction (),

    -- ** Read-only transactions
    beginReadOnlyTransaction,
    abortReadOnlyTransaction,
    withReadOnlyTransaction,
    waitReaders,

    -- ** Read-write transactions
    beginReadWriteTransaction,
    abortReadWriteTransaction,
    commitReadWriteTransaction,
    withReadWriteTransaction,

    -- * Cursors
    Cursor,
    openCursor,
    closeCursor,
    withCursor,

    -- * Reading

    -- ** Stream-based reading
    readLMDB,
    unsafeReadLMDB,
    ReadOptions (..),
    defaultReadOptions,
    ReadDirection (..),
    ReadStart (..),

    -- ** Direct reading
    getLMDB,

    -- * Writing

    -- ** Stream-based writing
    writeLMDB,
    WriteOptions (..),
    defaultWriteOptions,
    OverwriteOptions (..),

    -- *** Accumulators

    -- | Accumulator types for 'OverwriteDisallow' and 'OverwriteAppend'. Various commonly used
    -- accumulators are provided as well.
    WriteAccum,
    WriteAccumWithOld,
    ShowKey,
    ShowValue,
    writeAccumThrow,
    writeAccumThrowAllowSameValue,
    writeAccumIgnore,
    writeAccumStop,

    -- ** Chunked writing #chunkedwriting#
    chunkPairs,
    chunkPairsFold,
    writeLMDBChunk,

    -- * Deleting
    deleteLMDB,
    clearDatabase,

    -- * Mode
    Mode,
    ReadWrite,
    ReadOnly,
    SubMode,

    -- * Error types
    LMDB_Error (..),
    MDB_ErrCode (..),

    -- * Miscellaneous
    ChunkSize (..),
    MaybeTxn (..),
    EitherTxn (..),
    kibibyte,
    mebibyte,
    gibibyte,
    tebibyte,
  )
where

import Streamly.External.LMDB.Internal
import Streamly.External.LMDB.Internal.Foreign
