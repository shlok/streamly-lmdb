-- Much of the code below was obtained from the lmdb library:
-- https://hackage.haskell.org/package/lmdb (https://hackage.haskell.org/package/lmdb-0.2.5/src/LICENSE)
module Streamly.External.LMDB.Internal.Foreign where

#include <lmdb.h>

import Control.Exception (Exception, throwIO)
import Control.Monad (when)
import Foreign ((.|.), Ptr, Storable (alignment, peek, peekByteOff,
                poke, pokeByteOff, sizeOf), Word16, Word32, alloca, nullPtr)
import Foreign.C.String (CString, peekCString, withCString)
import Foreign.C.Types (CChar, CInt (CInt), CSize (CSize), CUInt (CUInt))

import qualified Data.List as L

type MDB_mode_t = #type mdb_mode_t
type MDB_dbi_t = #type MDB_dbi
type MDB_cursor_op_t = #type MDB_cursor_op

data MDB_env
data MDB_txn
data MDB_cursor

data MDB_val = MDB_val
    { mv_size :: {-# UNPACK #-} !CSize
    , mv_data :: {-# UNPACK #-} !(Ptr CChar) }

instance Storable MDB_val where
    alignment _ = #{alignment MDB_val}
    {-# INLINE alignment #-}
    sizeOf _ = #{size MDB_val}
    {-# INLINE sizeOf #-}
    peek ptr = do
        sz <- #{peek MDB_val, mv_size} ptr
        pd <- #{peek MDB_val, mv_data} ptr
        return $! MDB_val sz pd
    {-# INLINE peek #-}
    poke ptr (MDB_val sz pd) = do
        #{poke MDB_val, mv_size} ptr sz
        #{poke MDB_val, mv_data} ptr pd
    {-# INLINE poke #-}

foreign import ccall unsafe "lmdb.h mdb_strerror"
    c_mdb_strerror :: CInt -> IO CString

foreign import ccall unsafe "lmdb.h mdb_env_create"
    c_mdb_env_create :: Ptr (Ptr MDB_env) -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_env_set_mapsize"
    c_mdb_env_set_mapsize :: Ptr MDB_env -> CSize -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_env_set_maxreaders"
    c_mdb_env_set_maxreaders :: Ptr MDB_env -> CUInt -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_env_set_maxdbs"
    c_mdb_env_set_maxdbs :: Ptr MDB_env -> MDB_dbi_t -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_env_open"
    c_mdb_env_open :: Ptr MDB_env -> CString -> CUInt -> MDB_mode_t -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_txn_begin"
    c_mdb_txn_begin :: Ptr MDB_env -> Ptr MDB_txn -> CUInt -> Ptr (Ptr MDB_txn) -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_dbi_open"
    c_mdb_dbi_open :: Ptr MDB_txn -> CString -> CUInt -> Ptr MDB_dbi_t -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_txn_commit"
    c_mdb_txn_commit :: Ptr MDB_txn -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_txn_abort"
    c_mdb_txn_abort :: Ptr MDB_txn -> IO ()

foreign import ccall unsafe "lmdb.h mdb_cursor_open"
    c_mdb_cursor_open :: Ptr MDB_txn -> MDB_dbi_t -> Ptr (Ptr MDB_cursor) -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_cursor_get"
    c_mdb_cursor_get :: Ptr MDB_cursor -> Ptr MDB_val -> Ptr MDB_val -> MDB_cursor_op_t -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_cursor_close"
    c_mdb_cursor_close :: Ptr MDB_cursor -> IO ()

foreign import ccall unsafe "lmdb.h mdb_get"
    c_mdb_get :: Ptr MDB_txn -> MDB_dbi_t -> Ptr MDB_val -> Ptr MDB_val -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_put"
    c_mdb_put :: Ptr MDB_txn -> MDB_dbi_t -> Ptr MDB_val -> Ptr MDB_val -> CUInt -> IO CInt

foreign import ccall unsafe "streamly_lmdb_foreign.h mdb_put_"
    c_mdb_put_ :: Ptr MDB_txn -> MDB_dbi_t -> Ptr CChar -> CSize -> Ptr CChar -> CSize -> CUInt -> IO CInt

foreign import ccall unsafe "lmdb.h mdb_drop"
    c_mdb_drop :: Ptr MDB_txn -> MDB_dbi_t -> CInt -> IO CInt

data LMDB_Error = LMDB_Error
    { e_context     :: String
    , e_description :: String
    , e_code        :: Either Int MDB_ErrCode
    } deriving (Show)
instance Exception LMDB_Error

data MDB_ErrCode
    = MDB_KEYEXIST
    | MDB_NOTFOUND
    | MDB_PAGE_NOTFOUND
    | MDB_CORRUPTED
    | MDB_PANIC
    | MDB_VERSION_MISMATCH
    | MDB_INVALID
    | MDB_MAP_FULL
    | MDB_DBS_FULL
    | MDB_READERS_FULL
    | MDB_TLS_FULL
    | MDB_TXN_FULL
    | MDB_CURSOR_FULL
    | MDB_PAGE_FULL
    | MDB_MAP_RESIZED
    | MDB_INCOMPATIBLE
    | MDB_BAD_RSLOT
    | MDB_BAD_TXN
    | MDB_BAD_VALSIZE
    | MDB_BAD_DBI
    deriving (Eq, Show)

{-# INLINE errCodes #-}
errCodes :: [(MDB_ErrCode, Int)]
errCodes =
    [ (MDB_KEYEXIST, #const MDB_KEYEXIST)
    , (MDB_NOTFOUND, #const MDB_NOTFOUND)
    , (MDB_PAGE_NOTFOUND, #const MDB_PAGE_NOTFOUND)
    , (MDB_CORRUPTED, #const MDB_CORRUPTED)
    , (MDB_PANIC, #const MDB_PANIC)
    , (MDB_VERSION_MISMATCH, #const MDB_VERSION_MISMATCH)
    , (MDB_INVALID, #const MDB_INVALID)
    , (MDB_MAP_FULL, #const MDB_MAP_FULL)
    , (MDB_DBS_FULL, #const MDB_DBS_FULL)
    , (MDB_READERS_FULL, #const MDB_READERS_FULL)
    , (MDB_TLS_FULL, #const MDB_TLS_FULL)
    , (MDB_TXN_FULL, #const MDB_TXN_FULL)
    , (MDB_CURSOR_FULL, #const MDB_CURSOR_FULL)
    , (MDB_PAGE_FULL, #const MDB_PAGE_FULL)
    , (MDB_MAP_RESIZED, #const MDB_MAP_RESIZED)
    , (MDB_INCOMPATIBLE, #const MDB_INCOMPATIBLE)
    , (MDB_BAD_RSLOT, #const MDB_BAD_RSLOT)
    , (MDB_BAD_TXN, #const MDB_BAD_TXN)
    , (MDB_BAD_VALSIZE, #const MDB_BAD_VALSIZE)
    , (MDB_BAD_DBI, #const MDB_BAD_DBI) ]

{-# INLINE numToErrVal #-}
numToErrVal :: Int -> Either Int MDB_ErrCode
numToErrVal code =
    case L.find ((== code) . snd) errCodes of
        Nothing -> Left code
        Just (ec,_) -> Right ec

{-# INLINE throwLMDBErrNum #-}
throwLMDBErrNum :: String -> CInt -> IO noReturn
throwLMDBErrNum context errNum = do
    desc <- peekCString =<< c_mdb_strerror errNum
    throwIO $! LMDB_Error
        { e_context = context
        , e_description = desc
        , e_code = numToErrVal (fromIntegral errNum) }

mdb_notfound :: CInt
mdb_notfound = #const MDB_NOTFOUND

mdb_rdonly :: CUInt
mdb_rdonly = #const MDB_RDONLY

mdb_notls :: CUInt
mdb_notls = #const MDB_NOTLS

mdb_nosubdir :: CUInt
mdb_nosubdir = #const MDB_NOSUBDIR

mdb_nooverwrite :: CUInt
mdb_nooverwrite = #const MDB_NOOVERWRITE

mdb_append :: CUInt
mdb_append = #const MDB_APPEND

mdb_create :: CUInt
mdb_create = #const MDB_CREATE

combineOptions :: [CUInt] -> CUInt
combineOptions = foldr (.|.) 0

mdb_first :: MDB_cursor_op_t
mdb_first = #const MDB_FIRST

mdb_last :: MDB_cursor_op_t
mdb_last = #const MDB_LAST

mdb_next :: MDB_cursor_op_t
mdb_next = #const MDB_NEXT

mdb_prev :: MDB_cursor_op_t
mdb_prev = #const MDB_PREV

mdb_set_range :: MDB_cursor_op_t
mdb_set_range = #const MDB_SET_RANGE

mdb_env_create :: IO (Ptr MDB_env)
mdb_env_create = do
    alloca $ \ppenv -> c_mdb_env_create ppenv >>= \rc ->
        if rc /= 0 then throwLMDBErrNum "mdb_env_create" rc else peek ppenv

mdb_env_set_mapsize :: Ptr MDB_env -> Int -> IO ()
mdb_env_set_mapsize penv size =
    c_mdb_env_set_mapsize penv (fromIntegral size) >>= \rc ->
        when (rc /= 0) $ throwLMDBErrNum "mdb_env_set_mapsize" rc

mdb_env_set_maxdbs :: Ptr MDB_env -> Int -> IO ()
mdb_env_set_maxdbs penv num =
    c_mdb_env_set_maxdbs penv (fromIntegral num) >>= \rc ->
        when (rc /= 0) $ throwLMDBErrNum "mdb_env_set_maxdbs" rc

mdb_env_set_maxreaders :: Ptr MDB_env -> Int -> IO ()
mdb_env_set_maxreaders penv num =
    c_mdb_env_set_maxreaders penv (fromIntegral $ num) >>= \rc ->
        when (rc /= 0) $ throwLMDBErrNum "mdb_env_set_maxreaders" rc

mdb_env_open :: Ptr MDB_env -> FilePath -> CUInt -> IO ()
mdb_env_open penv path flags =
    withCString path $ \cpath ->
        c_mdb_env_open penv cpath flags 0o660 >>= \rc ->
            when (rc /= 0) $ throwLMDBErrNum "mdb_env_open" rc

mdb_txn_begin :: Ptr MDB_env -> Ptr MDB_txn -> CUInt -> IO (Ptr MDB_txn)
mdb_txn_begin penv parent flags =
    alloca $ \pptxn -> c_mdb_txn_begin penv parent flags pptxn >>= \rc ->
        if rc /= 0 then throwLMDBErrNum "mdb_txn_begin" rc else peek pptxn

-- If the commit fails, aborts the transaction.
mdb_txn_commit :: Ptr MDB_txn -> IO ()
mdb_txn_commit ptxn =
    c_mdb_txn_commit ptxn >>= \rc ->
        when (rc /= 0) $ c_mdb_txn_abort ptxn >> throwLMDBErrNum "mdb_txn_commit" rc

mdb_cursor_open :: Ptr MDB_txn -> MDB_dbi_t -> IO (Ptr MDB_cursor)
mdb_cursor_open ptxn dbi =
    alloca $ \ppcurs -> c_mdb_cursor_open ptxn dbi ppcurs >>= \rc ->
        if rc /= 0 then c_mdb_txn_abort ptxn >> throwLMDBErrNum "mdb_cursor_open" rc else peek ppcurs

mdb_dbi_open :: Ptr MDB_txn -> Maybe String -> CUInt -> IO MDB_dbi_t
mdb_dbi_open ptxn name flags = do
    withCStringMaybe name $ \cname ->
        alloca $ \pdbi -> c_mdb_dbi_open ptxn cname flags pdbi >>= \rc ->
            if rc /= 0 then c_mdb_txn_abort ptxn >> throwLMDBErrNum "mdb_dbi_open" rc else peek pdbi

{-# INLINE mdb_put #-}
mdb_put :: Ptr MDB_txn -> MDB_dbi_t -> Ptr MDB_val -> Ptr MDB_val -> CUInt -> IO ()
mdb_put ptxn dbi pk pv flags =
    c_mdb_put ptxn dbi pk pv flags >>= \rc ->
        when (rc /= 0) $ throwLMDBErrNum "mdb_put" rc

{-# INLINE mdb_put_ #-}
mdb_put_ :: Ptr MDB_txn -> MDB_dbi_t -> Ptr CChar -> CSize -> Ptr CChar -> CSize -> CUInt -> IO ()
mdb_put_ ptxn dbi pk ks pv vs flags =
    c_mdb_put_ ptxn dbi pk ks pv vs flags >>= \rc ->
        when (rc /= 0) $ throwLMDBErrNum "mdb_put_" rc

mdb_clear :: Ptr MDB_txn -> MDB_dbi_t -> IO ()
mdb_clear ptxn dbi =
    c_mdb_drop ptxn dbi 0 >>= \rc ->
        when (rc /= 0) $ throwLMDBErrNum "mdb_clear" rc

-- | Use a nullable CString.
withCStringMaybe :: Maybe String -> (CString -> IO a) -> IO a
withCStringMaybe Nothing f = f nullPtr
withCStringMaybe (Just s) f = withCString s f
