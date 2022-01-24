module Streamly.External.LMDB.Internal where

import Foreign (Ptr)
import Streamly.External.LMDB.Internal.Foreign (MDB_dbi_t, MDB_env)

-- This is in a separate internal module because the tests make use of the Database constructor.

class Mode a where
  isReadOnlyMode :: a -> Bool

data ReadWrite

data ReadOnly

instance Mode ReadWrite where isReadOnlyMode _ = False

instance Mode ReadOnly where isReadOnlyMode _ = True

data Database mode = Database (Ptr MDB_env) MDB_dbi_t
