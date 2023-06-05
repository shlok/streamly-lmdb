module Streamly.External.LMDB.Internal.Error where

import Control.Exception
import Text.Printf

data Error = Error !String !String

instance Show Error where
  show (Error ctx msg) = printf "streamly-lmdb; %s; %s" ctx msg

instance Exception Error

throwError :: String -> String -> m a
throwError ctx msg = throw $ Error ctx msg
