{-# LANGUAGE DataKinds, TypeOperators, ConstraintKinds, FlexibleContexts,
             TemplateHaskell, GeneralizedNewtypeDeriving, StandaloneDeriving #-}
{-# OPTIONS -fno-warn-orphans #-}
module Servant.Async.Utils ( module Servant.Async.Utils, trace ) where

import Control.Concurrent.MVar (newMVar, takeMVar, putMVar)
import Data.Aeson
import Data.Aeson.Types
import Data.Maybe
import Data.Monoid
import Data.Swagger
import Data.Text (Text)
import qualified Data.Text as T
import Debug.Trace
import Web.FormUrlEncoded
import Servant

(</>) :: String -> String -> String
"" </> x  = x
x  </> "" = x
x  </> y  = x ++ "/" ++ y

type Endom a = a -> a

nil :: Monoid m => m
nil = mempty

modifier :: Text -> String -> String
modifier pref field = T.unpack $ T.stripPrefix pref (T.pack field) ?! "Expecting prefix " <> T.unpack pref

jsonOptions :: Text -> Options
jsonOptions pref = defaultOptions
  { Data.Aeson.Types.fieldLabelModifier = modifier pref
  , Data.Aeson.Types.unwrapUnaryRecords = True
  , Data.Aeson.Types.omitNothingFields = True }

formOptions :: Text -> FormOptions
formOptions pref = defaultFormOptions
  { Web.FormUrlEncoded.fieldLabelModifier = modifier pref
  }

swaggerOptions :: Text -> SchemaOptions
swaggerOptions pref = defaultSchemaOptions
  { Data.Swagger.fieldLabelModifier = modifier pref
  , Data.Swagger.unwrapUnaryRecords = True
  }

infixr 4 ?|

-- Reverse infix form of "fromMaybe"
(?|) :: Maybe a -> a -> a
(?|) = flip fromMaybe

infixr 4 ?!

-- Reverse infix form of "fromJust" with a custom error message
(?!) :: Maybe a -> String -> a
(?!) ma msg = ma ?| error msg

simpleStreamGenerator :: ((a -> IO ()) -> IO ()) -> StreamGenerator a
simpleStreamGenerator k = StreamGenerator $ \emit1 emit2 -> do
  emitM <- newMVar emit1
  k $ \a -> do
    emit <- takeMVar emitM
    emit a
    putMVar emitM emit2
