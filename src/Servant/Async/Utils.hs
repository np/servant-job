{-# LANGUAGE DataKinds, TypeOperators, ConstraintKinds, FlexibleContexts,
             TemplateHaskell, GeneralizedNewtypeDeriving, StandaloneDeriving #-}
{-# OPTIONS -fno-warn-orphans #-}
module Servant.Async.Utils ( module Servant.Async.Utils, trace ) where

import Data.Aeson
import Data.Aeson.Types
import Data.Maybe
import Data.Monoid
import Data.Swagger
import Data.Text (Text)
import qualified Data.Text as T
import Debug.Trace

nil :: Monoid m => m
nil = mempty

modifier :: Text -> String -> String
modifier pref field = T.unpack $ T.stripPrefix pref (T.pack field) ?! "Expecting prefix " <> T.unpack pref

jsonOptions :: Text -> Options
jsonOptions pref = defaultOptions
  { Data.Aeson.Types.fieldLabelModifier = modifier pref
  , Data.Aeson.Types.unwrapUnaryRecords = True
  , Data.Aeson.Types.omitNothingFields = True }

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
