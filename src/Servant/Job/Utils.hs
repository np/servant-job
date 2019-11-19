{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS -fno-warn-orphans #-}
module Servant.Job.Utils ( module Servant.Job.Utils, trace ) where

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
import Servant.Types.SourceT
import Servant.Client hiding (manager, ClientEnv)

(</>) :: String -> String -> String
"" </> x  = x
x  </> "" = x
x  </> y  = x ++ "/" ++ y

extendBaseUrl :: ToHttpApiData a => a -> BaseUrl -> BaseUrl
extendBaseUrl a u =
  u { baseUrlPath = baseUrlPath u ++ "/" ++ T.unpack (toUrlPiece a) }

type Endom a = a -> a

nil :: Monoid m => m
nil = mempty

modifier :: Text -> String -> String
modifier pref field = T.unpack $ T.stripPrefix pref (T.pack field) ?! "Expecting prefix " <> T.unpack pref

jsonOptions :: Text -> Options
jsonOptions pref = defaultOptions
  { Data.Aeson.Types.fieldLabelModifier = modifier pref
  , Data.Aeson.Types.unwrapUnaryRecords = False
  , Data.Aeson.Types.omitNothingFields = True }

formOptions :: Text -> FormOptions
formOptions pref = defaultFormOptions
  { Web.FormUrlEncoded.fieldLabelModifier = modifier pref
  }

swaggerOptions :: Text -> SchemaOptions
swaggerOptions pref = defaultSchemaOptions
  { Data.Swagger.fieldLabelModifier = modifier pref
  , Data.Swagger.unwrapUnaryRecords = False
  }

infixr 4 ?|

-- Reverse infix form of "fromMaybe"
(?|) :: Maybe a -> a -> a
(?|) = flip fromMaybe

infixr 4 ?!

-- Reverse infix form of "fromJust" with a custom error message
(?!) :: Maybe a -> String -> a
(?!) ma msg = ma ?| error msg

{-
simpleStreamGeneratorIO :: ((a -> IO ()) -> IO ()) -> SourceT IO a
simpleStreamGeneratorIO k = SourceT $ \k' ->
  k' $ \
  k $ \a ->
-}
-- TODO STREAMING
simpleStreamGenerator :: ((a -> IO ()) -> IO ()) -> SourceT m a
simpleStreamGenerator = undefined
{-
simpleStreamGenerator k = SourceT $ \k' -> do
  v <- newMVar emit1
  k $ \a -> do
    s <- takeMVar v
    putMVar v (Yield a s)
  s <- takeMVar v
  k' s
-}
