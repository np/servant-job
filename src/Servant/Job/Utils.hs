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

import Control.Concurrent (forkFinally)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Control.Monad.IO.Class
import Data.Aeson hiding (Error)
import Data.Aeson.Types hiding (Error)
import Data.Maybe
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
  , Data.Aeson.Types.omitNothingFields = True
  }

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

data StepA a = StopA
             | YieldA a
             | ErrorA String
          -- | SkipA
          -- not yet needed

fromActionStepA :: Functor m => m (StepA a) -> StepT m a
fromActionStepA action = loop where
    loop = Effect $ step <$> action
    step StopA      = Stop
    step (ErrorA s) = Error s
  --step SkipA      = Skip loop
    step (YieldA a) = Yield a loop

simpleStreamGenerator :: MonadIO m => ((a -> IO ()) -> IO ()) -> SourceT m a
simpleStreamGenerator k = SourceT $ \k' -> do
  v <- liftIO $ newEmptyMVar
  let
    act = do k (putMVar v . YieldA)
             putMVar v StopA
    and_then = either (putMVar v . ErrorA . show) pure
  _ <- liftIO $ forkFinally act and_then
  k' . fromActionStepA . liftIO $ takeMVar v
