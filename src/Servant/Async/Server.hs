{-# LANGUAGE ScopedTypeVariables, GeneralizedNewtypeDeriving, KindSignatures, DataKinds, DeriveGeneric, TemplateHaskell, TypeOperators, FlexibleContexts, OverloadedStrings, RankNTypes, GADTs, GeneralizedNewtypeDeriving, TypeFamilies, StandaloneDeriving, ConstraintKinds #-}
module Servant.Async.Server
  (

  -- Chans and callbacks
    Chans
  , newChans
  , serveCallbacks
  , serveApiWithCallbacks
  )
  where

import Control.Concurrent.Chan
import Control.Concurrent.MVar (newMVar)
import Control.Lens
import Control.Monad.IO.Class
import Data.Aeson
import qualified Data.Set as Set
import Network.HTTP.Client hiding (Proxy, path)
import Servant
import Servant.API.Flatten
import qualified Servant.Async.Core as Core
import Servant.Async.Core hiding (newEnv, Env)
import Servant.Async.Job
import Servant.Async.Types
import Servant.Client hiding (manager, ClientEnv)

-- See newChans
serveCallbacks :: ChansEnv -> CallbacksServer
serveCallbacks env
    =  wrap mkChanEvent
  :<|> wrap mkChanError
  :<|> wrap (mkChanResult . view job_output)

  where
    wrap :: (msg -> ChanMessage Value Value Value) -> ChanID 'Unsafe -> msg -> Handler ()
    wrap mk chanID' msg = do
      chanID <- checkID env chanID'
      item <- Core.getItem env chanID
      liftIO $ writeChan (item ^. env_item) (toJSON $ mk msg)

newChans :: URL -> IO (Chans, CallbacksServer)
newChans url = do
  env <- Core.newEnv
  pure (Chans env url, serveCallbacks env)

newClientEnv :: Manager -> Chans -> LogEvent -> IO ClientEnv
newClientEnv manager chans log_event
  = ClientEnv manager oneSecond log_event
    <$> newMVar Set.empty
    <*> pure chans
  where
    oneSecond = 1000000

type WithCallbacks api = "chans" :> Flat CallbacksAPI
                     :<|> api

-- A default setup for the typical situation where we have an
-- API together with its server which is a client for some job servers.
-- The `Callback` protocol requires a channel servers which is exposed
-- under the route `chans/`.
--
-- Arguments are:
-- * `BaseUrl`: is the server base URL required to send the callback URLs,
--   to callback job servers.
-- * `Manager`: the HTTP client manager
-- * `LogEvent`: how to process events produced by the clients.
-- * `Env -> Server api`: the API server which can use `runJobM`.
serveApiWithCallbacks :: forall api. HasServer api '[]
                      => Proxy api
                      -> BaseUrl
                      -> Manager
                      -> LogEvent
                      -> (ClientEnv -> Server api)
                      -> IO Application
serveApiWithCallbacks _ url manager log_event s = do
  (chans, callbacksServer) <- newChans $ mkURL url "chans"
  client_env <- newClientEnv manager chans log_event
  pure $ serve (Proxy :: Proxy (WithCallbacks api))
               (callbacksServer :<|> s client_env)
