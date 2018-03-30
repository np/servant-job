{-# LANGUAGE ScopedTypeVariables, GeneralizedNewtypeDeriving, KindSignatures, DataKinds, DeriveGeneric, TemplateHaskell, TypeOperators, FlexibleContexts, OverloadedStrings, RankNTypes, GADTs, GeneralizedNewtypeDeriving, TypeFamilies, StandaloneDeriving, ConstraintKinds #-}
module Servant.Async.Client
  ( JobsAPI
  , MonadJob
  , callJob

  , serveWithCallbacks

  , JobM
  , runJobM
  , runJobMLog

  , Chans
  , newChans
  , serveCallbacks

  , Env
  , LogEvent(..)
  , forwardInnerEvents
  , env_manager
  , env_polling_delay_ms
  , env_log_event
  , env_jobs_mvar
  , env_chans
  , newEnv

  , URL(..)
  , mkURL
  , JobServerAPI(..)
  , JobServerURL(..)
  , ClientOrServer(..)

  , JobFrame(..)
  , StreamFunctor
  , StreamJobsAPI'
  , StreamJobsAPI
  , clientStreamAPI
  , simpleStreamGenerator

  , CallbackJobsAPI
  , CallbackJobsAPI'
  , CallbackAPI
  , CallbacksAPI
  , CallbacksServer
  , CallbackInput
  , cbi_input
  , cbi_callback
  , ChanMessage(..)

  , killRunningJobs

  , clientCallback

  -- Proxies
  , callbackJobsAPI

  -- Internals
  , MonadClientJob
  , clientSyncJob
  , clientStreamJob
  , clientAsyncJob
  , clientCallbackJob'
  , clientCallbackJob
  , clientNewJob
  , clientPollJob
  , clientKillJob
  , clientWaitJob
  , fillLog
  , Event(..)
  , progress
  , RunningJob(..)
  , running_job_url
  , running_job_api
  , running_job_id
  , msg_event
  , msg_result
  , msg_error
  , mkChanEvent
  , mkChanResult
  , mkChanError
  , isTransientFailure
  , retryOnTransientFailure
  )
  where

import Control.Concurrent.Chan
import Control.Concurrent.MVar (MVar, newMVar, readMVar, modifyMVar_, takeMVar, putMVar)
import Control.Concurrent (threadDelay)
import Control.Lens
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Monad.Except
import Data.Aeson
import qualified Data.Aeson.Types as Aeson
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text as T
import GHC.Generics hiding (to)
import Network.HTTP.Client hiding (Proxy, path)
import Servant
import Servant.API.Flatten
import qualified Servant.Async.Core as Core
import Servant.Async.Core hiding (newEnv, Env)
import Servant.Async.Job
import Servant.Async.Utils
import Servant.Async.Types
import Servant.Client hiding (manager)

simpleStreamGenerator :: ((a -> IO ()) -> IO ()) -> StreamGenerator a
simpleStreamGenerator k = StreamGenerator $ \emit1 emit2 -> do
  emitM <- newMVar emit1
  k $ \a -> do
    emit <- takeMVar emitM
    emit a
    putMVar emitM emit2

clientStreamAPI :: proxy e i o
                -> Proxy (StreamJobsAPI 'Client e i o)
clientStreamAPI _ = Proxy

asyncJobsAPI :: proxy e i o -> Proxy (Flat (AsyncJobsAPI' 'Unsafe 'Unsafe '[JSON] '[JSON] e i o))
asyncJobsAPI _ = Proxy

class Monad m => MonadJob m where
  callJob :: (ToJSON e, FromJSON e, ToJSON i, FromJSON o)
          => JobServerURL e i o -> i -> m o

data ClientJobError
  = DecodingChanMessageError
  | MissingOutputError
  | FrameError String
  | ChanMessageError String
  | StartingJobError ServantError
  | WaitingJobError  ServantError
  | KillingJobError  ServantError
  | PollingJobError  ServantError
  | CallbackError    ServantError

  -- Show instance is used by `error` which is bad.
  deriving Show

data RunningJob e i o = PrivateRunningJob
  { _running_job_url :: URL
  , _running_job_api :: JobServerAPI
  , _running_job_id  :: JobID 'Unsafe
  }
  deriving (Eq, Ord, Generic)

makeLenses ''RunningJob

runningJob :: JobServerURL e i o -> JobID 'Unsafe -> RunningJob e i o
runningJob jurl jid = PrivateRunningJob (jurl ^. job_server_url) (jurl ^. job_server_api) jid

data Event e i o
  = NewTask  { _event_server :: JobServerURL e i o }
  | Started  { _event_server :: JobServerURL e i o
             , _event_job_id :: Maybe (JobID 'Unsafe) }
  | Finished { _event_server :: JobServerURL e i o
             , _event_job_id :: Maybe (JobID 'Unsafe) }
  | Event    { _event_server :: JobServerURL e i o
             , _event_job_id :: Maybe (JobID 'Unsafe)
             , _event_event  :: e }
  | BadEvent { _event_server :: JobServerURL e i o
             , _event_job_id :: Maybe (JobID 'Unsafe)
             , _event_event_value :: Value }
  | Debug e
  deriving (Generic)

instance ToJSON e => ToJSON (Event e i o) where
  toJSON = genericToJSON $ jsonOptions "_event_"

newtype LogEvent = LogEvent
  { unLogEvent :: forall e i o. ToJSON e => Event e i o -> IO () }

forwardInnerEvents :: FromJSON e => (e -> IO ()) -> LogEvent -> LogEvent
forwardInnerEvents log_value (LogEvent log_event) = LogEvent $ \event -> do
  case event of
    Event s i e' ->
      let v = toJSON e' in
      case Aeson.parseMaybe parseJSON v of
        Just e -> do
          log_value e
          log_event event
        Nothing ->
          log_event $ BadEvent s i v
    _ -> log_event event

type instance SymbolOf (Chan Value) = "chan"

type ChansEnv = Core.Env (Chan Value)

data Chans = Chans
  { _chans_env :: !ChansEnv
  , _chans_url :: !URL
  }

makeLenses ''Chans

data Env = Env
  { _env_manager          :: !Manager
  , _env_polling_delay_ms :: !Int
  , _env_log_event        :: !LogEvent
  , _env_jobs_mvar        :: !(MVar (Set (RunningJob Value Value Value)))
  , _env_chans            :: !Chans
  }

makeLenses ''Env

type MonadClientJob m = (MonadReader Env m, MonadError ClientJobError m, MonadIO m)
type M m = MonadClientJob m

-- TODO
isTransientFailure :: ClientJobError -> Bool
isTransientFailure _ = False

retryOnTransientFailure :: M m => m a -> m a
retryOnTransientFailure m = m `catchError` f
  where
    f e | isTransientFailure e = retryOnTransientFailure m
        | otherwise            = throwError e

progress :: (ToJSON e, MonadReader Env m, MonadIO m) => Event e i o -> m ()
progress event = do
  log_event <- view env_log_event
  liftIO $ unLogEvent log_event event

runClientJob :: M m => URL -> (ServantError -> ClientJobError) -> ClientM a -> m a
runClientJob url err m = do
  env <- ask
  liftIO (runClientM m (ClientEnv (env ^. env_manager) (url ^. base_url) Nothing))
    >>= either (throwError . err) pure

onRunningJob :: M m => RunningJob e i o
                    -> (forall a. Ord a => a -> Endom (Set a))
                    -> m ()
onRunningJob job f = do
  env <- ask
  liftIO . modifyMVar_ (env ^. env_jobs_mvar) $ pure . f (forgetRunningJob job)

forgetRunningJob :: RunningJob e i o -> RunningJob e' i' o'
forgetRunningJob (PrivateRunningJob u a i) = PrivateRunningJob u a i

clientSyncJob :: (ToJSON i, FromJSON e, FromJSON o, M m)
              => JobServerURL e i o -> i -> m (JobOutput o)
clientSyncJob jurl =
  runClientJob (jurl ^. job_server_url) StartingJobError . client (syncJobsAPI jurl)

clientStreamJob :: (ToJSON i, ToJSON e, FromJSON e, FromJSON o, M m)
                => JobServerURL e i o -> i -> m (JobOutput o)
clientStreamJob jurl input = do
  ResultStream k <- runClientJob (jurl ^. job_server_url) StartingJobError $
                      client (clientStreamAPI jurl) input
  LogEvent log_event <- view env_log_event
  res <- liftIO . k $ \getResult ->
    let
      onFrame (Left err) = return (Left (FrameError err))
      onFrame (Right (JobFrame me mo)) = do
        forM_ me $ log_event . Event jurl Nothing
        case mo of
          Nothing -> loop
          Just o  -> return (Right o)
      loop = do
        r <- getResult
        case r of
          Nothing -> return (Left MissingOutputError)
          Just x  -> onFrame x
    in loop
  either throwError pure res

newEventChan :: (FromJSON e, FromJSON o, M m)
             => m (ChanID 'Safe, IO (Maybe (ChanMessage e i o)))
newEventChan = do
  env <- ask
  (i, item) <- liftIO $ Core.newItem (env ^. env_chans . chans_env) newChan
  pure (i, Aeson.parseMaybe parseJSON <$> readChan (item ^. env_item))

chanURL :: Env -> ChanID 'Safe -> URL
chanURL env i = (env ^. env_chans . chans_url) & base_url %~ extend_url
  where
    extend_url x = x { baseUrlPath = baseUrlPath x ++ "/" ++ T.unpack (toUrlPiece i) }

callbackJobsAPI :: proxy e i o -> Proxy (CallbackJobsAPI e i o)
callbackJobsAPI _ = Proxy

clientCallbackJob' :: (ToJSON e, FromJSON e, FromJSON o, M m)
                   => JobServerURL e i o
                   -> (URL -> ClientM ())
                   -> m (JobOutput o)
clientCallbackJob' jurl inner = do
  (chanID, readNextEvent) <- newEventChan
  env <- ask
  let
    url = chanURL env chanID
    cli = inner url

  runClientJob (jurl ^. job_server_url) StartingJobError cli
  progress $ Started jurl Nothing
  loop readNextEvent

  where
    loop readNextEvent = do
      mmsg <- liftIO readNextEvent
      case mmsg of
        Nothing ->
          throwError DecodingChanMessageError
        Just msg -> do
          forM_ (msg ^. msg_event) $ progress . Event jurl Nothing
          forM_ (msg ^. msg_error) $ throwError . ChanMessageError
            -- TODO: should we have an error event?
            -- progress . ErrorEvent jurl Nothing
          case msg ^. msg_result of
            Nothing -> loop readNextEvent
            Just o  -> pure o

clientCallbackJob :: (ToJSON i, ToJSON e, FromJSON e, FromJSON o, M m)
                  => JobServerURL e i o -> i -> m (JobOutput o)
clientCallbackJob jurl input = do
  clientCallbackJob' jurl (client (callbackJobsAPI jurl) . CallbackInput input)

clientMCallback :: (ToJSON e, ToJSON i, ToJSON o)
                => ChanMessage e i o -> ClientM ()
clientMCallback msg = do
  forM_ (msg ^. msg_event)  cli_event
  forM_ (msg ^. msg_error)  cli_error
  forM_ (msg ^. msg_result) (cli_result . JobOutput)
  where
    (cli_event :<|> cli_error :<|> cli_result) =
        client (Proxy :: Proxy (CallbackAPI e o))

clientCallback :: (ToJSON e, ToJSON i, ToJSON o, M m)
               => URL -> ChanMessage e i o -> m ()
clientCallback cb_url = runClientJob cb_url CallbackError . clientMCallback

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

clientNewJob :: (ToJSON i, FromJSON e, FromJSON o, M m)
             => JobServerURL e i o -> JobInput i -> m (JobStatus 'Unsafe e)
clientNewJob jurl = runClientJob (jurl ^. job_server_url) StartingJobError . newJobClient
  where
    newJobClient :<|> _ :<|> _ :<|> _ = client $ asyncJobsAPI jurl

clientWaitJob :: (ToJSON i, FromJSON e, FromJSON o, M m)
              => RunningJob e i o -> m o
clientWaitJob job =
    runClientJob jurl WaitingJobError (view job_output <$> waitJobClient jid)
  where
    jurl = job ^. running_job_url
    jid  = job ^. running_job_id . to forgetID
    _ :<|> _ :<|> _ :<|> waitJobClient = client $ asyncJobsAPI job

clientKillJob :: (ToJSON i, FromJSON e, FromJSON o, M m)
              => RunningJob e i o -> m (JobStatus 'Unsafe e)
clientKillJob job = runClientJob jurl KillingJobError (killJobClient jid)
  where
    jurl = job ^. running_job_url
    jid  = job ^. running_job_id
    _ :<|> killJobClient :<|> _ :<|> _ = client $ asyncJobsAPI job

clientPollJob :: (ToJSON i, FromJSON e, FromJSON o, M m)
              => RunningJob e i o -> m (JobStatus 'Unsafe e)
clientPollJob job = runClientJob jurl PollingJobError (clientMPollJob jid)
  where
    jurl = job ^. running_job_url
    jid  = job ^. running_job_id
    _ :<|> _ :<|> clientMPollJob :<|> _ = client $ asyncJobsAPI job

-- NOTES:
-- * retryOnTransientFailure ?
-- * mapM_ in parallel ?
killRunningJobs :: M m => m ()
killRunningJobs = do
  env <- ask
  jobs <- liftIO $ readMVar (env ^. env_jobs_mvar)
  mapM_ clientKillJob $ Set.toList jobs
  liftIO $ modifyMVar_ (env ^. env_jobs_mvar) (\new -> pure $ new `Set.difference` jobs)

isFinishedJob :: JobStatus 'Unsafe e -> Bool
isFinishedJob status = status ^. job_status == "finished"

fillLog :: (ToJSON e, FromJSON e, ToJSON i, FromJSON o, M m)
        => JobServerURL e i o -> RunningJob e i o -> Int -> m ()
fillLog jurl job pos = do
  env <- ask
  liftIO . threadDelay $ env ^. env_polling_delay_ms
  status <- retryOnTransientFailure $ clientPollJob job
  let events = drop pos $ status ^. job_log
  forM_ events $ progress . Event jurl (Just $ job ^. running_job_id)
  unless (isFinishedJob status) $ fillLog jurl job (pos + length events)

clientAsyncJob :: (FromJSON e, ToJSON e, ToJSON i, FromJSON o, M m)
               => JobServerURL e i o -> i -> m o
clientAsyncJob jurl i = do
  status <- retryOnTransientFailure . clientNewJob jurl $ JobInput i Nothing
  let
    jid = status ^. job_id
    job = runningJob jurl jid
  progress . Started jurl $ Just jid
  onRunningJob job Set.insert
  fillLog jurl job 0
  out <- retryOnTransientFailure $ clientWaitJob job
  progress . Finished jurl $ Just jid
  _ <- clientKillJob job
  onRunningJob job Set.delete
  pure out

callJobM :: (FromJSON o, FromJSON e, ToJSON i, ToJSON e, M m)
         => JobServerURL e i o -> i -> m o
callJobM jurl input = do
  progress $ NewTask jurl
  case jurl ^. job_server_api of
    Async    -> clientAsyncJob jurl input
    Sync     -> wrap clientSyncJob
    Stream   -> wrap clientStreamJob
    Callback -> wrap clientCallbackJob

  where
    wrap f = do
      out <- view job_output <$> retryOnTransientFailure (f jurl input)
      progress $ Finished jurl Nothing
      pure out

newtype JobM a =
    JobM { _unMonadJobIO :: ReaderT Env (ExceptT ClientJobError IO) a }
  deriving ( Functor, Applicative, Monad, MonadIO
           , MonadReader Env, MonadError ClientJobError)

instance MonadJob JobM where
  callJob = callJobM

newEnv :: Manager -> Chans -> LogEvent -> IO Env
newEnv manager chans log_event
  = Env manager oneSecond log_event
    <$> newMVar Set.empty
    <*> pure chans
  where
    oneSecond = 1000000

runJobM :: MonadIO m => Env -> JobM a -> m (Either ClientJobError a)
runJobM env (JobM m) = liftIO . runExceptT $ runReaderT m env

runJobMLog :: (FromJSON e, MonadIO m) => Env -> (e -> IO ()) -> JobM a -> m (Either ClientJobError a)
runJobMLog env log_ =
  runJobM (env & env_log_event %~ forwardInnerEvents log_)

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
serveWithCallbacks :: forall api. HasServer api '[]
                   => Proxy api
                   -> BaseUrl
                   -> Manager
                   -> LogEvent
                   -> (Env -> Server api)
                   -> IO Application
serveWithCallbacks _ url manager log_event s = do
  (chans, callbacksServer) <- newChans $ mkURL url "chans"
  client_env <- newEnv manager chans log_event
  pure $ serve (Proxy :: Proxy (WithCallbacks api))
               (callbacksServer :<|> s client_env)
