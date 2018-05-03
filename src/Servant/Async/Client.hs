{-# LANGUAGE ScopedTypeVariables, GeneralizedNewtypeDeriving, KindSignatures, DataKinds, DeriveGeneric, TemplateHaskell, TypeOperators, FlexibleContexts, OverloadedStrings, RankNTypes, GADTs, GeneralizedNewtypeDeriving, TypeFamilies, StandaloneDeriving, ConstraintKinds #-}
module Servant.Async.Client
  ( JobsAPI
  , MonadJob
  , callJob

  , JobM
  , runJobM
  , runJobMLog

  , ClientEnv
  , LogEvent(..)
  , forwardInnerEvents
  , cenv_manager
  , cenv_polling_delay_ms
  , cenv_log_event
  , cenv_jobs_mvar
  , cenv_chans
  , newEnv

  , URL(..)
  , mkURL
  , JobServerAPI(..)
  , JobServerURL(..)
  , ClientOrServer(..)

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
  , clientAsyncJob
  , clientCallbackJob'
  , clientCallbackJob
  , clientNewJob
  , clientPollJob
  , clientKillJob
  , clientWaitJob
  , clientMCallback
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
import Control.Concurrent.MVar (readMVar, modifyMVar_)
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
import Servant
import Servant.API.Flatten
import qualified Servant.Async.Core as Core
import Servant.Async.Core
import Servant.Async.Job
import Servant.Async.Utils
import Servant.Async.Types
import Servant.Client hiding (manager, ClientEnv)
import qualified Servant.Client as S

asyncJobsAPI :: proxy e i o -> Proxy (Flat (AsyncJobsAPI' 'Unsafe 'Unsafe '[JSON] '[JSON] e i o))
asyncJobsAPI _ = Proxy

class Monad m => MonadJob m where
  callJob :: (ToJSON e, FromJSON e, ToJSON i, FromJSON o)
          => JobServerURL e i o -> i -> m o

data ClientJobError
  = DecodingChanMessageError String
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

runningJob :: JobServerURL e i o -> JobID 'Unsafe -> RunningJob e i o
runningJob jurl jid = PrivateRunningJob (jurl ^. job_server_url) (jurl ^. job_server_api) jid

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

type MonadClientJob m = (MonadReader ClientEnv m, MonadError ClientJobError m, MonadIO m)
type M m = MonadClientJob m

-- TODO
isTransientFailure :: ClientJobError -> Bool
isTransientFailure _ = False

retryOnTransientFailure :: M m => m a -> m a
retryOnTransientFailure m = m `catchError` f
  where
    f e | isTransientFailure e = retryOnTransientFailure m
        | otherwise            = throwError e

progress :: (ToJSON e, MonadReader ClientEnv m, MonadIO m) => Event e i o -> m ()
progress event = do
  log_event <- view cenv_log_event
  liftIO $ unLogEvent log_event event

runClientJob :: M m => URL -> (ServantError -> ClientJobError) -> ClientM a -> m a
runClientJob url err m = do
  env <- ask
  liftIO (runClientM m (S.ClientEnv (env ^. cenv_manager) (url ^. base_url) Nothing))
    >>= either (throwError . err) pure

onRunningJob :: M m => RunningJob e i o
                    -> (forall a. Ord a => a -> Endom (Set a))
                    -> m ()
onRunningJob job f = do
  env <- ask
  liftIO . modifyMVar_ (env ^. cenv_jobs_mvar) $ pure . f (forgetRunningJob job)

forgetRunningJob :: RunningJob e i o -> RunningJob e' i' o'
forgetRunningJob (PrivateRunningJob u a i) = PrivateRunningJob u a i

clientSyncJob :: (ToJSON i, ToJSON e, FromJSON e, FromJSON o, M m)
              => Bool -> JobServerURL e i o -> i -> m (JobOutput o)
clientSyncJob streamMode jurl input = do
  let clientStream = client (syncJobsAPIClient jurl) streamMode input
  ResultStream k <- runClientJob (jurl ^. job_server_url) StartingJobError $
                      clientStream
  LogEvent log_event <- view cenv_log_event
  res <- liftIO . k $ \getResult ->
    let
      onFrame (Left err) = return (Left (FrameError err))
      onFrame (Right (JobFrame me mo)) = do
        forM_ me $ log_event . Event jurl Nothing
        case mo of
          Nothing -> loop
          Just o  -> return (Right (JobOutput o))
      loop = do
        r <- getResult
        case r of
          Nothing -> return (Left MissingOutputError)
          Just x  -> onFrame x
    in loop
  either throwError pure res

newEventChan :: (FromJSON e, FromJSON o, M m)
             => m (ChanID 'Safe, IO (Either String (ChanMessage e i o)))
newEventChan = do
  env <- ask
  (i, item) <- liftIO $ Core.newItem (env ^. cenv_chans . chans_env) newChan
  pure (i, Aeson.parseEither parseJSON <$> readChan (item ^. env_item))

chanURL :: ClientEnv -> ChanID 'Safe -> URL
chanURL env i = (env ^. cenv_chans . chans_url) & base_url %~ extendBaseUrl i

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
        Left err ->
          throwError $ DecodingChanMessageError err
        Right msg -> do
          forM_ (msg ^. msg_event) $ progress . Event jurl Nothing
          forM_ (msg ^. msg_error) $ throwError . ChanMessageError
            -- TODO: should we have an error event?
            -- progress . ErrorEvent jurl Nothing
          case msg ^. msg_result of
            Nothing -> loop readNextEvent
            Just o  -> pure $ JobOutput o

clientCallbackJob :: (ToJSON i, ToJSON e, FromJSON e, FromJSON o, M m)
                  => JobServerURL e i o -> i -> m (JobOutput o)
clientCallbackJob jurl input = do
  clientCallbackJob' jurl (client (callbackJobsAPI jurl) . CallbackInput input)

clientMCallback :: (ToJSON e, ToJSON o)
                => ChanMessage e i o -> ClientM ()
clientMCallback msg = do
  forM_ (msg ^. msg_event)  cli_event
  forM_ (msg ^. msg_error)  cli_error
  forM_ (msg ^. msg_result) (cli_result . JobOutput)
  where
    (cli_event :<|> cli_error :<|> cli_result) =
        client (Proxy :: Proxy (CallbackAPI e o))

clientCallback :: (ToJSON e, ToJSON o, M m)
               => URL -> ChanMessage e i o -> m ()
clientCallback cb_url = runClientJob cb_url CallbackError . clientMCallback

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
              => RunningJob e i o
              -> Maybe Limit -> Maybe Offset -> m (JobStatus 'Unsafe e)
clientKillJob job limit offset =
    runClientJob jurl KillingJobError (killJobClient jid limit offset)
  where
    jurl = job ^. running_job_url
    jid  = job ^. running_job_id
    _ :<|> killJobClient :<|> _ :<|> _ = client $ asyncJobsAPI job

clientPollJob :: (ToJSON i, FromJSON e, FromJSON o, M m)
              => RunningJob e i o -> Maybe Limit -> Maybe Offset -> m (JobStatus 'Unsafe e)
clientPollJob job limit offset =
    runClientJob jurl PollingJobError (clientMPollJob jid limit offset)
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
  jobs <- liftIO $ readMVar (env ^. cenv_jobs_mvar)
  forM_ (Set.toList jobs) $ \job ->
    clientKillJob job (Just (Limit 0)) Nothing
  liftIO . modifyMVar_ (env ^. cenv_jobs_mvar) $ \new ->
    pure $ new `Set.difference` jobs

isFinishedJob :: JobStatus 'Unsafe e -> Bool
isFinishedJob status = status ^. job_status == "finished"

fillLog :: (ToJSON e, FromJSON e, ToJSON i, FromJSON o, M m)
        => JobServerURL e i o -> RunningJob e i o -> Offset -> m ()
fillLog jurl job pos = do
  env <- ask
  liftIO . threadDelay $ env ^. cenv_polling_delay_ms
  status <- retryOnTransientFailure $ clientPollJob job Nothing (Just pos)
  let events = status ^. job_log
  forM_ events $ progress . Event jurl (Just $ job ^. running_job_id)
  unless (isFinishedJob status) $
    fillLog jurl job (Offset $ unOffset pos + length events)

clientAsyncJob :: (FromJSON e, ToJSON e, ToJSON i, FromJSON o, M m)
               => JobServerURL e i o -> i -> m o
clientAsyncJob jurl i = do
  -- TODO
  -- We could take a parameter pass a callback.
  -- This would avoid the need to poll for the logs.
  status <- retryOnTransientFailure . clientNewJob jurl $ JobInput i Nothing
  let
    jid = status ^. job_id
    job = runningJob jurl jid
  progress . Started jurl $ Just jid
  onRunningJob job Set.insert
  fillLog jurl job (Offset 0)
  out <- retryOnTransientFailure $ clientWaitJob job
  progress . Finished jurl $ Just jid
  _ <- clientKillJob job (Just (Limit 0)) Nothing
  onRunningJob job Set.delete
  pure out

callJobM :: (FromJSON o, FromJSON e, ToJSON i, ToJSON e, M m)
         => JobServerURL e i o -> i -> m o
callJobM jurl input = do
  progress $ NewTask jurl
  case jurl ^. job_server_api of
    Async    -> clientAsyncJob jurl input
    Sync     -> wrap $ clientSyncJob False -- TODO true
    Callback -> wrap clientCallbackJob

  where
    wrap f = do
      out <- view job_output <$> retryOnTransientFailure (f jurl input)
      progress $ Finished jurl Nothing
      pure out

newtype JobM a =
    JobM { _unMonadJobIO :: ReaderT ClientEnv (ExceptT ClientJobError IO) a }
  deriving ( Functor, Applicative, Monad, MonadIO
           , MonadReader ClientEnv, MonadError ClientJobError)

instance MonadJob JobM where
  callJob = callJobM

runJobM :: MonadIO m => ClientEnv -> JobM a -> m (Either ClientJobError a)
runJobM env (JobM m) = liftIO . runExceptT $ runReaderT m env

runJobMLog :: (FromJSON e, MonadIO m) => ClientEnv -> (e -> IO ()) -> JobM a -> m (Either ClientJobError a)
runJobMLog env log_ =
  runJobM (env & cenv_log_event %~ forwardInnerEvents log_)
