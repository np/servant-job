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
module Servant.Job.Client
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
import qualified Servant.Job.Core as Core
import Servant.Job.Core
import Servant.Job.Utils
import Servant.Job.Types
-- import Servant.Client (ClientM, ClientError, client) -- hiding (manager, ClientEnv)
import Servant.Client.Streaming (ClientM, ClientError, client) -- hiding (manager, ClientEnv)
-- import qualified Servant.Client as SC
import qualified Servant.Client.Streaming as SC

asyncJobsAPI :: proxy event input output
             -> Proxy (Flat (AsyncJobsAPI' 'Unsafe 'Unsafe '[JSON] '[JSON] NoCallbacks event input output))
asyncJobsAPI _ = Proxy

class Monad m => MonadJob m where
  callJob :: (ToJSON event, FromJSON event, ToJSON input, FromJSON output)
          => JobServerURL event input output -> input -> m output

data ClientJobError
  = DecodingChanMessageError String
  | MissingOutputError
  | FrameError String
  | ChanMessageError String
  | StartingJobError ClientError
  | WaitingJobError  ClientError
  | KillingJobError  ClientError
  | PollingJobError  ClientError
  | CallbackError    ClientError

  -- Show instance is used by `error` which is bad.
  deriving Show

runningJob :: JobServerURL event input output -> JobID 'Unsafe -> RunningJob event input output
runningJob jurl jid = PrivateRunningJob (jurl ^. job_server_url) (jurl ^. job_server_api) jid

forwardInnerEvents :: FromJSON event => (event -> IO ()) -> LogEvent -> LogEvent
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
-- We should return True on non-fatal errors which we believe are transient.
-- Do we want this to be part of the ClientEnv to be configurable or
-- is the notion of transient failure universal enough?
isTransientFailure :: ClientJobError -> Bool
isTransientFailure _ = False

retryOnTransientFailure :: M m => m a -> m a
retryOnTransientFailure m = m `catchError` f
  where
    f e | isTransientFailure e = retryOnTransientFailure m
        | otherwise            = throwError e

progress :: (ToJSON event, MonadReader ClientEnv m, MonadIO m) => Event event input output -> m ()
progress event = do
  log_event <- view cenv_log_event
  liftIO $ unLogEvent log_event event

runClientJob :: M m => URL -> (ClientError -> ClientJobError) -> ClientM a -> m a
runClientJob = undefined
{- TODO STREAMING
runClientJob url err m = do
  env <- ask
  let cenv = SC.ClientEnv (env ^. cenv_manager) (url ^. base_url) Nothing
  liftIO (runClientM m cenv)
    >>= either (throwError . err) pure
-}

onRunningJob :: M m => RunningJob event input output
                    -> (forall a. Ord a => a -> Endom (Set a))
                    -> m ()
onRunningJob job f = do
  env <- ask
  liftIO . modifyMVar_ (env ^. cenv_jobs_mvar) $ pure . f (forgetRunningJob job)

forgetRunningJob :: RunningJob event input output -> RunningJob event' input' output'
forgetRunningJob (PrivateRunningJob u a i) = PrivateRunningJob u a i

clientSyncJob :: (ToJSON input, ToJSON event, FromJSON event, FromJSON output, M m)
              => Bool -> JobServerURL event input output -> input -> m (JobOutput output)
clientSyncJob streamMode jurl input = do
  undefined
{- TODO STREAMING
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
-}

newEventChan :: (FromJSON error, FromJSON event, FromJSON output, M m)
             => m (ChanID 'Safe, IO (Either String (ChanMessage error event input output)))
newEventChan = do
  env <- ask
  (i, item) <- liftIO $ Core.newItem (env ^. cenv_chans . chans_env) newChan
  pure (i, Aeson.parseEither parseJSON <$> readChan (item ^. env_item))

chanURL :: ClientEnv -> ChanID 'Safe -> URL
chanURL env i = (env ^. cenv_chans . chans_url) & base_url %~ extendBaseUrl i

callbackJobsAPI :: proxy event input output -> Proxy (CallbackJobsAPI event input output)
callbackJobsAPI _ = Proxy

clientCallbackJob' :: (ToJSON event, FromJSON event, FromJSON output, M m)
                   => JobServerURL event input output
                   -> (URL -> ClientM ())
                   -> m (JobOutput output)
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

clientCallbackJob :: (ToJSON input, ToJSON event, FromJSON event, FromJSON output, M m)
                  => JobServerURL event input output -> input -> m (JobOutput output)
clientCallbackJob jurl input = do
  clientCallbackJob' jurl (client (callbackJobsAPI jurl) . CallbackInput input)

clientMCallback :: (ToJSON error, ToJSON event, ToJSON output)
                => ChanMessage error event input output -> ClientM ()
clientMCallback msg = do
  forM_ (msg ^. msg_event)  cli_event
  forM_ (msg ^. msg_error)  cli_error
  forM_ (msg ^. msg_result) (cli_result . JobOutput)
  where
    (cli_event :<|> cli_error :<|> cli_result) =
        client (Proxy :: Proxy (CallbackAPI error event output))

clientCallback :: (ToJSON error, ToJSON event, ToJSON output, M m)
               => URL -> ChanMessage error event input output -> m ()
clientCallback cb_url = runClientJob cb_url CallbackError . clientMCallback

clientNewJob :: (ToJSON input, FromJSON event, FromJSON output, M m)
             => JobServerURL event input output -> JobInput NoCallbacks input -> m (JobStatus 'Unsafe event)
clientNewJob jurl = runClientJob (jurl ^. job_server_url) StartingJobError . newJobClient
  where
    newJobClient :<|> _ :<|> _ :<|> _ = client $ asyncJobsAPI jurl

clientWaitJob :: (ToJSON input, FromJSON event, FromJSON output, M m)
              => RunningJob event input output -> m output
clientWaitJob job =
    runClientJob jurl WaitingJobError (view job_output <$> waitJobClient jid)
  where
    jurl = job ^. running_job_url
    jid  = job ^. running_job_id . to forgetID
    _ :<|> _ :<|> _ :<|> waitJobClient = client $ asyncJobsAPI job

clientKillJob :: (ToJSON input, FromJSON event, FromJSON output, M m)
              => RunningJob event input output
              -> Maybe Limit -> Maybe Offset -> m (JobStatus 'Unsafe event)
clientKillJob job limit offset =
    runClientJob jurl KillingJobError (killJobClient jid limit offset)
  where
    jurl = job ^. running_job_url
    jid  = job ^. running_job_id
    _ :<|> killJobClient :<|> _ :<|> _ = client $ asyncJobsAPI job

clientPollJob :: (ToJSON input, FromJSON event, FromJSON output, M m)
              => RunningJob event input output -> Maybe Limit -> Maybe Offset -> m (JobStatus 'Unsafe event)
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

isFinishedJob :: JobStatus 'Unsafe event -> Bool
isFinishedJob status = status ^. job_status == IsFinished

fillLog :: (ToJSON event, FromJSON event, ToJSON input, FromJSON output, M m)
        => JobServerURL event input output -> RunningJob event input output -> Offset -> m ()
fillLog jurl job pos = do
  env <- ask
  liftIO . threadDelay $ env ^. cenv_polling_delay_ms
  status <- retryOnTransientFailure $ clientPollJob job Nothing (Just pos)
  let events = status ^. job_log
  forM_ events $ progress . Event jurl (Just $ job ^. running_job_id)
  unless (isFinishedJob status) $
    fillLog jurl job (Offset $ unOffset pos + length events)

clientAsyncJob :: (FromJSON event, ToJSON event, ToJSON input, FromJSON output, M m)
               => JobServerURL event input output -> input -> m output
clientAsyncJob jurl i = do
  -- TODO
  -- We could take a callback mode flag.
  -- With this flag on we would aquire a callback URL and we would
  -- directly receive the logs without polling.
  status <- retryOnTransientFailure . clientNewJob jurl $ JobInput i NoCallbacks
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

callJobM :: (FromJSON output, FromJSON event, ToJSON input, ToJSON event, M m)
         => JobServerURL event input output -> input -> m output
callJobM jurl input = do
  progress $ NewTask jurl
  case jurl ^. job_server_api of
    Async    -> clientAsyncJob jurl input
    Sync     -> wrap $ clientSyncJob streamMode
    Callback -> wrap clientCallbackJob

  where
    -- TODO we should have a way to control streaming
    streamMode = False
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

runJobMLog :: (FromJSON event, MonadIO m) => ClientEnv -> (event -> IO ()) -> JobM a -> m (Either ClientJobError a)
runJobMLog env log_ =
  runJobM (env & cenv_log_event %~ forwardInnerEvents log_)
