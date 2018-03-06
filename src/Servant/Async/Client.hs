{-# LANGUAGE ScopedTypeVariables, GeneralizedNewtypeDeriving, KindSignatures, DataKinds, DeriveGeneric, TemplateHaskell, TypeOperators, FlexibleContexts, OverloadedStrings, RankNTypes, GADTs, GeneralizedNewtypeDeriving #-}
module Servant.Async.Client where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (MVar, newMVar, readMVar, modifyMVar_)
import Control.Lens
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Reader
import Data.Aeson
import GHC.Generics hiding (to)
import Servant
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text as T
import Servant.Client hiding (manager)
import Network.HTTP.Client hiding (Proxy)
import Servant.Async.Utils
import Servant.Async.Job

{-
-- from Job.hs
data Safety = Safe | Unsafe
data JobID (s :: Safety) = PrivateJobID
  { _job_number :: Int
  , _job_time   :: UTCTime
  , _job_token  :: String
  }
  deriving (Generic, Eq, Ord)
data JobStatus safety = JobStatus
  { _job_id     :: !(JobID safety)
  , _job_log    :: ![Value]
  , _job_status :: !Text
  }
  deriving Generic

makeLenses ''JobStatus

instance FromJSON (JobStatus safety)
instance FromJSON (JobID safety)
instance ToHttpApiData (JobID safety)
-}

data JobServerAPI = Sync | Async | Stream
  deriving (Eq, Ord, Generic)

instance ToJSON JobServerAPI

newtype URL = URL { _base_url :: BaseUrl } deriving (Eq, Ord)

makeLenses ''URL

instance ToJSON URL where
  toJSON url = toJSON (showBaseUrl (url ^. base_url))

data JobServerURL e i o = JobServerURL
  { _job_server_url :: URL
  , _job_server_api :: JobServerAPI
  }
  deriving (Eq, Ord, Generic)

makeLenses ''JobServerURL

instance ToJSON (JobServerURL e i o) where
  toJSON = genericToJSON $ jsonOptions "_job_server_"

type SyncAPI i o = ReqBody '[JSON] i :> Post '[JSON] (JobOutput o)

syncAPI :: proxy e i o -> Proxy (SyncAPI i o)
syncAPI _ = Proxy

data JobFrame e o = JobFrame
  { job_frame_event  :: Maybe e
  , job_frame_output :: Maybe (JobOutput o)
  }
  deriving (Generic)

instance (FromJSON e, FromJSON o) => FromJSON (JobFrame e o) where
  parseJSON = genericParseJSON $ jsonOptions "_job_frame_"

instance (ToJSON e, ToJSON o) => ToJSON (JobFrame e o) where
  toJSON = genericToJSON $ jsonOptions "_job_frame_"

type StreamAPI f e i o =
  ReqBody '[JSON] i :> StreamPost NewlineFraming JSON (f (JobFrame e o))

type ServerStreamAPI e i o = StreamAPI StreamGenerator e i o
type ClientStreamAPI e i o = StreamAPI ResultStream    e i o

clientStreamAPI :: proxy e i o -> Proxy (ClientStreamAPI e i o)
clientStreamAPI _ = Proxy

jobAPI :: proxy e i o -> Proxy (JobAPI' 'Unsafe 'Unsafe '[JSON] '[JSON] e i o)
jobAPI _ = Proxy

class Monad m => MonadJob m where
  callJob :: (ToJSON e, FromJSON e, ToJSON i, FromJSON o) => JobServerURL e i o -> i -> m o

-- TODO
isTransientFailure :: ServantError -> Bool
isTransientFailure _ = False

-- TODO
retryOnTransientFailure :: IO (Either ServantError a) -> IO a
-- retryOnTransientFailure = undefined
-- {-
retryOnTransientFailure m = do
  esa <- m
  case esa of
    Left s ->
      if isTransientFailure s then
        retryOnTransientFailure m
      else
        error (show s) -- throwError s
    Right a -> pure a
-- -}
data RunningJob e i o = PrivateRunningJob
  { _running_job_url :: URL
  , _running_job_api :: JobServerAPI
  , _running_job_id  :: JobID 'Unsafe
  }
  deriving (Eq, Ord, Generic)

makeLenses ''RunningJob

runningJob :: JobServerURL e i o -> JobID 'Unsafe -> RunningJob e i o
runningJob jurl jid = PrivateRunningJob (jurl ^. job_server_url) (jurl ^. job_server_api) jid

instance ToJSON (RunningJob e i o) where
  toJSON = genericToJSON $ jsonOptions "_running_job_"

data Event e i o
  = NewTask  { _event_server :: JobServerURL e i o }
  | Started  { _event_server :: JobServerURL e i o
             , _event_job_id :: Maybe (JobID 'Unsafe) }
  | Finished { _event_server :: JobServerURL e i o
             , _event_job_id :: Maybe (JobID 'Unsafe) }
  | Event    { _event_server :: JobServerURL e i o
             , _event_job_id :: Maybe (JobID 'Unsafe)
             , _event_event  :: e }
  deriving (Generic)

instance ToJSON e => ToJSON (Event e i o) where
  toJSON = genericToJSON $ jsonOptions "_event_"

data Env = Env
  { _env_manager          :: Manager
  , _env_polling_delay_ms :: Int
  , _env_jobs_mvar        :: !(MVar (Set (RunningJob Value Value Value)))
  , _env_progress         :: forall e i o. ToJSON e => Event e i o -> IO ()
  }

makeLenses ''Env

progress :: ToJSON e => Env -> Event e i o -> IO ()
progress env = env ^. env_progress

runClientJob :: Env -> URL -> ClientM a -> IO (Either ServantError a)
runClientJob env url m =
  runClientM m (ClientEnv (env ^. env_manager) (url ^. base_url) Nothing)

addRunningJob, delRunningJob :: Env -> RunningJob e i o -> IO ()
addRunningJob env job =
  modifyMVar_ (env ^. env_jobs_mvar) $ pure . Set.insert (forgetRunningJob job)
delRunningJob env job =
  modifyMVar_ (env ^. env_jobs_mvar) $ pure . Set.delete (forgetRunningJob job)

forgetRunningJob :: RunningJob e i o -> RunningJob e' i' o'
forgetRunningJob (PrivateRunningJob u a i) = PrivateRunningJob u a i

clientSyncJob :: (ToJSON i, FromJSON e, FromJSON o) => Env -> JobServerURL e i o -> i
              -> IO (Either ServantError (JobOutput o))
clientSyncJob env jurl =
  runClientJob env (jurl ^. job_server_url) . client (syncAPI jurl)

  --ResultStream (forall b. (IO (Maybe (Either String a)) -> IO b) -> IO b)

clientStreamJob :: (ToJSON i, ToJSON e, FromJSON e, FromJSON o) => Env -> JobServerURL e i o -> i
              -> IO (Either ServantError (JobOutput o))
clientStreamJob env jurl input = do
  mstream <- runClientJob env (jurl ^. job_server_url) $ client (clientStreamAPI jurl) input
  case mstream of
    Left err -> pure (Left err)
    Right (ResultStream k) ->
      k $ \getResult ->
        let
          convertError m = ConnectionError{-TODO-} m
          missingOutput = convertError "Missing output"
          onFrame (Left err) = return (Left (convertError (T.pack err)))
          onFrame (Right (JobFrame me mo)) = do
            forM_ me $ progress env . Event jurl Nothing
            case mo of
              Nothing -> loop
              Just o  -> return (Right o)
          loop = do
            r <- getResult
            case r of
              Nothing -> return (Left missingOutput)
              Just x  -> onFrame x
        in loop

clientNewJob :: (ToJSON i, FromJSON e, FromJSON o) => Env -> JobServerURL e i o -> i
             -> IO (Either ServantError (JobStatus 'Unsafe e))
clientNewJob env jurl = runClientJob env (jurl ^. job_server_url) . newJobClient
  where
    newJobClient :<|> _ :<|> _ :<|> _ = client $ jobAPI jurl

clientWaitJob :: (ToJSON i, FromJSON e, FromJSON o)
              => Env -> RunningJob e i o -> IO (Either ServantError o)
clientWaitJob env job =
    runClientJob env jurl (view job_output <$> waitJobClient jid)
  where
    jurl = job ^. running_job_url
    jid  = job ^. running_job_id . to forgetJobID
    _ :<|> _ :<|> _ :<|> waitJobClient = client $ jobAPI job

clientKillJob :: (ToJSON i, FromJSON e, FromJSON o) => Env -> RunningJob e i o
              -> IO (Either ServantError (JobStatus 'Unsafe e))
clientKillJob env job = runClientJob env jurl (killJobClient jid)
  where
    jurl = job ^. running_job_url
    jid  = job ^. running_job_id
    _ :<|> killJobClient :<|> _ :<|> _ = client $ jobAPI job

clientPollJob :: (ToJSON i, FromJSON e, FromJSON o) => Env -> RunningJob e i o
              -> IO (Either ServantError (JobStatus 'Unsafe e))
clientPollJob env job = runClientJob env jurl (clientMPollJob jid)
  where
    jurl = job ^. running_job_url
    jid  = job ^. running_job_id
    _ :<|> _ :<|> clientMPollJob :<|> _ = client $ jobAPI job

-- NOTES:
-- * retryOnTransientFailure ?
-- * mapM_ in parallel ?
killRunningJobs :: Env -> IO ()
killRunningJobs env = do
  jobs <- readMVar (env ^. env_jobs_mvar)
  mapM_ (clientKillJob env) $ Set.toList jobs

isFinishedJob :: JobStatus 'Unsafe e -> Bool
isFinishedJob status = status ^. job_status == "finished"

fillLog :: (ToJSON e, FromJSON e, ToJSON i, FromJSON o) => Env -> JobServerURL e i o -> RunningJob e i o -> Int -> IO ()
fillLog env jurl job pos = do
  threadDelay $ env ^. env_polling_delay_ms
  status <- retryOnTransientFailure $ clientPollJob env job
  let events = drop pos $ status ^. job_log
  forM_ events $ progress env . Event jurl (Just $ job ^. running_job_id)
  unless (isFinishedJob status) $ fillLog env jurl job (pos + length events)

-- TODO catch errors ?
callJobIO :: (FromJSON e, ToJSON e, ToJSON i, FromJSON o)
          => Env -> JobServerURL e i o -> i -> IO o
callJobIO env jurl input = do
  progress env $ NewTask jurl
  case jurl ^. job_server_api of
    Async -> do
      status <- retryOnTransientFailure $ clientNewJob env jurl input
      let
        jid = status ^. job_id
        job = runningJob jurl jid
      progress env . Started jurl $ Just jid
      addRunningJob env job
      fillLog env jurl job 0
      out <- retryOnTransientFailure $ clientWaitJob env job
      progress env . Finished jurl $ Just jid
      _ <- clientKillJob env job
      delRunningJob env job
      pure out
    Sync   -> wrap clientSyncJob
    Stream -> wrap clientStreamJob

  where
    wrap f = do
      out <- view job_output <$> retryOnTransientFailure (f env jurl input)
      progress env $ Finished jurl Nothing
      pure out

newtype MonadJobIO a = MonadJobIO { _unMonadJobIO :: ReaderT Env IO a }
  deriving (Functor, Applicative, Monad, MonadReader Env, MonadIO)

instance MonadJob MonadJobIO where
  callJob jurl input = do
    env <- ask
    liftIO $ callJobIO env jurl input

runMonadJobIO :: Manager -> (forall e i o. ToJSON e => Event e i o -> IO ()) -> MonadJobIO a -> IO a
runMonadJobIO manager log_event (MonadJobIO m) = do
    mvar <- newMVar Set.empty
    let env = Env manager oneSecond mvar log_event
    runReaderT m env
  where
    oneSecond = 1000000
