{-# LANGUAGE ScopedTypeVariables, GeneralizedNewtypeDeriving, KindSignatures, DataKinds, DeriveGeneric, TemplateHaskell, TypeOperators, FlexibleContexts, OverloadedStrings, RankNTypes, GADTs #-}
module Job.Orch where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (MVar, newMVar, readMVar, modifyMVar_)
import Control.Lens
import Control.Monad
import Data.Aeson
import GHC.Generics
import Servant
import Data.Set as Set
import Servant.Client
import Network.HTTP.Client hiding (Proxy)
import Job.Utils
import Job.Job

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

newtype JobServerURL e i o = JobServerURL
  { _job_server_url :: BaseUrl
  }
  deriving (Eq, Ord)

makeLenses ''JobServerURL

instance ToJSON (JobServerURL e i o) where
  toJSON (JobServerURL url) = toJSON (showBaseUrl url)

{-
type JobAPI co o
    =  "kill" :> Post '[JSON] (JobStatus 'Safe)
  :<|> "poll" :> Get  '[JSON] (JobStatus 'Safe)
  :<|> "wait" :> Get co o

type JobsAPI ci i co o
    =  ReqBody ci i                 :> Post '[JSON] (JobStatus 'Safe)
  :<|> Capture "id" (JobID 'Unsafe) :> JobAPI co o
-}

{-
type JobAPI (ci :: [*]) i co o
    =  ReqBody ci i                 :> Post '[JSON] (JobStatus 'Safe)
  :<|> Capture "id" (JobID 'Unsafe) :> "kill" :> Post '[JSON] (JobStatus 'Safe)
  :<|> Capture "id" (JobID 'Unsafe) :> "poll" :> Get  '[JSON] (JobStatus 'Safe)
  :<|> Capture "id" (JobID 'Unsafe) :> "wait" :> Get co (JobOutput o)
-}

jobAPI :: JobServerURL e i o -> Proxy (JobAPI 'Unsafe 'Unsafe '[JSON] i '[JSON] o)
jobAPI _ = Proxy

class Monad m => MonadJob m where
  callJob :: (ToJSON i, FromJSON o) => JobServerURL e i o -> i -> m o

isTransientFailure :: ServantError -> Bool
isTransientFailure _ = False
  -- TODO

retryOnTransientFailure :: IO (Either ServantError a) -> IO a
retryOnTransientFailure = undefined
{-
retryOnTransientFailure m = do
  esa <- m
  case esa of
    Left s ->
      if isTransientFailure s then
        retryOnTransientFailure m
      else
        throwError s
    Right a -> pure a
-}
data RunningJob e i o = RunningJob
  { _running_job_url :: JobServerURL e i o
  , _running_job_id  :: JobID 'Unsafe
  }
  deriving (Eq, Ord, Generic)

instance ToJSON (RunningJob e i o) where
  toJSON = genericToJSON $ jsonOptions "_running_job_"

makeLenses ''RunningJob

data Event e i o
  = NewTask   { _event_url :: JobServerURL e i o }
  | Starting  { _event_job :: RunningJob e i o }
  | Finishing { _event_job :: RunningJob e i o }
  | Event     { _event_job :: RunningJob e i o, _event_event :: e }
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

runClientJob :: Env -> JobServerURL e i o -> ClientM a -> IO (Either ServantError a)
runClientJob env jurl m =
  runClientM m (ClientEnv (env ^. env_manager) (jurl ^. job_server_url))

addRunningJob, delRunningJob :: Env -> RunningJob e i o -> IO ()
addRunningJob env job =
  modifyMVar_ (env ^. env_jobs_mvar) $ pure . Set.insert (forgetRunningJob job)
delRunningJob env job =
  modifyMVar_ (env ^. env_jobs_mvar) $ pure . Set.delete (forgetRunningJob job)

forgetJobServerURL :: JobServerURL e i o -> JobServerURL e' i' o'
forgetJobServerURL (JobServerURL u) = JobServerURL u

forgetRunningJob :: RunningJob e i o -> RunningJob e' i' o'
forgetRunningJob (RunningJob u i) = RunningJob (forgetJobServerURL u) i

clientNewJob :: (ToJSON i, FromJSON o) => Env -> JobServerURL e i o -> i
             -> IO (Either ServantError (JobStatus 'Unsafe))
clientNewJob env jurl = runClientJob env jurl . clientMNewJob
  where
    clientMNewJob :<|> _ :<|> _ :<|> _ = client $ jobAPI jurl

clientWaitJob :: (ToJSON i, FromJSON o) => Env -> RunningJob e i o -> IO (Either ServantError o)
clientWaitJob env (RunningJob jurl jid) =
    runClientJob env jurl (view job_output <$> clientMWaitJob (forgetJobID jid))
  where
    _ :<|> _ :<|> _ :<|> clientMWaitJob = client $ jobAPI jurl

clientKillJob :: (ToJSON i, FromJSON o) => Env -> RunningJob e i o
              -> IO (Either ServantError (JobStatus 'Unsafe))
clientKillJob env (RunningJob jurl jid) = runClientJob env jurl (clientMKillJob jid)
  where
    _ :<|> clientMKillJob :<|> _ :<|> _ = client $ jobAPI jurl

clientPollJob :: (ToJSON i, FromJSON o) => Env -> RunningJob e i o
              -> IO (Either ServantError (JobStatus 'Unsafe))
clientPollJob env (RunningJob jurl jid) =
    runClientJob env jurl (clientMPollJob jid)
  where
    _ :<|> _ :<|> clientMPollJob :<|> _ = client $ jobAPI jurl

-- NOTES:
-- * retryOnTransientFailure ?
-- * mapM_ in parallel ?
killRunningJobs :: Env -> IO ()
killRunningJobs env = do
  jobs <- readMVar (env ^. env_jobs_mvar)
  mapM_ (clientKillJob env) $ Set.toList jobs

isFinishedJob :: JobStatus 'Unsafe -> Bool
isFinishedJob status = status ^. job_status == "finished"

fillLog :: (ToJSON i, FromJSON o) => Env -> RunningJob Value i o -> Int -> IO ()
fillLog env job pos = do
  threadDelay $ env ^. env_polling_delay_ms
  status <- retryOnTransientFailure $ clientPollJob env job
  let events = drop pos $ status ^. job_log
  forM_ events $ progress env . Event job
  unless (isFinishedJob status) $ fillLog env job (pos + length events)

-- catch errors ?
callJobIO :: (e ~ Value, ToJSON e, ToJSON i, FromJSON o)
          => Env -> JobServerURL e i o -> i -> IO o
callJobIO env jurl input = do
  progress env $ NewTask jurl
  status <- retryOnTransientFailure $ clientNewJob env jurl input
  let
    jid = status ^. job_id
    job = RunningJob jurl jid
  progress env $ Starting job
  addRunningJob env job
  fillLog env job 0
  out <- retryOnTransientFailure $ clientWaitJob env job
  progress env $ Finishing job
  _ <- clientKillJob env job
  delRunningJob env job
  return out

newtype MonadJobIO a = MonadJobIO { _unMonadJobIO :: Env -> IO a }

runMonadJobIO :: Manager -> (Value -> IO ()) -> MonadJobIO a -> IO a
runMonadJobIO manager log_event (MonadJobIO m) = do
    mvar <- newMVar Set.empty
    let env = Env manager oneSecond mvar (log_event . toJSON)
    m env
  where
    oneSecond = 1000000
