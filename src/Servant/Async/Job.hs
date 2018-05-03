{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
module Servant.Async.Job
  -- Essentials
  ( AsyncJobAPI
  , AsyncJobAPI'
  , AsyncJobsAPI
  , AsyncJobsAPI'
  , AsyncJobsServer

  , JobID
  , JobStatus
  , job_id
  , job_log
  , job_status

  , JobInput(JobInput)
  , job_input
  , job_callback

  , JobOutput(JobOutput)
  , job_output

  , JobFunction(JobFunction)
  , ioJobFunction
  , pureJobFunction
  , newIOJob

  , JobEnv
  , defaultDuration
  , newJobEnv

  , EnvSettings
  , defaultSettings
  , env_duration

  , serveJobsAPI

  , JobsStats(..)
  , serveJobEnvStats

  , deleteExpiredJobs
  , deleteExpiredJobsPeriodically
  , deleteExpiredJobsHourly

  -- Re-exports
  , Safety(..)

  -- Internals
  , Job
  , job_async
  , deleteJob
  , checkID
  , newJob
  , getJob
  , pollJob
  , killJob
  , waitJob
  )
  where

import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async, async, waitCatch, poll, cancel)
import Control.Concurrent.MVar (MVar, newMVar, readMVar, modifyMVar_)
import Control.Lens
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Except
import Data.Aeson
import qualified Data.ByteString.Lazy.Char8 as LBS
import qualified Data.IntMap.Strict as IntMap
import Data.Maybe (isNothing)
import Data.Monoid
import Data.Text (Text)
import qualified Data.Text as T
import GHC.Generics hiding (to)
import Network.HTTP.Client hiding (Proxy, path)
import Prelude hiding (log)
import Servant
import Servant.Async.Client (clientMCallback)
import Servant.Async.Types
import Servant.Async.Core
import Servant.Async.Utils (jsonOptions)
import Servant.Client hiding (manager, ClientEnv)
import qualified Servant.Client as S

data Job e a = Job
  { _job_async   :: !(Async a)
  , _job_get_log :: !(IO [e])
  }

makeLenses ''Job

type instance SymbolOf (Job e o) = "job"

data JobEnv e o = JobEnv
  { _jenv_jobs    :: !(Env (Job e o))
  , _jenv_manager :: !Manager
  }

makeLenses ''JobEnv

newJobEnv :: EnvSettings -> Manager -> IO (JobEnv e o)
newJobEnv settings manager = JobEnv <$> newEnv settings <*> pure manager

deleteJob :: MonadIO m => JobEnv e o -> JobID 'Safe -> m ()
deleteJob = deleteItem . view jenv_jobs

deleteExpiredJobs :: JobEnv e o -> IO ()
deleteExpiredJobs = deleteExpiredItems gcJob . view jenv_jobs
  where
    gcJob job = cancel $ job ^. job_async

deleteExpiredJobsPeriodically :: Int -> JobEnv e o -> IO ()
deleteExpiredJobsPeriodically delay env = do
  threadDelay delay
  deleteExpiredJobs env
  deleteExpiredJobsPeriodically delay env

deleteExpiredJobsHourly :: JobEnv e o -> IO ()
deleteExpiredJobsHourly = deleteExpiredJobsPeriodically hourly
  where
    hourly = 1000000 * 60 * 60

newtype JobFunction e i o = JobFunction
  { runJobFunction :: i -> (e -> IO ()) -> IO o }

newJob :: forall e i o m. (MonadIO m, ToJSON e, ToJSON o)
       => JobEnv e o -> JobFunction e i o
       -> JobInput i -> m (JobStatus 'Safe e)
newJob env task i = liftIO $ do
  log <- newMVar []
  let mkJob = async $ do
        out <- runJobFunction task (i ^. job_input) (pushLog log)
        postCallback $ mkChanResult out
        pure out

  (jid, _) <- newItem (env ^. jenv_jobs)
                      (Job <$> mkJob <*> pure (readLog log))
  pure $ JobStatus jid [] "running"

  where
    postCallback :: ChanMessage e i o -> IO ()
    postCallback m =
      forM_ (i ^. job_callback) $ \url ->
        liftIO (runClientM (clientMCallback m)
                (S.ClientEnv (env ^. jenv_manager)
                             (url ^. base_url) Nothing))
    pushLog :: MVar [e] -> e -> IO ()
    pushLog m e = do
      postCallback $ mkChanEvent e
      modifyMVar_ m (pure . (e :))

    readLog :: MVar [e] -> IO [e]
    readLog = readMVar

ioJobFunction :: (i -> IO o) -> JobFunction e i o
ioJobFunction = JobFunction . (const .)

pureJobFunction :: (i -> o) -> JobFunction e i o
pureJobFunction = ioJobFunction . (pure .)

newIOJob :: (MonadIO m, ToJSON e, ToJSON o)
         => JobEnv e o -> IO o -> m (JobStatus 'Safe e)
newIOJob env m = newJob env (JobFunction (\_ _ -> m)) (JobInput () Nothing)

getJob :: JobEnv e o -> JobID 'Safe -> Handler (Job e o)
getJob env jid = view env_item <$> getItem (env ^. jenv_jobs) jid

jobStatus :: JobID 'Safe -> Maybe Limit -> Maybe Offset -> [e] -> Text -> JobStatus 'Safe e
jobStatus jid limit offset log =
  JobStatus jid (maybe id (take . unLimit)  limit $
                 maybe id (drop . unOffset) offset log)

pollJob :: MonadIO m => Maybe Limit -> Maybe Offset
        -> JobEnv e o -> JobID 'Safe -> Job e a -> m (JobStatus 'Safe e)
pollJob limit offset _env jid job = do
  -- It would be tempting to ensure that the log is consistent with the result
  -- of the polling by "locking" the log. Instead for simplicity we read the
  -- log after polling the job. The edge case being that the log shows more
  -- items which would tell that the job is actually finished while the
  -- returned status is running.
  r <- liftIO . poll $ job ^. job_async
  log <- liftIO $ job ^. job_get_log
  pure . jobStatus jid limit offset log $ maybe "running" (either failed (const "finished")) r

  where
    failed = ("failed " <>) . T.pack . show

killJob :: MonadIO m => Maybe Limit -> Maybe Offset
        -> JobEnv e o -> JobID 'Safe -> Job e a -> m (JobStatus 'Safe e)
killJob limit offset env jid job = do
  liftIO . cancel $ job ^. job_async
  log <- liftIO $ job ^. job_get_log
  deleteJob env jid
  pure $ jobStatus jid limit offset log "killed"

waitJob :: Bool -> JobEnv e o -> JobID 'Safe -> Job e a -> Handler (JobOutput a)
waitJob alsoDeleteJob env jid job = do
  r <- either err pure =<< liftIO (waitCatch $ job ^. job_async)
  -- NOTE one might prefer not to deleteJob here and ask the client to
  -- manually killJob.
  --
  -- deleteJob:
  --   Pros: no need to killJob manually (less code, more efficient)
  --   Cons:
  --     * if the client miss the response to waitJob, it cannot
  --       pollJob or waitJob again as it is deleted.
  --     * the client cannot call pollJob/killJob after the waitJob to get
  --       the final log
  --
  -- Hence the alsoDeleteJob parameter
  when alsoDeleteJob $ deleteJob env jid
  pure $ JobOutput r

  where
    err e = throwError $ err500 { errBody = LBS.pack $ show e }

serveJobsAPI :: forall e i o ctI ctO.
                (ToJSON e, ToJSON o)
             => JobEnv e o
             -> JobFunction e i o
             -> AsyncJobsServer' ctI ctO e i o
serveJobsAPI env f
    =  newJob env f
  :<|> wrap' killJob
  :<|> wrap' pollJob
  :<|> (wrap . waitJob) False

  where
    wrap :: forall a. (JobEnv e o -> JobID 'Safe -> Job e o -> Handler a)
                   -> JobID 'Unsafe -> Handler a
    wrap g jid' = do
      jid <- checkID (env ^. jenv_jobs) jid'
      job <- getJob env jid
      g env jid job

    wrap' g jid' limit offset = wrap (g limit offset) jid'

data JobsStats = JobsStats
  { job_count  :: !Int
  , job_active :: !Int
  }
  deriving (Generic)

instance ToJSON JobsStats where
  toJSON = genericToJSON $ jsonOptions "job_"

-- this API should be authorized to admins only
serveJobEnvStats :: JobEnv e o -> Server (Get '[JSON] JobsStats)
serveJobEnvStats env = liftIO $ do
  jobs <- view env_map <$> readMVar (env ^. jenv_jobs . env_state_mvar)
  active <- length <$> mapM (fmap isNothing . poll . view (env_item . job_async)) jobs
  pure $ JobsStats
    { job_count  = IntMap.size jobs
    , job_active = active
    }
