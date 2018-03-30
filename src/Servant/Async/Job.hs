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
  , asyncJobFunction
  , ioJobFunction
  , pureJobFunction
  , newAsyncJob
  , newIOJob

  , JobEnv
  , newJobEnv

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
import qualified Data.HashMap.Strict as H
import qualified Data.IntMap.Strict as IntMap
import Data.Maybe (isNothing)
import Data.Monoid
import Data.Swagger hiding (URL)
import Data.Text (Text)
import qualified Data.Text as T
import GHC.Generics hiding (to)
import Prelude hiding (log)
import Servant
import Servant.API.Flatten
import Servant.Async.Core
import Servant.Async.Utils (jsonOptions, swaggerOptions)
import Web.FormUrlEncoded

data Job e a = Job
  { _job_async   :: !(Async a)
  , _job_get_log :: !(IO [e])
  }

type instance SymbolOf (Job e o) = "job"

type JobEnv e o = Env (Job e o)

data JobInput a = JobInput
  { _job_input    :: !a
  , _job_callback :: !(Maybe URL)
  }
  deriving Generic

makeLenses ''JobInput

instance ToJSON i => ToJSON (JobInput i) where
  toJSON = genericToJSON $ jsonOptions "_job_"

instance FromJSON i => FromJSON (JobInput i) where
  parseJSON v =  genericParseJSON (jsonOptions "_job_") v
             <|> JobInput <$> parseJSON v <*> pure Nothing

instance ToForm i => ToForm (JobInput i) where
  toForm i =
    Form . H.insert "callback" (i ^.. job_callback . to toUrlPiece)
         $ unForm (toForm (i ^. job_input))

instance FromForm i => FromForm (JobInput i) where
  fromForm f =
    JobInput <$> fromForm (Form (H.delete "input" (unForm f)))
             <*> parseMaybe "callback" f

newtype JobOutput a = JobOutput
  { _job_output :: a }
  deriving Generic

makeLenses ''JobOutput

instance ToJSON o => ToJSON (JobOutput o) where
  toJSON = genericToJSON $ jsonOptions "_job_"

instance FromJSON o => FromJSON (JobOutput o) where
  parseJSON = genericParseJSON $ jsonOptions "_job_"

type JobID safety = ID safety "job"

data JobStatus safety e = JobStatus
  { _job_id     :: !(JobID safety)
  , _job_log    :: ![e]
  , _job_status :: !Text
  }
  deriving Generic

type AsyncJobAPI' safetyO ctO e o
    =  "kill" :> Post '[JSON] (JobStatus safetyO e)
  :<|> "poll" :> Get  '[JSON] (JobStatus safetyO e)
                 -- TODO: Add a query param to
                 -- drop part of the log in
                 -- kill/poll
  :<|> "wait" :> Get ctO (JobOutput o)

type AsyncJobsAPI' safetyI safetyO ctI ctO e i o
    =  ReqBody ctI (JobInput i)     :> Post '[JSON] (JobStatus safetyO e)
  :<|> Capture "id" (JobID safetyI) :> AsyncJobAPI' safetyO ctO e o

type AsyncJobAPI event output = AsyncJobAPI' 'Safe '[JSON] event output

type AsyncJobsAPI event input output = Flat (AsyncJobsAPI' 'Unsafe 'Safe '[JSON] '[JSON] event input output)

type AsyncJobsServer' ctI ctO e i o =
  Server (Flat (AsyncJobsAPI' 'Unsafe 'Safe ctI ctO e i o))

type AsyncJobsServer e i o = AsyncJobsServer' '[JSON] '[JSON] e i o

makeLenses ''Job
makeLenses ''JobStatus

instance (safety ~ 'Safe, ToJSON e) => ToJSON (JobStatus safety e) where
  toJSON = genericToJSON $ jsonOptions "_job_"

instance (safety ~ 'Unsafe, FromJSON e) => FromJSON (JobStatus safety e) where
  parseJSON = genericParseJSON $ jsonOptions "_job_"

instance ToSchema e => ToSchema (JobStatus safety e) where
  declareNamedSchema = genericDeclareNamedSchema $ swaggerOptions "_job_"

newJobEnv :: IO (JobEnv e o)
newJobEnv = newEnv

deleteJob :: MonadIO m => JobEnv e o -> JobID 'Safe -> m ()
deleteJob = deleteItem

deleteExpiredJobs :: JobEnv e o -> IO ()
deleteExpiredJobs = deleteExpiredItems gcJob
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

newJob :: MonadIO m => JobEnv e o -> JobFunction e i o
                    -> JobInput i -> m (JobStatus 'Safe e)
newJob env task i = liftIO $ do
  log <- newMVar []
  let mkJob = runJobFunction task (i ^. job_input) (pushLog log)
  (jid, _) <- newItem env (Job <$> mkJob <*> pure (readLog log))
  pure $ JobStatus jid [] "running"

  where
    pushLog :: MVar [e] -> e -> IO ()
    pushLog m x = modifyMVar_ m (pure . (x :))

    readLog :: MVar [e] -> IO [e]
    readLog = readMVar

asyncJobFunction :: (i -> IO (Async o)) -> JobFunction e i o
asyncJobFunction = JobFunction . (const .)

ioJobFunction :: (i -> IO o) -> JobFunction e i o
ioJobFunction = asyncJobFunction . (async .)

pureJobFunction :: (i -> o) -> JobFunction e i o
pureJobFunction = ioJobFunction . (pure .)

newAsyncJob :: MonadIO m => JobEnv e o -> IO (Async o) -> m (JobStatus 'Safe e)
newAsyncJob env m = newJob env (asyncJobFunction (const m)) (JobInput () Nothing)

newIOJob :: MonadIO m => JobEnv e o -> IO o -> m (JobStatus 'Safe e)
newIOJob env = newAsyncJob env . async

getJob :: JobEnv e o -> JobID 'Safe -> Handler (Job e o)
getJob env jid = view env_item <$> getItem env jid

pollJob :: MonadIO m => JobEnv e o -> JobID 'Safe -> Job e a -> m (JobStatus 'Safe e)
pollJob _env jid job = do
  -- It would be tempting to ensure that the log is consistent with the result
  -- of the polling by "locking" the log. Instead for simplicity we read the
  -- log after polling the job. The edge case being that the log shows more
  -- items which would tell that the job is actually finished while the
  -- returned status is running.
  r <- liftIO . poll $ job ^. job_async
  log <- liftIO $ job ^. job_get_log
  pure . JobStatus jid log $ maybe "running" (either failed (const "finished")) r

  where
    failed = ("failed " <>) . T.pack . show

killJob :: MonadIO m => JobEnv e o -> JobID 'Safe -> Job e a -> m (JobStatus 'Safe e)
killJob env jid job = do
  liftIO . cancel $ job ^. job_async
  log <- liftIO $ job ^. job_get_log
  deleteJob env jid
  pure $ JobStatus jid log "killed"

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

newtype JobFunction e i o = JobFunction
  { runJobFunction :: i -> (e -> IO ()) -> IO (Async o) }

serveJobsAPI :: forall e i o ctI ctO. ToJSON e
            => JobEnv e o
            -> JobFunction e i o
            -> AsyncJobsServer' ctI ctO e i o
serveJobsAPI env f
    =  newJob env f
  :<|> wrap killJob
  :<|> wrap pollJob
  :<|> (wrap . waitJob) False

  where
    wrap :: forall a. (JobEnv e o -> JobID 'Safe -> Job e o -> Handler a)
                   -> JobID 'Unsafe -> Handler a
    wrap g jid' = do
      jid <- checkID env jid'
      job <- getJob env jid
      g env jid job

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
  jobs <- view env_map <$> readMVar (env ^. env_state_mvar)
  active <- length <$> mapM (fmap isNothing . poll . view (env_item . job_async)) jobs
  pure $ JobsStats
    { job_count  = IntMap.size jobs
    , job_active = active
    }
