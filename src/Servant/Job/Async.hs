{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS -fno-warn-orphans #-}
module Servant.Job.Async
  -- Essentials
  ( AsyncJobAPI
  , AsyncJobAPI'
  , AsyncJobsAPI
  , AsyncJobsAPI'
  , AsyncJobsServer
  , MonadAsyncJobs
  , MonadAsyncJobs'

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
  , jobFunction
  , SimpleJobFunction
  , simpleJobFunction
  , ioJobFunction
  , pureJobFunction
  , newIOJob

  , JobEnv
  , jenv_jobs
  , jenv_manager
  , HasJobEnv(job_env)
  , defaultDuration
  , newJobEnv

  , EnvSettings
  , defaultSettings
  , env_duration

  , simpleServeJobsAPI
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
import Control.Exception.Base (Exception, throwIO)
import Control.Lens hiding (transform)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.Base (liftBase)
import Control.Monad.Trans.Control (MonadBaseControl(..))
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
import Servant.Job.Client (clientMCallback)
import Servant.Job.Types
import Servant.Job.Core
import Servant.Job.Utils (jsonOptions)
import qualified Servant.Client as C

data Job event a = Job
  { _job_async   :: !(Async a)
  , _job_get_log :: !(IO [event])
  }

makeLenses ''Job

type instance SymbolOf (Job event output) = "job"

data JobEnv event output = JobEnv
  { _jenv_jobs    :: !(Env (Job event output))
  , _jenv_manager :: !Manager
  }

makeLenses ''JobEnv

instance HasEnv (JobEnv event output) (Job event output) where
  _env = jenv_jobs

class HasEnv jenv (Job event output)
   => HasJobEnv jenv event output
    | jenv -> event, jenv -> output
  where
    job_env :: Lens' jenv (JobEnv event output)

instance HasJobEnv (JobEnv event output) event output where
  job_env = id

type MonadAsyncJobs env err event output m =
  ( MonadServantJob env err (Job event output) m
  , HasJobEnv env event output
  , Exception err
  , ToJSON event
  , ToJSON output
  , ToJSON err
  )

type MonadAsyncJobs' callbacks env err event output m =
  ( MonadAsyncJobs env err event output m
  , Traversable callbacks
  )

newJobEnv :: EnvSettings -> Manager -> IO (JobEnv event output)
newJobEnv settings manager = JobEnv <$> newEnv settings <*> pure manager

deleteJob :: (MonadBaseControl IO m, MonadReader env m, HasEnv env (Job event output))
          => JobID 'Safe -> m ()
deleteJob i = do
  env <- view _env
  deleteItem env i

deleteExpiredJobs :: JobEnv event output -> IO ()
deleteExpiredJobs = deleteExpiredItems gcJob . view jenv_jobs
  where
    gcJob job = cancel $ job ^. job_async

deleteExpiredJobsPeriodically :: Int -> JobEnv event output -> IO ()
deleteExpiredJobsPeriodically delay env = do
  threadDelay delay
  deleteExpiredJobs env
  deleteExpiredJobsPeriodically delay env

deleteExpiredJobsHourly :: JobEnv event output -> IO ()
deleteExpiredJobsHourly = deleteExpiredJobsPeriodically hourly
  where
    hourly = 1000000 * 60 * 60

newtype JobFunction env err event input output = JobFunction
  { runJobFunction ::
      forall m.
        ( MonadReader env m
        , MonadError  err m
        , MonadBaseControl IO m
        ) => input -> (event -> IO ()) -> m output
  }

class MonadState event m => MonadPushEvent event m where
  pushEvent :: (event -> event) -> m ()

newtype PushEventT event m a = PushEventT { runPushEventT :: ((event -> event) -> StateT event m ()) -> StateT event m a }

instance Monad (PushEventT event m) where

instance MonadState (PushEventT event m) where
  get = PushEventT $ const get
  put = PushEventT . const . put

instance MonadPushEvent event (PushEventT event m) where
  pushEvent f = PushEventT $ \cb -> cb f

newtype JobFunctionS env err event input output = JobFunction
  { runJobFunctionS ::
      forall m.
        ( MonadReader env m
        , MonadError  err m
        , MonadPushEvent event m
        , MonadBaseControl IO m
        ) => input -> m output
  }

fromJobFunctionS :: JobFunctionS env err event input output -> JobFunction env err event input output
fromJobFunctionS jf = JobFunction $ \input logEvent ->
  let cb eventMod = do
       s <- get
       let s' = eventMod s
       logEvent s'
       put s'
  in
  runStateT (runPushEventT (runJobFunctionS jf input) cb) initEvent

serveJobsAPI (fromJobFunctionS (JobFunctionS (\input -> do
     ...
     pushEvent (\old -> ... new)
     s <- get
     ...
     pushEvent ...
  )))

jobFunction :: ( forall m
               . ( MonadReader env m
                 , MonadError  err m
                 , MonadBaseControl IO m
                 )
                 => (input -> (event -> m ()) -> m output)
               )
            -> JobFunction env err event input output
jobFunction f = JobFunction (\i log -> f i (liftBase . log))

type SimpleJobFunction event input output =
  JobFunction (JobEnv event output) ServerError event input output

simpleJobFunction :: (input -> (event -> IO ()) -> IO output)
                  -> SimpleJobFunction event input output
simpleJobFunction f = JobFunction (\i log -> liftBase (f i log))

newJob :: forall callbacks env err event input output m
        . ( MonadAsyncJobs' callbacks env err event output m
          , MimeRender JSON event
          , MimeRender JSON input
          , MimeRender JSON output
          , MimeRender JSON err
          )
       => JobFunction env err event input output
       -> JobInput callbacks input -> m (JobStatus 'Safe event)
newJob task i = do
  env <- ask
  let
    jenv = env ^. job_env

    postCallback :: (MimeRender JSON err, MimeRender JSON output) => ChanMessage err event input output -> IO ()
    postCallback m =
      forM_ (i ^. job_callback) $ \url ->
        liftBase (C.runClientM (clientMCallback m)
                 (C.mkClientEnv (jenv ^. jenv_manager)
                               (url ^. base_url))
                               )

    pushLog :: MVar [event] -> event -> IO ()
    pushLog m e = do
      postCallback $ mkChanEvent e
      modifyMVar_ m (pure . (e :))

  liftBase $ do
    log <- newMVar []
    let mkJob = async $ do
          out <- runExceptT $
                  (`runReaderT` env) $
                     runJobFunction task (i ^. job_input) (liftBase . pushLog log)
          postCallback $ either mkChanError mkChanResult out
          either throwIO pure out

    (jid, _) <- newItem (jenv ^. jenv_jobs)
                        (Job <$> mkJob <*> pure (readLog log))
    pure $ JobStatus jid [] IsRunning Nothing

  where
    readLog :: MVar [event] -> IO [event]
    readLog = readMVar

ioJobFunction :: (input -> IO output) -> JobFunction env err event input output
ioJobFunction f = JobFunction (const . liftBase . f)

pureJobFunction :: (input -> output) -> JobFunction env err event input output
pureJobFunction = ioJobFunction . (pure .)

newIOJob :: MonadAsyncJobs env err event output m
         => IO output -> m (JobStatus 'Safe event)
newIOJob m = newJob (JobFunction (\_ _ -> liftBase m)) (JobInput () Nothing)

getJob :: MonadAsyncJobs env err event output m => JobID 'Safe -> m (Job event output)
getJob jid = view env_item <$> getItem jid

jobStatus :: JobID 'Safe -> Maybe Limit -> Maybe Offset -> [event] -> States -> Maybe Text -> JobStatus 'Safe event
jobStatus jid limit offset log =
  JobStatus jid (maybe id (take . unLimit)  limit $
                 maybe id (drop . unOffset) offset log)

pollJob :: MonadBaseControl IO m => Maybe Limit -> Maybe Offset
        -> JobID 'Safe -> Job event a -> m (JobStatus 'Safe event)
pollJob limit offset jid job = do
  -- It would be tempting to ensure that the log is consistent with the result
  -- of the polling by "locking" the log. Instead for simplicity we read the
  -- log after polling the job. The edge case being that the log shows more
  -- items which would tell that the job is actually finished while the
  -- returned status is running.
  r   <- liftBase . poll $ job ^. job_async
  log <- liftBase $ job ^. job_get_log
  let st  = maybe IsRunning (either (const IsFailure) (const IsFinished)) r
      err = maybe Nothing   (either Just (const Nothing)) r
  pure $ jobStatus jid limit offset log st ((T.pack . show) <$> err)


killJob :: (MonadBaseControl IO m, MonadReader env m, HasEnv env (Job event output))
        => Maybe Limit -> Maybe Offset
        -> JobID 'Safe -> Job event a -> m (JobStatus 'Safe event)
killJob limit offset jid job = do
  liftBase . cancel $ job ^. job_async
  log <- liftBase $ job ^. job_get_log
  deleteJob jid
  pure $ jobStatus jid limit offset log IsKilled Nothing

waitJob :: MonadServantJob env err (Job event output) m
        => Bool -> JobID 'Safe -> Job event a -> m (JobOutput a)
waitJob alsoDeleteJob jid job = do
  r <- either err pure =<< liftBase (waitCatch $ job ^. job_async)
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
  when alsoDeleteJob $ deleteJob jid
  pure $ JobOutput r

  where
    err e = serverError $ err500 { errBody = LBS.pack $ show e }

serveJobAPI :: forall env err event output m
             . MonadAsyncJobs env err event output m
            => JobID 'Unsafe
            -> AsyncJobServerT event output m
serveJobAPI jid'
    =  wrap' killJob
  :<|> wrap' pollJob
  :<|> (wrap . waitJob) False

  where
    wrap :: forall a. (JobID 'Safe -> Job event output -> m a) -> m a
    wrap g = do
      jid <- checkID jid'
      job <- getJob jid
      g jid job

    wrap' g limit offset = wrap (g limit offset)

serveJobsAPI :: forall callbacks env err m event input output ctI ctO
              . (MonadAsyncJobs' callbacks env err event output m, MimeRender JSON input)
             => JobFunction env err event input output
             -> AsyncJobsServerT' ctI ctO callbacks event input output m
serveJobsAPI f
    =  newJob f (JobInput undefined Nothing)
  :<|> newJob f
  :<|> serveJobAPI

instance ToJSON ServerError where
  toJSON = toJSON . show

-- `serveJobsAPI` specialized to the `Handler` monad.
simpleServeJobsAPI :: forall event input output.
                      ( FromJSON input
                      , ToJSON   event
                      , ToJSON   output
                      , MimeRender JSON input
                      , MimeRender JSON output
                      , MimeRender JSON event
                      )
                   => JobEnv event output
                   -> SimpleJobFunction event input output
                   -> AsyncJobsServer event input output
simpleServeJobsAPI env fun =
  hoistServer (Proxy :: Proxy (AsyncJobsAPI event input output)) transform (serveJobsAPI fun)
  where
    transform :: forall a. ReaderT (JobEnv event output) Handler a -> Handler a
    transform = flip runReaderT env

data JobsStats = JobsStats
  { job_count  :: !Int
  , job_active :: !Int
  }
  deriving (Generic)

instance ToJSON JobsStats where
  toJSON = genericToJSON $ jsonOptions "job_"

-- this API should be authorized to admins only
serveJobEnvStats :: JobEnv event output -> Server (Get '[JSON] JobsStats)
serveJobEnvStats env = liftBase $ do
  jobs <- view env_map <$> readMVar (env ^. jenv_jobs . env_state_mvar)
  active <- length <$> mapM (fmap isNothing . poll . view (env_item . job_async)) jobs
  pure $ JobsStats
    { job_count  = IntMap.size jobs
    , job_active = active
    }
