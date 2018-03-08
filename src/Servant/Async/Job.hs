{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
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

  , JobID
  , mkJobID
  , job_number
  , job_time
  , job_token

  , JobStatus
  , job_id
  , job_log
  , job_status

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
  , job_env_secret_key
  , job_env_duration
  , generateSecretKey
  , defaultDuration

  , serveJobAPI

  , Safety(..)
  , SecretKey

  , deleteExpiredJobs
  , deleteExpiredJobsPeriodically
  , deleteExpiredJobsHourly

  -- Internals
  , Job
  , job_async
  , job_timeout
  , deleteJob
  , checkJobID
  , newJob
  , getJob
  , pollJob
  , killJob
  , waitJob
  , isValidJob
  , forgetJobID
  )
  where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async, async, waitCatch, poll, cancel)
import Control.Concurrent.MVar (MVar, newMVar, readMVar, modifyMVar, modifyMVar_)
import Control.Lens
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Except
import Data.Aeson
import qualified Data.ByteString.Lazy.Char8 as LBS
import Data.Digest.Pure.SHA (hmacSha256, showDigest)
import qualified Data.IntMap.Strict as IntMap
import Data.Monoid
import Data.Swagger
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as E
import Data.Time.Clock (UTCTime, NominalDiffTime, addUTCTime, getCurrentTime)
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds, posixSecondsToUTCTime)
import GHC.Generics
import Prelude hiding (log)
import Servant
import System.IO
import Web.HttpApiData (ToHttpApiData(toUrlPiece), parseUrlPiece)

import Servant.Async.Utils (jsonOptions, swaggerOptions)

type LBS = LBS.ByteString

newtype SecretKey = SecretKey LBS

data Safety = Safe | Unsafe

data JobID (s :: Safety) = PrivateJobID
  { _job_number :: Int
  , _job_time   :: UTCTime
  , _job_token  :: String
  }
  deriving (Eq, Ord)

data Job e a = Job
  { _job_async   :: !(Async a)
  , _job_get_log :: !(IO [e])
  , _job_timeout :: !UTCTime
  }

data Jobs e a = Jobs
  { _job_map  :: !(IntMap.IntMap (Job e a))
  , _job_next :: !Int
  }

data JobEnv e a = JobEnv
  { _job_env_secret_key :: !SecretKey
  , _job_env_jobs_mvar  :: !(MVar (Jobs e a))
  , _job_env_duration   :: !NominalDiffTime
  -- ^ This duration specifies for how long one can ask the result of the job.
  -- Since job identifiers are of type 'Int' the number of jobs should not
  -- exceed the bounds of integers within this duration.
  }

newtype JobOutput a = JobOutput
  { _job_output :: a }
  deriving Generic

makeLenses ''JobOutput

instance ToJSON o => ToJSON (JobOutput o) where
  toJSON = genericToJSON $ jsonOptions "_job_"

instance FromJSON o => FromJSON (JobOutput o) where
  parseJSON = genericParseJSON $ jsonOptions "_job_"

data JobStatus safety e = JobStatus
  { _job_id     :: !(JobID safety)
  , _job_log    :: ![e]
  , _job_status :: !Text
  }
  deriving Generic

type AsyncJobAPI' safetyI safetyO ctI ctO e i o
    =  ReqBody ctI i                          :> Post '[JSON] (JobStatus safetyO e)
  :<|> Capture "id" (JobID safetyI) :> "kill" :> Post '[JSON] (JobStatus safetyO e)
  :<|> Capture "id" (JobID safetyI) :> "poll" :> Get  '[JSON] (JobStatus safetyO e)
                                                 -- TODO: Add a query param to
                                                 -- drop part of the log in
                                                 -- kill/poll
  :<|> Capture "id" (JobID safetyI) :> "wait" :> Get ctO (JobOutput o)

type AsyncJobAPI event input output = AsyncJobAPI' 'Unsafe 'Safe '[JSON] '[JSON] event input output
{-
type AsyncJobAPI co o
    =  "kill" :> Post '[JSON] (JobStatus 'Safe)
  :<|> "poll" :> Get  '[JSON] (JobStatus 'Safe)
  :<|> "wait" :> Get co o

type JobsAPI ci i co o
    =  ReqBody ci i                 :> Post '[JSON] (JobStatus 'Safe)
  :<|> Capture "id" (JobID 'Unsafe) :> AsyncJobAPI co o
-}

makeLensesWith (lensRules & generateSignatures .~ False) ''JobID
job_number :: Lens' (JobID safety) Int
job_time   :: Lens' (JobID safety) UTCTime
job_token  :: Lens' (JobID safety) String
{-
job_number :: Lens' (JobID 'Safe) Int
job_time   :: Lens' (JobID 'Safe) UTCTime
job_token  :: Lens' (JobID 'Safe) String
-}

makeLenses ''Job
makeLenses ''JobEnv
makeLenses ''Jobs
makeLenses ''JobStatus

emptyJobs :: Jobs e a
emptyJobs = Jobs mempty 0

instance (safety ~ 'Safe, ToJSON e) => ToJSON (JobStatus safety e) where
  toJSON = genericToJSON $ jsonOptions "_job_"

{-
instance FromJSON (JobStatus 'Unsafe) where
  parseJSON = genericParseJSON $ jsonOptions "_job_"

instance FromJSON (JobStatus 'Safe) where
  parseJSON = genericParseJSON $ jsonOptions "_job_"
-}

instance (safety ~ 'Unsafe, FromJSON e) => FromJSON (JobStatus safety e) where
  parseJSON = genericParseJSON $ jsonOptions "_job_"

instance ToSchema e => ToSchema (JobStatus safety e) where
  declareNamedSchema = genericDeclareNamedSchema $ swaggerOptions "_job_"

macJobID :: SecretKey -> UTCTime -> Int -> String
macJobID (SecretKey s) now n =
  showDigest . hmacSha256 s . LBS.fromStrict . E.encodeUtf8 $
    T.unwords [toUrlPiece (utcTimeToPOSIXSeconds now), toUrlPiece n]

forgetJobID :: JobID safety -> JobID safety2
forgetJobID (PrivateJobID x y z) = PrivateJobID x y z

newJobID :: SecretKey -> UTCTime -> Int -> JobID 'Safe
newJobID s t n = PrivateJobID n t $ macJobID s t n

mkJobID :: Int -> UTCTime -> String -> JobID 'Unsafe
mkJobID n t d = PrivateJobID n t d

instance safety ~ 'Unsafe => FromHttpApiData (JobID safety) where
  parseUrlPiece s =
    case T.splitOn "-" s of
      [n, t, d] -> mkJobID <$> parseUrlPiece n
                           <*> (posixSecondsToUTCTime <$> parseUrlPiece t)
                           <*> parseUrlPiece d
      _ -> Left "Invalid job identifier (expecting 3 parts separated by '-')"

instance {-safety ~ 'Safe =>-} ToHttpApiData (JobID safety) where
  toUrlPiece j =
    T.intercalate "-" [ toUrlPiece (j ^. job_number)
                      , toUrlPiece (utcTimeToPOSIXSeconds (j ^. job_time))
                      , toUrlPiece (j ^. job_token)]

instance safety ~ 'Unsafe => FromJSON (JobID safety) where
  parseJSON s = either (fail . T.unpack) pure . parseUrlPiece =<< parseJSON s

instance {-safety ~ 'Safe =>-} ToJSON (JobID safety) where
  toJSON = toJSON . toUrlPiece

{-
instance ToJSON (JobID 'Safe) where
  toJSON = toJSON . toUrlPiece

instance ToJSON (JobID 'Unsafe) where
  toJSON = toJSON . toUrlPiece
-}

instance ToParamSchema (JobID safety) where
  toParamSchema _ = mempty
    & type_   .~ SwaggerString
    & pattern ?~ "[0-9]+-[0-9]+s-[0-9a-f]{64}"

instance ToSchema (JobID safety) where
  declareNamedSchema p = pure . NamedSchema (Just "JobID") $ mempty
    & title       ?~ "Job identifier"
    & paramSchema .~ toParamSchema p

-- Default duration is one day
defaultDuration :: NominalDiffTime
defaultDuration = 86400 -- it is called nominalDay in time >= 1.8

newJobEnv :: IO (JobEnv e a)
newJobEnv = do
  s <- generateSecretKey
  v <- newMVar emptyJobs
  pure $ JobEnv s v defaultDuration

generateSecretKey :: IO SecretKey
generateSecretKey = SecretKey <$> withBinaryFile "/dev/random" ReadMode (\h -> LBS.hGet h 16)

deleteJob :: MonadIO m => JobEnv e a -> JobID 'Safe -> m ()
deleteJob env jid =
  liftIO . modifyMVar_ (env ^. job_env_jobs_mvar) $ pure . (job_map . at (jid ^. job_number) .~ Nothing)

isValidJob :: UTCTime -> Job e a -> Bool
isValidJob now job = now < job ^. job_timeout

deleteExpiredJobs :: JobEnv e a -> IO ()
deleteExpiredJobs env = do
  expired <- modifyMVar (env ^. job_env_jobs_mvar) gcJobs
  mapM_ gcJob (expired ^.. each)

  where
    gcJobs jobs = do
      now <- getCurrentTime
      let (valid, expired) = IntMap.partition (isValidJob now) (jobs ^. job_map)
      pure (jobs & job_map .~ valid, expired)
    gcJob job = cancel $ job ^. job_async

deleteExpiredJobsPeriodically :: Int -> JobEnv e a -> IO ()
deleteExpiredJobsPeriodically delay env = do
  threadDelay delay
  deleteExpiredJobs env
  deleteExpiredJobsPeriodically delay env

deleteExpiredJobsHourly :: JobEnv e a -> IO ()
deleteExpiredJobsHourly = deleteExpiredJobsPeriodically hourly
  where
    hourly = 1000000 * 60 * 60

checkJobID :: JobEnv e a -> JobID 'Unsafe -> Handler (JobID 'Safe)
checkJobID env (PrivateJobID n t d) = do
  now <- liftIO getCurrentTime
  when (now > addUTCTime (env ^. job_env_duration) t) $
    throwError $ err410 { errBody = "Expired job" }
  when (d /= macJobID (env ^. job_env_secret_key) t n) $
    throwError $ err401 { errBody = "Invalid job identifier authentication code" }
  pure $ PrivateJobID n t d

newJob :: MonadIO m => JobEnv e a -> JobFunction e a -> m (JobStatus 'Safe e)
newJob env task = liftIO $ do
  log <- newMVar []
  a <- runJobFunction task (pushLog log)
  now <- getCurrentTime
  let job = Job a (readLog log) $ addUTCTime (env ^. job_env_duration) now
  jid <- modifyMVar (env ^. job_env_jobs_mvar) $ \jobs ->
    let n = jobs ^. job_next in
    pure (jobs & job_map . at n ?~ job
               & job_next +~ 1,
          newJobID (env ^. job_env_secret_key) now n)
  pure $ JobStatus jid [] "running"

  where
    pushLog :: MVar [e] -> e -> IO ()
    pushLog m x = modifyMVar_ m (pure . (x :))

    readLog :: MVar [e] -> IO [e]
    readLog = readMVar

asyncJobFunction :: IO (Async a) -> JobFunction e a
asyncJobFunction = JobFunction . const

ioJobFunction :: IO a -> JobFunction e a
ioJobFunction = asyncJobFunction . async

pureJobFunction :: a -> JobFunction e a
pureJobFunction = ioJobFunction . pure

newAsyncJob :: MonadIO m => JobEnv e a -> IO (Async a) -> m (JobStatus 'Safe e)
newAsyncJob env = newJob env . asyncJobFunction

newIOJob :: MonadIO m => JobEnv e a -> IO a -> m (JobStatus 'Safe e)
newIOJob env = newAsyncJob env . async

getJob :: JobEnv e a -> JobID 'Safe -> Handler (Job e a)
getJob env jid = do
  m <- liftIO . readMVar $ env ^. job_env_jobs_mvar
  maybe notFound pure $ m ^. job_map . at (jid ^. job_number)

  where
    notFound = throwError $ err404 { errBody = "No such job" }

pollJob :: MonadIO m => JobEnv e a -> JobID 'Safe -> Job e a -> m (JobStatus 'Safe e)
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

killJob :: MonadIO m => JobEnv e a -> JobID 'Safe -> Job e a -> m (JobStatus 'Safe e)
killJob env jid job = do
  liftIO . cancel $ job ^. job_async
  log <- liftIO $ job ^. job_get_log
  deleteJob env jid
  pure $ JobStatus jid log "killed"

waitJob :: Bool -> JobEnv e a -> JobID 'Safe -> Job e a -> Handler (JobOutput a)
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

newtype JobFunction e o = JobFunction
  { runJobFunction :: (e -> IO ()) -> IO (Async o) }

serveJobAPI :: forall e i o ctI ctO. ToJSON e
            => JobEnv e o
            -> (i -> JobFunction e o)
            -> Server (AsyncJobAPI' 'Unsafe 'Safe ctI ctO e i o)
serveJobAPI env f
    =  newJob env . f -- (\x -> threadDelay 5000000 >> f x)
  :<|> wrap killJob
  :<|> wrap pollJob
  :<|> (wrap . waitJob) False

  where
    wrap :: forall a. (JobEnv e o -> JobID 'Safe -> Job e o -> Handler a)
                   -> JobID 'Unsafe -> Handler a
    wrap g jid' = do
      jid <- checkJobID env jid'
      job <- getJob env jid
      g env jid job
