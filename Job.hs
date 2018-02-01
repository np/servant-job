{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
module Job
  -- Essentials
  ( JobAPI

  , JobID
  , mkJobID
  , job_number
  , job_time
  , job_token

  , JobStatus
  , job_id
  , job_status

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
import Data.Aeson.Types
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
import Servant
import System.IO
import Web.HttpApiData (ToHttpApiData(toUrlPiece), parseUrlPiece)

-- import Debug.Trace

import Lib.Utils ((?!))

type LBS = LBS.ByteString

newtype SecretKey = SecretKey LBS

data Safety = Safe | Unsafe

data JobID (s :: Safety) = PrivateJobID
  { _job_number :: Int
  , _job_time   :: UTCTime
  , _job_token  :: String
  }

data Job a = Job
  { _job_async   :: !(Async a)
  , _job_timeout :: !UTCTime
  }

data Jobs a = Jobs
  { _job_map  :: !(IntMap.IntMap (Job a))
  , _job_next :: !Int
  }

data JobEnv a = JobEnv
  { _job_env_secret_key :: !SecretKey
  , _job_env_jobs_mvar  :: !(MVar (Jobs a))
  , _job_env_duration   :: !NominalDiffTime
  -- ^ This duration specifies for how long one can ask the result of the job.
  -- Since job identifiers are of type 'Int' the number of jobs should not
  -- exceed the bounds of integers within this duration.
  }

data JobStatus safety = JobStatus
  { _job_id     :: !(JobID safety)
  , _job_status :: !Text
  }
  deriving Generic

type JobAPI ci i co o
    =  ReqBody ci i                           :> Post '[JSON] (JobStatus 'Safe)
  :<|> Capture "id" (JobID 'Unsafe) :> "kill" :> Post '[JSON] (JobStatus 'Safe)
  :<|> Capture "id" (JobID 'Unsafe) :> "poll" :> Get  '[JSON] (JobStatus 'Safe)
  :<|> Capture "id" (JobID 'Unsafe) :> "wait" :> Get co o

modifier :: Text -> String -> String
modifier pref field = T.unpack $ T.stripPrefix pref (T.pack field) ?! "Expecting prefix " <> T.unpack pref

jsonOptions :: Text -> Options
jsonOptions pref = defaultOptions
  { Data.Aeson.Types.fieldLabelModifier = modifier pref
  , Data.Aeson.Types.unwrapUnaryRecords = True
  , Data.Aeson.Types.omitNothingFields = True }

swaggerOptions :: Text -> SchemaOptions
swaggerOptions pref = defaultSchemaOptions
  { Data.Swagger.fieldLabelModifier = modifier pref
  , Data.Swagger.unwrapUnaryRecords = True
  }

makeLensesWith (lensRules & generateSignatures .~ False) ''JobID
job_number :: Lens' (JobID 'Safe) Int
job_time   :: Lens' (JobID 'Safe) UTCTime
job_token  :: Lens' (JobID 'Safe) String

makeLenses ''Job
makeLenses ''JobEnv
makeLenses ''Jobs
makeLenses ''JobStatus

emptyJobs :: Jobs a
emptyJobs = Jobs mempty 0

instance safety ~ 'Safe => ToJSON (JobStatus safety) where
  toJSON = genericToJSON $ jsonOptions "_job_"

instance safety ~ 'Unsafe => FromJSON (JobStatus safety) where
  parseJSON = genericParseJSON $ jsonOptions "_job_"

instance ToSchema (JobStatus safety) where
  declareNamedSchema = genericDeclareNamedSchema $ swaggerOptions "_job_"

macJobID :: SecretKey -> UTCTime -> Int -> String
macJobID (SecretKey s) now n =
  showDigest . hmacSha256 s . LBS.fromStrict . E.encodeUtf8 $
    T.unwords [toUrlPiece (utcTimeToPOSIXSeconds now), toUrlPiece n]

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

instance safety ~ 'Safe => ToHttpApiData (JobID safety) where
  toUrlPiece j =
    T.intercalate "-" [ toUrlPiece (j ^. job_number)
                      , toUrlPiece (utcTimeToPOSIXSeconds (j ^. job_time))
                      , toUrlPiece (j ^. job_token)]

instance safety ~ 'Unsafe => FromJSON (JobID safety) where
  parseJSON s = either (fail . T.unpack) pure . parseUrlPiece =<< parseJSON s

instance safety ~ 'Safe => ToJSON (JobID safety) where
  toJSON = toJSON . toUrlPiece

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

newJobEnv :: IO (JobEnv a)
newJobEnv = do
  s <- generateSecretKey
  v <- newMVar emptyJobs
  pure $ JobEnv s v defaultDuration

generateSecretKey :: IO SecretKey
generateSecretKey = SecretKey <$> withBinaryFile "/dev/random" ReadMode (\h -> LBS.hGet h 16)

deleteJob :: MonadIO m => JobEnv a -> JobID 'Safe -> m ()
deleteJob env jid =
  liftIO . modifyMVar_ (env ^. job_env_jobs_mvar) $ pure . (job_map . at (jid ^. job_number) .~ Nothing)

isValidJob :: UTCTime -> Job a -> Bool
isValidJob now job = now < job ^. job_timeout

deleteExpiredJobs :: JobEnv a -> IO ()
deleteExpiredJobs env = do
  expired <- modifyMVar (env ^. job_env_jobs_mvar) gcJobs
  mapM_ gcJob (expired ^.. each)

  where
    gcJobs jobs = do
      now <- getCurrentTime
      let (valid, expired) = IntMap.partition (isValidJob now) (jobs ^. job_map)
      pure (jobs & job_map .~ valid, expired)
    gcJob job = cancel $ job ^. job_async

deleteExpiredJobsPeriodically :: Int -> JobEnv a -> IO ()
deleteExpiredJobsPeriodically delay env = do
  threadDelay delay
  deleteExpiredJobs env
  deleteExpiredJobsPeriodically delay env

deleteExpiredJobsHourly :: JobEnv a -> IO ()
deleteExpiredJobsHourly = deleteExpiredJobsPeriodically hourly
  where
    hourly = 1000000 * 60 * 60

checkJobID :: JobEnv a -> JobID 'Unsafe -> Handler (JobID 'Safe)
checkJobID env (PrivateJobID n t d) = do
  now <- liftIO getCurrentTime
  when (now > addUTCTime (env ^. job_env_duration) t) $
    throwError $ err410 { errBody = "Expired job" }
  when (d /= macJobID (env ^. job_env_secret_key) t n) $
    throwError $ err401 { errBody = "Invalid job identifier authentication code" }
  pure $ PrivateJobID n t d

newJob :: MonadIO m => JobEnv a -> IO a -> m (JobStatus 'Safe)
newJob env task = liftIO $ do
  a <- async task
  now <- getCurrentTime
  let job = Job a $ addUTCTime (env ^. job_env_duration) now
  jid <- modifyMVar (env ^. job_env_jobs_mvar) $ \jobs ->
    let n = jobs ^. job_next in
    pure (jobs & job_map . at n ?~ job
               & job_next +~ 1,
          newJobID (env ^. job_env_secret_key) now n)
  pure $ JobStatus jid "started"

getJob :: JobEnv a -> JobID 'Safe -> Handler (Job a)
getJob env jid = do
  m <- liftIO . readMVar $ env ^. job_env_jobs_mvar
  maybe notFound pure $ m ^. job_map . at (jid ^. job_number)

  where
    notFound = throwError $ err404 { errBody = "No such job" }

pollJob :: MonadIO m => JobEnv a -> JobID 'Safe -> Job a -> m (JobStatus 'Safe)
pollJob _env jid job = do
  r <- liftIO . poll $ job ^. job_async
  pure . JobStatus jid $ maybe "running" (either failed (const "finished")) r

  where
    failed = ("failed " <>) . T.pack . show

killJob :: MonadIO m => JobEnv a -> JobID 'Safe -> Job a -> m (JobStatus 'Safe)
killJob env jid job = do
  liftIO . cancel $ job ^. job_async
  deleteJob env jid
  pure $ JobStatus jid "killed"

waitJob :: JobEnv a -> JobID 'Safe -> Job a -> Handler a
waitJob env jid job = do
  r <- either err pure =<< liftIO (waitCatch $ job ^. job_async)
  deleteJob env jid
  pure r

  where
    err e = throwError $ err500 { errBody = LBS.pack $ show e }

serveJobAPI :: forall i o ci co. JobEnv o -> (i -> IO o) -> Server (JobAPI ci i co o)
serveJobAPI env f
    =  newJob env . f -- (\x -> threadDelay 5000000 >> f x)
  :<|> wrap killJob
  :<|> wrap pollJob
  :<|> wrap waitJob

  where
    wrap :: forall a. (JobEnv o -> JobID 'Safe -> Job o -> Handler a) -> JobID 'Unsafe -> Handler a
    wrap g jid' = do
      jid <- checkJobID env jid'
      job <- getJob env jid
      g env jid job
