{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeOperators     #-}

import Control.Lens
import Data.Aeson
import qualified Data.ByteString.Lazy.Char8 as LBS
import Data.Text (Text)
import GHC.Generics hiding (to)
import Network.HTTP.Client.TLS
import Network.Wai.Handler.Warp hiding (defaultSettings)
import Servant
import Servant.Job.Utils (jsonOptions)
import Servant.Client.Core
import qualified Servant.Client as C
import Servant.Job.Async
import Servant.Job.Client
import Servant.Job.Server
import System.Environment

import qualified Data.HashMap.Strict as H
import Web.FormUrlEncoded hiding (parseMaybe)

data Schedule = Schedule
  { s_project :: !Text
  , s_spider  :: !Text
  , s_setting :: ![Text]
  , s_jobid   :: !(Maybe Text)
  , s_version :: !(Maybe Text)
  , s_extra   :: ![(Text,[Text])]
  }
  deriving (Generic)

data ScheduleResponse = ScheduleResponse
  { _r_status :: !Text
  , _r_jobid  :: !Text
  }
  deriving (Generic)

instance FromJSON ScheduleResponse where
  parseJSON = genericParseJSON (jsonOptions "_r_")

instance ToForm Schedule where
  toForm s =
    Form . H.fromList $
      [("project",  [s_project s])
      ,("spider",   [s_spider  s])
      ,("setting",  s_setting s)
      ,("jobid",    s_jobid s ^.. _Just)
      ,("_version", s_version s ^.. _Just)
      ] ++ s_extra s

type Scrapy =
  "schedule.json" :> ReqBody '[FormUrlEncoded] Schedule
                  :> Post '[JSON] ScheduleResponse

scrapyAPI :: Proxy Scrapy
scrapyAPI = Proxy

scrapySchedule :: Schedule -> C.ClientM ScheduleResponse
scrapySchedule = C.client scrapyAPI

data ScraperInput = ScraperInput
  { _scin_spider       :: !Text
  , _scin_query        :: !(Maybe Text)
  , _scin_user         :: !Text
  , _scin_corpus       :: !Int
  , _scin_report_every :: !(Maybe Int)
  , _scin_limit        :: !(Maybe Int)
  , _scin_local_file   :: !(Maybe Text)
  , _scin_count_only   :: !(Maybe Bool)
  }
  deriving Generic

makeLenses ''ScraperInput

instance FromJSON ScraperInput where
  parseJSON = genericParseJSON $ jsonOptions "_scin_"

data ScraperEvent = ScraperEvent
  { _scev_message :: !(Maybe Text)
  , _scev_level   :: !(Maybe Text)
  , _scev_date    :: !(Maybe Text)
  }
  deriving Generic

instance ToJSON ScraperEvent where
  toJSON = genericToJSON $ jsonOptions "_scev_"

instance FromJSON ScraperEvent where
  parseJSON = genericParseJSON $ jsonOptions "_scev_"

data ScraperStatus = ScraperStatus
  { _scst_succeeded :: !(Maybe Int)
  , _scst_failed    :: !(Maybe Int)
  , _scst_remaining :: !(Maybe Int)
  , _scst_events    :: !(Maybe [ScraperEvent])
  }
  deriving Generic

instance ToJSON ScraperStatus where
  toJSON = genericToJSON $ jsonOptions "_scst_"

instance FromJSON ScraperStatus where
  parseJSON = genericParseJSON $ jsonOptions "_scst_"

callJobScrapy :: (ToJSON e, FromJSON e, FromJSON o, MonadClientJob m)
              => JobServerURL e Schedule o
              -> (URL -> Schedule)
              -> m o
callJobScrapy jurl schedule = do
  progress $ NewTask jurl
  out <- view job_output <$>
          retryOnTransientFailure (clientCallbackJob' jurl
            (fmap (const ()) . scrapySchedule . schedule))
  progress $ Finished jurl Nothing
  pure out

logConsole :: ToJSON a => a -> IO ()
logConsole = LBS.putStrLn . encode

callScraper :: MonadClientJob m => URL -> ScraperInput -> m ScraperStatus
callScraper url input =
  callJobScrapy jurl $ \cb ->
    Schedule
      { s_project = "gargantext"
      , s_spider  = input ^. scin_spider
      , s_setting = []
      , s_jobid   = Nothing
      , s_version = Nothing
      , s_extra   =
          [("query",        input ^.. scin_query . _Just)
          ,("user",         [input ^. scin_user])
          ,("corpus",       [input ^. scin_corpus . to toUrlPiece])
          ,("report_every", input ^.. scin_report_every . _Just . to toUrlPiece)
          ,("limit",        input ^.. scin_limit . _Just . to toUrlPiece)
          ,("url",          input ^.. scin_local_file . _Just)
          ,("count_only",   input ^.. scin_count_only . _Just . to toUrlPiece)
          ,("callback",     [toUrlPiece cb])]
      }
  where
    jurl :: JobServerURL ScraperStatus Schedule ScraperStatus
    jurl = JobServerURL url Callback

type API =
  "async" :> "scrapy" :> AsyncJobsAPI ScraperStatus ScraperInput ScraperStatus

pipeline :: FromJSON e => URL -> ClientEnv -> ScraperInput
                       -> (e -> IO ()) -> IO ScraperStatus
pipeline scrapyurl client_env input log_status = do
  e <- runJobMLog client_env log_status $ callScraper scrapyurl input
  either (fail . show) pure e

main :: IO ()
main = do
  [port', scrapyurl'] <- getArgs
  let port = (read port' :: Int)
  scrapyurl <- parseBaseUrl scrapyurl'
  selfurl <- parseBaseUrl $ "http://0.0.0.0:" ++ show port
  putStrLn $ "Server listening on port: " ++ show port
          ++ " and scrapyurl: " ++ scrapyurl'
  manager <- newTlsManager
  jenv <- newJobEnv defaultSettings manager
  app <-
    serveApiWithCallbacks (Proxy :: Proxy API) defaultSettings selfurl manager (LogEvent logConsole) $
      simpleServeJobsAPI jenv . simpleJobFunction . pipeline (URL scrapyurl)
  run port app
