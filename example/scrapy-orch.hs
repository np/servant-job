{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE TemplateHaskell   #-}

import Control.Concurrent.Async (async)
import Control.Lens
import Data.Aeson
import qualified Data.ByteString.Lazy.Char8 as LBS
import Data.Text (Text)
import GHC.Generics
import Network.HTTP.Client hiding (Proxy, path, port)
import Network.Wai.Handler.Warp
import Servant
import Servant.Async.Utils (jsonOptions)
import Servant.Client hiding (manager)
-- import Web.FormUrlEncoded hiding (parseMaybe)
import Servant.Scrapy.Schedule
import Servant.Async.Job
import Servant.Async.Client
import System.Environment

data ScraperInput = ScraperInput
  { _scin_spider :: !Text
  , _scin_query  :: !Text
  , _scin_user   :: !Text
  , _scin_corpus :: !Text
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
              -> (CallbackURL e o -> Schedule)
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
          [("query",    [input ^. scin_query])
          ,("user",     [input ^. scin_user])
          ,("corpus",   [input ^. scin_corpus])
       -- ,("report_every", ... Maybe Int
       -- ,("limit", ... Maybe Int
       -- ,("url", ... Text -- file name in local FS
       -- ,("count_only", ... Maybe Bool
          ,("callback", [toUrlPiece cb])]
      }
  where
    jurl :: JobServerURL ScraperStatus Schedule ScraperStatus
    jurl = JobServerURL url Callback

type API = AsyncJobsAPI ScraperStatus ScraperInput ScraperStatus

main :: IO ()
main = do
  [scrapyurl'] <- getArgs
  scrapyurl <- parseBaseUrl scrapyurl'
  selfurl <- parseBaseUrl "http://0.0.0.0:7000"
  manager <- newManager defaultManagerSettings
  job_env <- newJobEnv
  app <-
    serveWithCallbacks (Proxy :: Proxy API) selfurl manager (LogEvent logConsole) $ \client_env ->
      let
        f input log_status = async $ do
          e <- runJobMLog client_env log_status $ callScraper (URL scrapyurl) input
          either (fail . show) pure e

      in
        serveJobsAPI job_env (JobFunction f)
  run 7000 app
