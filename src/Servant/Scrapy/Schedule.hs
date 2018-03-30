{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}
module Servant.Scrapy.Schedule where

import Control.Lens
import Data.Aeson
import qualified Data.HashMap.Strict as H
import Data.Text (Text)
import GHC.Generics
import Servant
import Servant.Async.Utils (jsonOptions)
import Servant.Client
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
  { r_status :: !Text
  , r_jobid  :: !Text
  }
  deriving (Generic)

instance FromJSON ScheduleResponse where
  parseJSON = genericParseJSON (jsonOptions "r_")

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

scrapySchedule :: Schedule -> ClientM ScheduleResponse
scrapySchedule = client scrapyAPI
