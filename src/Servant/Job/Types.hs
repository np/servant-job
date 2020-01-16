{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}
module Servant.Job.Types where

import Control.Applicative
import Control.Concurrent.Chan
import Control.Concurrent.MVar (MVar)
import Control.DeepSeq (NFData)
import Control.Lens
import Control.Exception hiding (Handler)
import Control.Monad.Except
import Data.Aeson
import Data.Aeson.Types hiding (parseMaybe)
import qualified Data.HashMap.Strict as H
import Data.Set (Set)
import Data.Swagger hiding (url, URL)
import Data.Text (Text)
import qualified Data.Text as T
import GHC.Generics hiding (to)
import Network.HTTP.Client hiding (Proxy, path)
import Prelude hiding (log)
import Servant
import Servant.API.Flatten
import Servant.Client hiding (manager, ClientEnv)
import Servant.Job.Core as Core
import Servant.Job.Utils (jsonOptions, swaggerOptions, (</>))
import Servant.Types.SourceT (SourceT)
import Web.FormUrlEncoded

-- | Flavor: type of server to use
data APIMode = Sync | Stream | Async | Callback
  deriving (Eq, Ord, Generic)

instance ToJSON   APIMode
instance ToSchema APIMode

instance FromHttpApiData APIMode where
  parseUrlPiece "sync"     = pure Sync
  parseUrlPiece "stream"   = pure Stream
  parseUrlPiece "async"    = pure Async
  parseUrlPiece "callback" = pure Callback
  parseUrlPiece _          = Left "Unexpected value of type APIMode. Expecting sync, async, or callback"

instance ToHttpApiData APIMode where
  toUrlPiece Sync     = "sync"
  toUrlPiece Stream   = "stream"
  toUrlPiece Async    = "async"
  toUrlPiece Callback = "callback"

-----------
--- URL ---
-----------

newtype URL = URL { _base_url :: BaseUrl } deriving (Eq, Ord)

makeLenses ''URL

instance ToJSON URL where
  toJSON url = toJSON (showBaseUrl (url ^. base_url))

instance FromJSON URL where
  parseJSON = withText "URL" $ \t ->
    either (fail . displayException) (pure . URL) $ parseBaseUrl (T.unpack t)

instance FromHttpApiData URL where
  parseUrlPiece t =
    parseBaseUrl (T.unpack t) & _Left  %~ T.pack . displayException
                              & _Right %~ URL

instance ToHttpApiData URL where
  toUrlPiece = toUrlPiece . showBaseUrl . _base_url

mkURL :: BaseUrl -> String -> URL
mkURL url path = URL $ url { baseUrlPath = baseUrlPath url </> path }

data JobServerURL event input output = JobServerURL
  { _job_server_url  :: !URL
  , _job_server_mode :: !APIMode
  }
  deriving (Eq, Ord, Generic)

makeLenses ''JobServerURL

instance ToJSON (JobServerURL event input output) where
  toJSON = genericToJSON $ jsonOptions "_job_server_"

data NoCallbacks url = NoCallbacks
  deriving (Generic)

instance ToJSON (NoCallbacks url) where
  toJSON _ = Null

instance FromJSON (NoCallbacks url) where
  parseJSON Null = pure NoCallbacks
  parseJSON v    = prependFailure "parsing NoCallbacks failed, " (typeMismatch "Null" v)

instance Functor NoCallbacks where
  fmap _ NoCallbacks = NoCallbacks

instance Foldable NoCallbacks where
  foldMap _ NoCallbacks = mempty

instance Traversable NoCallbacks where
  traverse _ NoCallbacks = pure NoCallbacks

instance Applicative NoCallbacks where
  pure _  = NoCallbacks
  _ <*> _ = NoCallbacks

instance Alternative NoCallbacks where
  empty = NoCallbacks
  _ <|> _ = NoCallbacks

instance ToSchema (NoCallbacks a) where
  declareNamedSchema _ = pure $ NamedSchema (Just "NoCallbacks") $ mempty
    & type_ ?~ SwaggerNull
    & description ?~ "Only the `null` value."

data JobInput f a = JobInput
  { _job_input    :: !a
  , _job_callback :: !(f URL)
  }
  deriving Generic

makeLenses ''JobInput

instance (ToJSON (f URL), ToJSON input) => ToJSON (JobInput f input) where
  toJSON = genericToJSON $ jsonOptions "_job_"

instance (FromJSON (f URL), FromJSON input, Alternative f) => FromJSON (JobInput f input) where
  parseJSON v =  parseJobInput v
             <|> JobInput <$> parseJSON v <*> pure empty
             -- TODO improve parsing errors
    where
      parseJobInput (Object o) = JobInput <$> o .:  "input"
                                          <*> o .:? "callback" .!= empty
      parseJobInput _ = empty

instance ToForm input => ToForm (JobInput NoCallbacks input) where
  toForm i = toForm (i ^. job_input)

instance FromForm input => FromForm (JobInput NoCallbacks input) where
  fromForm f =
    JobInput <$> fromForm (Form (H.delete "input" (unForm f)))
             <*> pure NoCallbacks

instance ToForm input => ToForm (JobInput Maybe input) where
  toForm i =
    Form . H.insert "callback" (i ^.. job_callback . to toUrlPiece)
         $ unForm (toForm (i ^. job_input))

instance FromForm input => FromForm (JobInput Maybe input) where
  fromForm f =
    JobInput <$> fromForm (Form (H.delete "input" (unForm f)))
             <*> parseMaybe "callback" f

instance (ToSchema (f URL), ToSchema a) => ToSchema (JobInput f a) where
  declareNamedSchema = genericDeclareNamedSchema (swaggerOptions "_job_") &
    mapped . mapped . schema . required .~ ["input"]

newtype JobOutput a = JobOutput
  { _job_output :: a }
  deriving Generic

makeLenses ''JobOutput

instance ToJSON output => ToJSON (JobOutput output) where
  toJSON = genericToJSON $ jsonOptions "_job_"

instance FromJSON output => FromJSON (JobOutput output) where
  parseJSON = genericParseJSON $ jsonOptions "_job_"

instance ToSchema a => ToSchema (JobOutput a) where
  declareNamedSchema = genericDeclareNamedSchema $ swaggerOptions "_job_"

instance NFData a => NFData (JobOutput a)

type JobID safety = ID safety "job"

-- some states to help:
-- https://docs.celeryproject.org/en/latest/reference/celery.states.html#misc
data States = IsPending
            | IsReceived
            | IsStarted
            | IsRunning
            | IsKilled
            | IsFailure
            -- | IsRevoked
            -- | IsRetry
            | IsFinished
  deriving (Eq, Generic)

instance ToJSON States
instance FromJSON States

instance ToSchema States where
  declareNamedSchema = genericDeclareNamedSchema $ swaggerOptions ""


data JobStatus safety event = JobStatus
  { _job_id     :: !(JobID safety)
  , _job_log    :: ![event]
  , _job_status :: !States -- TODO: should be a type Started | Finished ...
  }
  deriving Generic

newtype Limit  = Limit  { unLimit  :: Int } deriving (ToHttpApiData, FromHttpApiData, Generic)
newtype Offset = Offset { unOffset :: Int } deriving (ToHttpApiData, FromHttpApiData, Generic)

type JobStatusAPI meth safetyO event =
  QueryParam "limit"  Limit  :>
  QueryParam "offset" Offset :>
  meth '[JSON] (JobStatus safetyO event)

type AsyncJobAPI' safetyO ctO event output
    =  "kill" :> JobStatusAPI Post safetyO event
  :<|> "poll" :> JobStatusAPI Get  safetyO event
  :<|> "wait" :> Get ctO (JobOutput output)

type AsyncJobsAPI' safetyI safetyO ctI ctO callbacks event input output
    =  "nobody" :> Post '[JSON] (JobStatus safetyO event)
  :<|> ReqBody ctI (JobInput callbacks input) :> Post '[JSON] (JobStatus safetyO event)
  :<|> Capture "id" (JobID safetyI) :> AsyncJobAPI' safetyO ctO event output

type AsyncJobAPI event output = AsyncJobAPI' 'Safe '[JSON] event output

type AsyncJobsAPI event input output =
  Flat (AsyncJobsAPI' 'Unsafe 'Safe '[JSON] '[JSON] Maybe event input output)

type AsyncJobsServerT' ctI ctO callbacks event input output m =
  ServerT (Flat (AsyncJobsAPI' 'Unsafe 'Safe ctI ctO callbacks event input output)) m

type AsyncJobsServerT event input output m = AsyncJobsServerT' '[JSON] '[JSON] Maybe event input output m

type AsyncJobsServer' ctI ctO callbacks event input output = AsyncJobsServerT' ctI ctO callbacks event input output Handler

type AsyncJobsServer event input output = AsyncJobsServerT event input output Handler

makeLenses ''JobStatus

instance (safety ~ 'Safe, ToJSON event) => ToJSON (JobStatus safety event) where
  toJSON = genericToJSON $ jsonOptions "_job_"

instance (safety ~ 'Unsafe, FromJSON event) => FromJSON (JobStatus safety event) where
  parseJSON = genericParseJSON $ jsonOptions "_job_"

instance ToSchema event => ToSchema (JobStatus safety event) where
  declareNamedSchema = genericDeclareNamedSchema $ swaggerOptions "_job_"

type SyncJobsAPI' ctI ctO input output =
  ReqBody ctI input :>
  Post ctO (JobOutput output)

type SyncJobsAPI input output = SyncJobsAPI' '[JSON] '[JSON] input output

proxySyncJobsAPI :: proxy input output -> Proxy (SyncJobsAPI input output)
proxySyncJobsAPI _ = Proxy

data JobFrame event output = JobFrame
  { _job_frame_event  :: Maybe event
  , _job_frame_output :: Maybe output
  }
  deriving (Generic)

instance (FromJSON event, FromJSON output) => FromJSON (JobFrame event output) where
  parseJSON = genericParseJSON $ jsonOptions "_job_frame_"

instance (ToJSON event, ToJSON output) => ToJSON (JobFrame event output) where
  toJSON = genericToJSON $ jsonOptions "_job_frame_"

type StreamJobsAPI' m ctI ctO event input output =
  ReqBody ctI input :>
  StreamPost NewlineFraming ctO (SourceT m (JobFrame event output))

type StreamJobsAPI event input output =
  StreamJobsAPI' IO '[JSON, FormUrlEncoded] JSON event input output

proxyStreamJobsAPI :: proxy event input output -> Proxy (StreamJobsAPI event input output)
proxyStreamJobsAPI _ = Proxy

type ChanID safety = ID safety "chan"

type CallbackAPI error event output
    =  "event"  :> ReqBody '[JSON] event  :> Post '[JSON] ()
  :<|> "error"  :> ReqBody '[JSON] error  :> Post '[JSON] ()
  :<|> "output" :> ReqBody '[JSON] output :> Post '[JSON] ()

newtype AnyError  = AnyError  Value
  deriving (FromJSON, ToJSON)
newtype AnyEvent  = AnyEvent  Value
  deriving (FromJSON, ToJSON)
newtype AnyInput  = AnyInput  Value
  deriving (FromJSON, ToJSON)
newtype AnyOutput = AnyOutput Value
  deriving (FromJSON, ToJSON)

instance ToSchema AnyOutput where
  declareNamedSchema _ = pure $ NamedSchema (Just "AnyOutput") $ mempty
    & description ?~ "Arbitrary JSON value."
    & additionalProperties ?~ AdditionalPropertiesAllowed True

instance ToSchema AnyInput where
  declareNamedSchema _ = pure $ NamedSchema (Just "AnyInput") $ mempty
    & description ?~ "Arbitrary JSON value."
    & additionalProperties ?~ AdditionalPropertiesAllowed True

instance ToSchema AnyEvent where
  declareNamedSchema _ = pure $ NamedSchema (Just "AnyEvent") $ mempty
    & description ?~ "Arbitrary JSON value."
    & additionalProperties ?~ AdditionalPropertiesAllowed True

instance ToSchema AnyError where
  declareNamedSchema _ = pure $ NamedSchema (Just "AnyError") $ mempty
    & description ?~ "Arbitrary JSON value."
    & additionalProperties ?~ AdditionalPropertiesAllowed True

type CallbacksAPI = Capture "id" (ChanID 'Unsafe) :> CallbackAPI AnyError AnyEvent AnyOutput

type CallbacksServerT m = ServerT (Flat CallbacksAPI) m
type CallbacksServer = CallbacksServerT Handler

-- This is internally almost equivalent to JobInput
-- in JobInput the callback is optional.
data CallbackInput event input output = CallbackInput
  { _cbi_input    :: !input
  , _cbi_callback :: !URL
  } deriving (Generic)

makeLenses ''CallbackInput

instance FromJSON input => FromJSON (CallbackInput event input output) where
  parseJSON = genericParseJSON $ jsonOptions "_cbi_"

instance ToJSON input => ToJSON (CallbackInput event input output) where
  toJSON = genericToJSON $ jsonOptions "_cbi_"

instance ToForm input => ToForm (CallbackInput event input output) where
  toForm cbi =
    Form . H.insert "callback" (cbi ^.. cbi_callback . to toUrlPiece)
         $ unForm (toForm (cbi ^. cbi_input))

instance FromForm input => FromForm (CallbackInput event input output) where
  fromForm f =
    CallbackInput
      <$> fromForm (Form (H.delete "callback" (unForm f)))
      <*> parseUnique "callback" f

type CallbackJobsAPI' ctI ctO event input output =
  ReqBody ctI (CallbackInput event input output) :> Post ctO ()

type CallbackJobsAPI event input output =
  CallbackJobsAPI' '[JSON, FormUrlEncoded] '[JSON] event input output

type family   JobsAPI' (mode :: APIMode)
                       (ctI :: [*]) ctO event input output
type instance JobsAPI' 'Sync     ctI ctO event input output = SyncJobsAPI' ctI '[ctO] input output
type instance JobsAPI' 'Stream   ctI ctO event input output = StreamJobsAPI' IO ctI ctO event input output
type instance JobsAPI' 'Async    ctI ctO event input output = AsyncJobsAPI' 'Unsafe 'Safe ctI '[ctO] Maybe event input output
-- TODO provide also a version of Async without the callbacks
type instance JobsAPI' 'Callback ctI ctO event input output = CallbackJobsAPI' ctI '[ctO] event input output

type JobsAPI mode ctI ctO event input output = Flat (JobsAPI' mode ctI ctO event input output)

data ChanMessage error event input output = ChanMessage
  { _msg_event  :: !(Maybe event)
  , _msg_result :: !(Maybe output)
  , _msg_error  :: !(Maybe error)
  }
  deriving (Generic)

makeLenses ''ChanMessage

type AnyChanMessage = ChanMessage AnyError AnyEvent AnyInput AnyOutput

{-
_ChanEvent  :: Prism' (ChanMessage error event input output) event
_ChanEvent =
_ChanResult :: Prism' (ChanMessage error event input output) output
_ChanResult =
_ChanError  :: Prism' (ChanMessage error event input output) String
_ChanError =
-}
mkChanEvent  :: event -> ChanMessage error event input output
mkChanEvent e = ChanMessage (Just e) Nothing Nothing
mkChanResult :: output -> ChanMessage error event input output
mkChanResult output = ChanMessage Nothing (Just output) Nothing
mkChanError  :: error -> ChanMessage error event input output
mkChanError m = ChanMessage Nothing Nothing (Just m)

instance (ToJSON error, ToJSON event, ToJSON output) => ToJSON (ChanMessage error event input output) where
  toJSON = genericToJSON $ jsonOptions "_msg_"

instance (FromJSON error, FromJSON event, FromJSON output) => FromJSON (ChanMessage error event input output) where
  parseJSON = genericParseJSON $ jsonOptions "_msg_"

type instance SymbolOf (Chan Value) = "chan"

type ChansEnv = Core.Env (Chan Value)

data Chans = Chans
  { _chans_env :: !ChansEnv
  , _chans_url :: !URL
  }

makeLenses ''Chans

data RunningJob event input output = PrivateRunningJob
  { _running_job_url :: URL
  , _running_job_api :: APIMode
  , _running_job_id  :: JobID 'Unsafe
  }
  deriving (Eq, Ord, Generic)

makeLenses ''RunningJob

data Event event input output
  = NewTask  { _event_server :: JobServerURL event input output }
  | Started  { _event_server :: JobServerURL event input output
             , _event_job_id :: Maybe (JobID 'Unsafe) }
  | Finished { _event_server :: JobServerURL event input output
             , _event_job_id :: Maybe (JobID 'Unsafe) }
  | Event    { _event_server :: JobServerURL event input output
             , _event_job_id :: Maybe (JobID 'Unsafe)
             , _event_event  :: event }
  | BadEvent { _event_server :: JobServerURL event input output
             , _event_job_id :: Maybe (JobID 'Unsafe)
             , _event_event_value :: Value }
  | Debug event
  deriving (Generic)

instance ToJSON event => ToJSON (Event event input output) where
  toJSON = genericToJSON $ jsonOptions "_event_"

newtype LogEvent = LogEvent
  { unLogEvent :: forall event input output. ToJSON event => Event event input output -> IO () }

data ClientEnv = ClientEnv
  { _cenv_manager          :: !Manager
  , _cenv_polling_delay_ms :: !Int
  , _cenv_log_event        :: !LogEvent
  , _cenv_jobs_mvar        :: !(MVar (Set (RunningJob Value Value Value)))
  , _cenv_chans            :: !Chans
  }

makeLenses ''ClientEnv
