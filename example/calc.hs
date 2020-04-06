{-# LANGUAGE FlexibleContexts             #-}
{-# LANGUAGE TypeOperators                #-}
{-# LANGUAGE DataKinds                    #-}
{-# LANGUAGE DeriveGeneric                #-}
{-# LANGUAGE GeneralizedNewtypeDeriving   #-}
{-# LANGUAGE KindSignatures               #-}
{-# LANGUAGE ScopedTypeVariables          #-}
{-# LANGUAGE OverloadedStrings            #-}
import Prelude hiding (log)
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (MVar, newMVar, readMVar, modifyMVar_)
import Control.Applicative
import Control.Lens
import Control.Monad
import Control.Monad.Reader
import Control.Monad.Except
import Control.Monad.Trans.Control
import Data.Aeson
import Data.Char (isDigit)
import Data.Foldable
import qualified Data.Text as T
import qualified Data.ByteString.Lazy.Char8 as LBS
import Servant
import Servant.Job.Async
import Servant.Job.Types
import Servant.Job.Server
import Servant.Job.Client
import Servant.Job.Utils
import Servant.Client hiding (manager, ClientEnv)
import System.Environment
import Network.HTTP.Client hiding (Proxy, path, port)
import Network.Wai.Handler.Warp hiding (defaultSettings)
import GHC.Generics
import Web.FormUrlEncoded hiding (parseMaybe)

{-
p :: Polynomial
p = ([1,2,3,4,5], 3)

f x = 1 + 2 * x + 3 * (x ^ 2) + 4 * (x ^ 3) + 5 * (x ^ 4)
f 3 == 547
-}
data Polynomial = P
  { _poly_c :: [Int]
  , _poly_x :: Int
  }
  deriving Generic

instance ToJSON Polynomial where
  toJSON = genericToJSON (jsonOptions "_poly_")

instance FromJSON Polynomial where
  parseJSON v =
    (uncurry P <$> parseJSON v) <|> genericParseJSON (jsonOptions "_poly_") v

instance FromForm Polynomial where
  fromForm = genericFromForm $ formOptions "_poly_"

instance ToForm Polynomial where
  toForm = genericToForm $ formOptions "_poly_"

data Ints = Ints { _ints_x :: [Int] }
  deriving Generic

instance ToJSON Ints where
  toJSON = genericToJSON (jsonOptions "_ints_")

instance FromJSON Ints where
  parseJSON v =
    (Ints <$> parseJSON v) <|> genericParseJSON (jsonOptions "_ints_") v

instance FromForm Ints where
  fromForm = genericFromForm $ formOptions "_ints_"

instance ToForm Ints where
  toForm = genericToForm $ formOptions "_ints_"

newtype Delay = Delay { unDelay :: Double } deriving (FromHttpApiData)

type IntOpAPI input mode =
  QueryParam "delay" Delay :>
  JobsAPI mode '[JSON, FormUrlEncoded] JSON Value input Int

type PolynomialAPI mode =
  QueryParam "sum"     APIMode :>
  QueryParam "product" APIMode :>
  QueryFlag  "pure" :>
  IntOpAPI Polynomial mode

type CalcAPI mode
    =  "sum"        :> IntOpAPI Ints mode
  :<|> "product"    :> IntOpAPI Ints mode
  :<|> "polynomial" :> PolynomialAPI mode

type API
    =  "sync"     :> CalcAPI 'Sync
  :<|> "stream"   :> CalcAPI 'Stream
  :<|> "async"    :> CalcAPI 'Async
  :<|> "callback" :> CalcAPI 'Callback


purePolynomial :: Polynomial -> Int
purePolynomial (P coefs input) =
  sum . zipWith (*) coefs $ iterate (input *) 1

makeJobServerURL :: String -> BaseUrl -> Maybe APIMode -> JobServerURL e input o
makeJobServerURL path url m =
    JobServerURL (mkURL url (T.unpack (toUrlPiece mode) </> path)) mode
  where
    mode = m ?| Sync

makeSumURL, makePrductURL :: BaseUrl -> Maybe APIMode -> JobServerURL Int [Int] Int
makeSumURL    = makeJobServerURL "sum"
makePrductURL = makeJobServerURL "product"

jobPolynomial :: MonadJob m
              => JobServerURL Int [Int] Int
              -> JobServerURL Int [Int] Int
              -> Polynomial
              -> m Int
jobPolynomial sumU productU (P coefs input) = do
  let xs = iterate (input *) 1
  ys <- zipWithM (\x y -> callJob productU [x,y]) coefs xs
  callJob sumU ys

ioPolynomial :: MonadBaseControl IO m
             => Env
             -> Maybe APIMode
             -> Maybe APIMode
             -> Bool
             -> (Value -> IO ())
             -> Polynomial
             -> m Int
ioPolynomial env sumA prodA pureA log p =
  if pureA then do
    let r = purePolynomial p
    -- liftIO $ log (toJSON ("pure", p, r))
    pure r
  else do
    e <- runJobMLog (envClient env) log $ jobPolynomial sumU prodU $ p
    either (fail . show) pure e

  where
    baseU = envBaseURL env
    sumU  = makeSumURL    baseU sumA
    prodU = makePrductURL baseU prodA

data Env = Env
  { envBaseURL  :: !BaseUrl
  , jobEnv      :: !(JobEnv Value Int)
  , envClient   :: !ClientEnv
  , envTestMVar :: !(MVar [([T.Text], Any)])
  }

foldrLog ::
  (Foldable c, Monad m, ToJSON b) =>
  (Value -> m ()) -> (a -> b -> b) -> b -> c a -> m b
foldrLog log f z c = do
  b <- foldrM (\a b -> log (toJSON b) >> pure (f a b)) z c
  log (toJSON b)
  pure b

sumLog, productLog ::
  (Num a, Foldable c, Monad m, ToJSON a) =>
  (Value -> m ()) -> c a -> m a
sumLog     log = foldrLog log (+) 0
productLog log = foldrLog log (*) 1

sumIntsLog, productIntsLog :: (Value -> IO ()) -> Ints -> IO Int
sumIntsLog log = sumLog log . _ints_x
productIntsLog log = productLog log . _ints_x

logConsole :: ToJSON a => a -> IO ()
logConsole = LBS.putStrLn . encode

serveSyncCalcAPI :: Env -> Server (CalcAPI 'Sync)
serveSyncCalcAPI env
    =  wrap sumIntsLog
  :<|> wrap productIntsLog
  :<|> \sumA prodA pureA -> wrap (ioPolynomial env sumA prodA pureA)

  where
    wrap :: ((Value -> IO ()) -> i -> IO Int) -> Maybe Delay
         -> Server (SyncJobsAPI i Int)
    wrap f delayA i = liftIO $ JobOutput <$> f log i
      where
        log e = do
          waitDelay delayA
          logConsole e

serveStreamCalcAPI :: Env -> Server (CalcAPI 'Stream)
serveStreamCalcAPI env
    =  wrap sumIntsLog
  :<|> wrap productIntsLog
  :<|> \sumA prodA pureA -> wrap (ioPolynomial env sumA prodA pureA)

  where
    wrap :: ((Value -> IO ()) -> i -> IO Int) -> Maybe Delay
         -> Server (StreamJobsAPI Value i Int)
    wrap f delayA i = pure . simpleStreamGenerator $ \emit -> do
      let log e = do
            waitDelay delayA
            logConsole e
            emit (JobFrame (Just e) Nothing)
      r <- f log i
      emit (JobFrame Nothing (Just r))

serveAsyncCalcAPI :: Env -> Server (CalcAPI 'Async)
serveAsyncCalcAPI env
    =  wrap sumIntsLog
  :<|> wrap productIntsLog
  :<|> \sumA prodA pureA -> wrap (ioPolynomial env sumA prodA pureA)

  where
    wrap :: (FromJSON i, ToJSON i) => ((Value -> IO ()) -> i -> IO Int) -> Maybe Delay
         -> Server (AsyncJobsAPI Value i Int)
    wrap f delayA =
      let wraplog log i = waitDelay delayA >> logConsole i >> log i in
      simpleServeJobsAPI (jobEnv env) (simpleJobFunction (\i log -> f (wraplog log) i))

runClientCallbackIO :: (ToJSON error, ToJSON event, ToJSON input, ToJSON output)
                    => ClientEnv -> URL
                    -> ChanMessage error event input output
                    -> IO ()
runClientCallbackIO env cb_url msg =
  runExceptT (runReaderT (clientCallback cb_url msg) env)
    >>= either (fail . show) pure

waitDelay :: Maybe Delay -> IO ()
waitDelay = mapM_ (threadDelay . ceiling . (* oneSecond) . unDelay)
  where
    oneSecond = 1000000

serveCallbackCalcAPI :: Env -> Server (CalcAPI 'Callback)
serveCallbackCalcAPI env
    =  wrap sumIntsLog
  :<|> wrap productIntsLog
  :<|> \sumA prodA pureA -> wrap (ioPolynomial env sumA prodA pureA)

  where
    wrap :: forall input. ToJSON input => ((Value -> IO ()) -> input -> IO Int)
         -> Maybe Delay -> Server (CallbackJobsAPI Value input Int)
    wrap f delayA cbi = liftIO $ do
      let
        log_event :: ChanMessage () Value input Int -> IO ()
        log_event e = do
          waitDelay delayA
          runClientCallbackIO (envClient env) (cbi ^. cbi_callback) e
      r <- f (log_event . mkChanEvent) (cbi ^. cbi_input)
      log_event (mkChanResult r)

serveAPI :: Env -> Server API
serveAPI env
    =  serveSyncCalcAPI     env
  :<|> serveStreamCalcAPI   env
  :<|> serveAsyncCalcAPI    env
  :<|> serveCallbackCalcAPI env

data Any = Any Value

instance FromForm Any where
  fromForm = pure . Any . toJSON . unForm

instance FromJSON Any where
  parseJSON = pure . Any

instance ToJSON Any where
  toJSON (Any x) = x

type TestAPI
    =  "push" :> CaptureAll "segments" T.Text
              :> ReqBody '[JSON, FormUrlEncoded] Any
              :> Post '[JSON] ()
  :<|> "pull" :> Get '[JSON] [([T.Text], Any)]
  :<|> "clear" :> PostNoContent '[JSON] ()

serveTestAPI :: Env -> Server TestAPI
serveTestAPI env
    =  (\segs val -> liftIO (modifyMVar_ m (pure . ((segs,val):))))
  :<|> liftIO (readMVar m)
  :<|> liftIO (modifyMVar_ m (pure . const []))
  where
    m = envTestMVar env

type TopAPI
    =  API
  :<|> "test" :> TestAPI

portOpt :: [String] -> Maybe Int
portOpt [p] | not (null p) && all isDigit p = Just $ read p
portOpt _ = Nothing

main :: IO ()
main = do
  args <- getArgs
  let (Just port) = portOpt args
  url <- parseBaseUrl $ "http://0.0.0.0:" ++ show port
  manager <- newManager defaultManagerSettings
  jenv <- newJobEnv defaultSettings manager
  testMVar <- newMVar []
  putStrLn $ "Server listening on port: " ++ show port
  app <-
    serveApiWithCallbacks (Proxy :: Proxy TopAPI) defaultSettings url
                          manager (LogEvent logConsole) $ \client_env ->
      let env = Env url jenv client_env testMVar in
      serveAPI env :<|> serveTestAPI env
  run port app
