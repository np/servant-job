{-# LANGUAGE TypeOperators  #-}
{-# LANGUAGE DataKinds      #-}
{-# LANGUAGE DeriveGeneric  #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
import Prelude hiding (log)
import Control.Concurrent.MVar (MVar, newMVar, readMVar, modifyMVar_)
import Control.Applicative
import Control.Lens
import Control.Concurrent.Async (async)
import Control.Monad
import Control.Monad.Reader
import Control.Monad.Except
import Data.Aeson
import Data.Char (isDigit)
import Data.Foldable
import qualified Data.Text as T
import qualified Data.ByteString.Lazy.Char8 as LBS
import Servant
import Servant.API.Flatten
import Servant.Async.Job
import Servant.Async.Server
import Servant.Async.Client
import Servant.Async.Utils
import Servant.Client hiding (manager, ClientEnv)
import System.Environment
import Network.HTTP.Client hiding (Proxy, path, port)
import Network.Wai.Handler.Warp
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

type PolynomialAPI sas cs ctI =
  QueryParam "sum"     JobServerAPI :>
  QueryParam "product" JobServerAPI :>
  QueryFlag  "pure" :>
  JobsAPI sas cs ctI JSON Value Polynomial Int

type CalcAPI sas cs ctI
    =  "sum"        :> JobsAPI       sas cs ctI JSON Value Ints Int
  :<|> "product"    :> JobsAPI       sas cs ctI JSON Value Ints Int
  :<|> "polynomial" :> PolynomialAPI sas cs ctI

type API' cs ctI
    =  "sync"     :> CalcAPI 'Sync     cs ctI
  :<|> "async"    :> CalcAPI 'Async    cs ctI
  :<|> "stream"   :> CalcAPI 'Stream   cs ctI
  :<|> "callback" :> CalcAPI 'Callback cs ctI

type API = API' 'Server '[JSON, FormUrlEncoded]

purePolynomial :: Polynomial -> Int
purePolynomial (P coefs input) =
  sum . zipWith (*) coefs $ iterate (input *) 1

makeJobServerURL :: String -> BaseUrl -> Maybe JobServerAPI -> JobServerURL e i o
makeJobServerURL path url m =
    JobServerURL (mkURL url (T.unpack (toUrlPiece sas) </> path)) sas
  where
    sas = m ?| Sync

makeSumURL, makePrductURL :: BaseUrl -> Maybe JobServerAPI -> JobServerURL Int [Int] Int
makeSumURL    = makeJobServerURL "sum"
makePrductURL = makeJobServerURL "product"

jobPolynomial :: MonadJob m =>
                 JobServerURL Int [Int] Int ->
                 JobServerURL Int [Int] Int ->
                 Polynomial -> m Int
jobPolynomial sumU productU (P coefs input) = do
  let xs = iterate (input *) 1
  ys <- zipWithM (\x y -> callJob productU [x,y]) coefs xs
  callJob sumU ys

ioPolynomial :: MonadIO m => Env -> Maybe JobServerAPI -> Maybe JobServerAPI -> Bool -> (Value -> IO ()) -> Polynomial -> m Int
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

serveSyncCalcAPI :: Env -> Server (CalcAPI 'Sync 'Server '[JSON])
serveSyncCalcAPI env
    =  wrap (pure . sum . _ints_x)
  :<|> wrap (pure . product . _ints_x)
  :<|> \sumA prodA pureA ->
          wrap (ioPolynomial env sumA prodA pureA logConsole)

  where
    wrap f = fmap JobOutput . f

serveStreamCalcAPI :: Env -> Server (CalcAPI 'Stream 'Server '[JSON])
serveStreamCalcAPI env
    =  wrap sumIntsLog
  :<|> wrap productIntsLog
  :<|> \sumA prodA pureA -> wrap (ioPolynomial env sumA prodA pureA)

  where
    wrap :: ((Value -> IO ()) -> i -> IO Int) -> Server (StreamJobsAPI 'Server Value i Int)
    wrap f i = pure . simpleStreamGenerator $ \emit -> do
      r <- f (\e -> emit $ JobFrame (Just e) Nothing) i
      emit (JobFrame Nothing (Just (JobOutput r)))

serveAsyncCalcAPI :: Env -> Server (CalcAPI 'Async 'Server '[JSON])
serveAsyncCalcAPI env
    =  wrap sumIntsLog
  :<|> wrap productIntsLog
  :<|> \sumA prodA pureA -> wrap (ioPolynomial env sumA prodA pureA)

  where
    wraplog log i = logConsole i >> log i
    wrap :: ((Value -> IO ()) -> i -> IO Int) -> Server (Flat (AsyncJobsAPI Value i Int))
    wrap f = serveJobsAPI (jobEnv env) (JobFunction (\i log -> async (f (wraplog log) i)))

runClientCallbackIO :: (ToJSON e, ToJSON i, ToJSON o) => ClientEnv -> URL -> ChanMessage e i o -> IO ()
runClientCallbackIO env cb_url msg =
  runExceptT (runReaderT (clientCallback cb_url msg) env)
    >>= either (fail . show) pure

serveCallbackCalcAPI :: Env -> Server (CalcAPI 'Callback 'Server '[JSON])
serveCallbackCalcAPI env
    =  wrap sumIntsLog
  :<|> wrap productIntsLog
  :<|> \sumA prodA pureA -> wrap (ioPolynomial env sumA prodA pureA)

  where
    wrap :: forall i. ToJSON i => ((Value -> IO ()) -> i -> IO Int) -> Server (CallbackJobsAPI Value i Int)
    wrap f cbi = liftIO $ do
      let
        log_event :: ChanMessage Value i Int -> IO ()
        log_event = runClientCallbackIO (envClient env) (cbi ^. cbi_callback)
      r <- f (log_event . mkChanEvent) (cbi ^. cbi_input)
      log_event (mkChanResult r)

serveAPI :: Env -> Server API
serveAPI env
    =  serveSyncCalcAPI env
  :<|> serveAsyncCalcAPI env
  :<|> serveStreamCalcAPI env
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
  job_env <- newJobEnv
  testMVar <- newMVar []
  putStrLn $ "Server listening on port: " ++ show port
  app <-
    serveApiWithCallbacks (Proxy :: Proxy TopAPI) url
                          manager (LogEvent logConsole) $ \client_env ->
      let env = Env url job_env client_env testMVar in
      serveAPI env :<|> serveTestAPI env
  run port app
