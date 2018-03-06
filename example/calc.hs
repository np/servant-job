{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds     #-}
import Data.Aeson
import Data.Foldable
import qualified Data.ByteString.Lazy.Char8 as LBS
import Servant
import Servant.Async.Job -- (serveJobAPI, JobAPI, JobOutput(..))
import Servant.Async.Client hiding (Env)
import Servant.Client
import Servant.Server
import Control.Concurrent.Async (async)
import Control.Monad
import Control.Monad.IO.Class
import Network.HTTP.Client hiding (Proxy)
import Network.Wai
import Network.Wai.Handler.Warp

{-
p :: Polynomial
p = ([1,2,3,4,5], 3)

f x = 1 + 2 * x + 3 * (x ^ 2) + 4 * (x ^ 3) + 5 * (x ^ 4)
f 3 == 547
-}
type Polynomial = ([Int], Int)

type SyncCalcAPI
    =  "sum"        :> SyncAPI [Int] Int
  :<|> "product"    :> SyncAPI [Int] Int
  :<|> "polynomial" :> SyncAPI Polynomial Int

type AsyncCalcAPI
    =  "sum"        :> JobAPI Value [Int] Int
  :<|> "product"    :> JobAPI Value [Int] Int
  :<|> "polynomial" :> JobAPI Value Polynomial Int

type API
    =  "sync"  :> SyncCalcAPI
  :<|> "async" :> AsyncCalcAPI

purePolynomial :: Polynomial -> Int
purePolynomial (coefs, input) =
  sum . zipWith (*) coefs $ iterate (input *) 1

apiPath :: JobServerAPI -> String
apiPath Sync  = "sync"
apiPath Async = "async"

makeURL :: String -> BaseUrl -> JobServerAPI -> JobServerURL e i o
makeURL path url api =
  JobServerURL (URL $ url { baseUrlPath = apiPath api ++ "/" ++ path }) api

makeSumURL, makePrductURL :: BaseUrl -> JobServerAPI -> JobServerURL Int [Int] Int
makeSumURL    = makeURL "sum"
makePrductURL = makeURL "product"

jobPolynomial :: MonadJob m =>
                 JobServerURL Int [Int] Int ->
                 JobServerURL Int [Int] Int ->
                 Polynomial -> m Int
jobPolynomial sum product (coefs, input) = do
  let xs = iterate (input *) 1
  ys <- zipWithM (\x y -> callJob product [x,y]) coefs xs
  callJob sum ys

ioPolynomial :: MonadIO m => Env -> (Value -> IO ()) -> Polynomial -> m Int
ioPolynomial env log =
  liftIO . runMonadJobIO (envManager env) (log . toJSON) -- _event
         . jobPolynomial sumURL productURL
  where
    -- log_event = LBS.putStrLn . encode
    base_url   = envBaseURL env
    sumURL     = makeSumURL    base_url Sync
    productURL = makePrductURL base_url Sync

data Env = Env
  { envManager :: Manager
  , envBaseURL :: BaseUrl
  , jobEnv     :: JobEnv Value Int
  }

foldrLog log f = foldrM (\x y -> log (toJSON y) >> pure (f x y))

sumLog     log = foldrLog log (+) 0
productLog log = foldrLog log (*) 1

logConsole = LBS.putStrLn . encode

serveSyncCalcAPI :: Env -> Server SyncCalcAPI
serveSyncCalcAPI env
    =  wrap (pure . sum)
  :<|> wrap (pure . product)
  -- :<|> wrap (pure . purePolynomial)
  :<|> wrap (ioPolynomial env logConsole)

  where
    wrap f = fmap JobOutput . f

serveAsyncCalcAPI :: Env -> Server AsyncCalcAPI
serveAsyncCalcAPI env
{-
    =  wrap (pure . sum)
  :<|> wrap (pure . product)
-}
    =  wrap sumLog
  :<|> wrap productLog
  :<|> wrap (ioPolynomial env)

  where
    wraplog log i = logConsole i >> log i
    wrap :: ((Value -> IO ()) -> i -> IO Int) -> Server (JobAPI Value i Int)
    wrap f = serveJobAPI (jobEnv env) (\i -> JobFunction (\log -> async (f (wraplog log) i)))

serveAPI :: Env -> Server API
serveAPI env
    =  serveSyncCalcAPI env
  :<|> serveAsyncCalcAPI env

calcAPI :: Proxy API
calcAPI = Proxy

app :: Env -> Application
app = serve calcAPI . serveAPI

main :: IO ()
main = do
  url <- parseBaseUrl "http://0.0.0.0:3000"
  manager <- newManager defaultManagerSettings
  job_env <- newJobEnv
  putStrLn "Server listening on port 3000"
  run 3000 . app $ Env manager url job_env
