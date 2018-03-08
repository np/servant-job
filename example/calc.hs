{-# LANGUAGE TypeOperators  #-}
{-# LANGUAGE DataKinds      #-}
{-# LANGUAGE KindSignatures #-}
import Prelude hiding (log)
import Data.Aeson
import Data.Foldable
import qualified Data.Text as T
import qualified Data.ByteString.Lazy.Char8 as LBS
import Servant
import Servant.Async.Job -- (serveJobAPI, JobAPI, JobOutput(..))
import Servant.Async.Client hiding (Env)
import Servant.Async.Utils ((?|))
import Servant.Client
import Control.Concurrent.Async (async)
import Control.Monad
import Control.Monad.IO.Class
import Network.HTTP.Client hiding (Proxy, path)
import Network.Wai.Handler.Warp

{-
p :: Polynomial
p = ([1,2,3,4,5], 3)

f x = 1 + 2 * x + 3 * (x ^ 2) + 4 * (x ^ 3) + 5 * (x ^ 4)
f 3 == 547
-}
type Polynomial = ([Int], Int)
type PolynomialAPI sas cs =
  QueryParam "sum"     JobServerAPI :>
  QueryParam "product" JobServerAPI :>
  QueryFlag  "pure" :>
  JobAPI sas cs Value Polynomial Int

type CalcAPI sas cs
    =  "sum"        :> JobAPI sas cs Value [Int] Int
  :<|> "product"    :> JobAPI sas cs Value [Int] Int
  :<|> "polynomial" :> PolynomialAPI sas cs

type API
    =  "sync"  :> CalcAPI 'Sync  'Server
  :<|> "async" :> CalcAPI 'Async 'Server

purePolynomial :: Polynomial -> Int
purePolynomial (coefs, input) =
  sum . zipWith (*) coefs $ iterate (input *) 1

makeURL :: String -> BaseUrl -> Maybe JobServerAPI -> JobServerURL e i o
makeURL path url m =
  JobServerURL (URL $ url { baseUrlPath = T.unpack (toUrlPiece sas) ++ "/" ++ path }) sas
  where
    sas = m ?| Sync

makeSumURL, makePrductURL :: BaseUrl -> Maybe JobServerAPI -> JobServerURL Int [Int] Int
makeSumURL    = makeURL "sum"
makePrductURL = makeURL "product"

jobPolynomial :: MonadJob m =>
                 JobServerURL Int [Int] Int ->
                 JobServerURL Int [Int] Int ->
                 Polynomial -> m Int
jobPolynomial sumU productU (coefs, input) = do
  let xs = iterate (input *) 1
  ys <- zipWithM (\x y -> callJob productU [x,y]) coefs xs
  callJob sumU ys

ioPolynomial :: MonadIO m => Env -> Maybe JobServerAPI -> Maybe JobServerAPI -> Bool -> (Value -> IO ()) -> Polynomial -> m Int
ioPolynomial env sumA prodA pureA log p =
  if pureA then do
    let r = purePolynomial p
    liftIO $ log (toJSON ("pure", p, r))
    pure r
  else do
    liftIO . runMonadJobIO (envManager env) (log . toJSON)
           . jobPolynomial sumU prodU $ p
  where
    baseU = envBaseURL env
    sumU  = makeSumURL    baseU sumA
    prodU = makePrductURL baseU prodA

data Env = Env
  { envManager :: Manager
  , envBaseURL :: BaseUrl
  , jobEnv     :: JobEnv Value Int
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

logConsole :: ToJSON a => a -> IO ()
logConsole = LBS.putStrLn . encode

serveSyncCalcAPI :: Env -> Server (CalcAPI 'Sync 'Server)
serveSyncCalcAPI env
    =  wrap (pure . sum)
  :<|> wrap (pure . product)
  :<|> \sumA prodA pureA ->
          wrap (ioPolynomial env sumA prodA pureA logConsole)

  where
    wrap f = fmap JobOutput . f

serveAsyncCalcAPI :: Env -> Server (CalcAPI 'Async 'Server)
serveAsyncCalcAPI env
{-
    =  wrap (pure . sum)
  :<|> wrap (pure . product)
-}
    =  wrap sumLog
  :<|> wrap productLog
  :<|> \sumA prodA pureA -> wrap (ioPolynomial env sumA prodA pureA)

  where
    wraplog log i = logConsole i >> log i
    wrap :: ((Value -> IO ()) -> i -> IO Int) -> Server (AsyncJobAPI Value i Int)
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
  a_manager <- newManager defaultManagerSettings
  job_env <- newJobEnv
  putStrLn "Server listening on port 3000"
  run 3000 . app $ Env a_manager url job_env
