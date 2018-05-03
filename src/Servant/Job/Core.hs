{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
module Servant.Job.Core
  -- Essentials
  ( ID
  , id_type
  , id_number
  , id_time
  , id_token
  , SymbolOf

  , deleteExpiredItems

  , Env
  , newEnv
  , env_secret_key
  , env_state_mvar
  , env_settings
  , generateSecretKey

  , EnvSettings
  , env_duration
  , defaultSettings

  , EnvState
  , env_map
  , env_next

  , EnvItem
  , env_timeout
  , env_item

  , Safety(..)
  , SecretKey

  -- Internals
  , checkID
  , newItem
  , getItem
  , deleteItem
  , isValidItem
  , forgetID
  , mkID
  , defaultDuration
  )
  where

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
import Data.Swagger hiding (items, URL, url)
import qualified Data.Text as T
import qualified Data.Text.Encoding as E
import Data.Time.Clock (UTCTime, NominalDiffTime, addUTCTime, getCurrentTime)
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds, posixSecondsToUTCTime)
import Prelude hiding (log)
import Servant
import System.IO
import Web.HttpApiData (ToHttpApiData(toUrlPiece), parseUrlPiece)
import GHC.TypeLits

type LBS = LBS.ByteString

newtype SecretKey = SecretKey LBS

data Safety = Safe | Unsafe

data ID (s :: Safety) (k :: Symbol) = PrivateID
  { _id_type   :: !String
  , _id_number :: !Int
  , _id_time   :: !UTCTime
  , _id_token  :: !String
  }
  deriving (Eq, Ord)

data EnvItem a = EnvItem
  { _env_timeout :: !UTCTime
  , _env_item    :: !a
  }

data EnvState a = EnvState
  { _env_map  :: !(IntMap.IntMap (EnvItem a))
  , _env_next :: !Int
  }

data EnvSettings = EnvSettings
  { _env_duration :: !NominalDiffTime
  -- ^ This duration specifies for how long one should keep an entry in this
  -- environment (a job for instance).
  -- Since internal identifiers are of type 'Int' the number of jobs should not
  -- exceed the bounds of integers within this duration.
  }

data Env a = Env
  { _env_secret_key :: !SecretKey
  , _env_state_mvar :: !(MVar (EnvState a))
  , _env_settings   :: !EnvSettings
  }

makeLensesWith (lensRules & generateSignatures .~ False) ''ID
-- TODO Lens' -> Getter ?
id_type   :: Lens' (ID safety k) String
id_number :: Lens' (ID safety k) Int
id_time   :: Lens' (ID safety k) UTCTime
id_token  :: Lens' (ID safety k) String
{-
id_type   :: Lens' (ID 'Safe k) String
id_number :: Lens' (ID 'Safe k) Int
id_time   :: Lens' (ID 'Safe k) UTCTime
id_token  :: Lens' (ID 'Safe k) String
-}

makeLenses ''Env
makeLenses ''EnvItem
makeLenses ''EnvState
makeLenses ''EnvSettings

type family SymbolOf (a :: *) :: Symbol

macID :: String -> SecretKey -> UTCTime -> Int -> String
macID t (SecretKey s) now n =
  showDigest . hmacSha256 s . LBS.fromStrict . E.encodeUtf8 $
    T.unwords [T.pack t, toUrlPiece (utcTimeToPOSIXSeconds now), toUrlPiece n]

forgetID :: ID safety k -> ID safety2 k
forgetID (PrivateID x y z t) = PrivateID x y z t

newID :: KnownSymbol k => Proxy k -> SecretKey -> UTCTime -> Int -> ID 'Safe k
newID p s t n = PrivateID tn n t $ macID tn s t n
  where tn = symbolVal p

mkID :: KnownSymbol k => Proxy k -> Int -> UTCTime -> String -> ID 'Unsafe k
mkID p n t d = PrivateID (symbolVal p) n t d

instance (KnownSymbol k, safety ~ 'Unsafe) => FromHttpApiData (ID safety k) where
  parseUrlPiece s =
    case T.splitOn "-" s of
      [n, t, d] -> mkID (Proxy :: Proxy k)
                     <$> parseUrlPiece n
                     <*> (posixSecondsToUTCTime <$> parseUrlPiece t)
                     <*> parseUrlPiece d
      _ -> Left "Invalid job identifier (expecting 3 parts separated by '-')"

instance {-safety ~ 'Safe =>-} ToHttpApiData (ID safety k) where
  toUrlPiece j =
    T.intercalate "-" [ toUrlPiece (j ^. id_number)
                      , toUrlPiece (utcTimeToPOSIXSeconds (j ^. id_time))
                      , toUrlPiece (j ^. id_token)]

instance (KnownSymbol k, safety ~ 'Unsafe) => FromJSON (ID safety k) where
  parseJSON s = either (fail . T.unpack) pure . parseUrlPiece =<< parseJSON s

instance {-safety ~ 'Safe =>-} ToJSON (ID safety k) where
  toJSON = toJSON . toUrlPiece

instance ToParamSchema (ID safety k) where
  toParamSchema _ = mempty
    & type_   .~ SwaggerString
    & pattern ?~ "[0-9]+-[0-9]+s-[0-9a-f]{64}"

instance KnownSymbol k => ToSchema (ID safety k) where
  declareNamedSchema p = pure . NamedSchema (Just "ID") $ mempty
    & title       ?~ T.pack k <> " idontifier"
    & paramSchema .~ toParamSchema p
    where
      k = symbolVal (Proxy :: Proxy k)

-- Default duration is one day
defaultDuration :: NominalDiffTime
defaultDuration = 86400 -- it is called nominalDay in time >= 1.8

-- The default suggested duration is one day: `newEnv defaultDuration`
defaultSettings :: EnvSettings
defaultSettings = EnvSettings { _env_duration = defaultDuration }

newEnv :: EnvSettings -> IO (Env a)
newEnv settings = do
  key <- generateSecretKey
  var <- newMVar $ EnvState mempty 0
  pure $ Env key var settings

generateSecretKey :: IO SecretKey
generateSecretKey = SecretKey <$> withBinaryFile "/dev/urandom" ReadMode (\h -> LBS.hGet h 16)

deleteItem :: MonadIO m => Env a -> ID 'Safe k -> m ()
deleteItem env jid =
  liftIO . modifyMVar_ (env ^. env_state_mvar) $ pure . (env_map . at (jid ^. id_number) .~ Nothing)

isValidItem :: UTCTime -> EnvItem a -> Bool
isValidItem now item = now < item ^. env_timeout

deleteExpiredItems :: (a -> IO ()) -> Env a -> IO ()
deleteExpiredItems gcItem env = do
  expired <- modifyMVar (env ^. env_state_mvar) gcItems
  mapM_ gcItem (expired ^.. each . env_item)

  where
    gcItems items = do
      now <- getCurrentTime
      let (valid, expired) = IntMap.partition (isValidItem now) (items ^. env_map)
      pure (items & env_map .~ valid, expired)

checkID :: (KnownSymbol k, k ~ SymbolOf a, MonadError ServantErr m, MonadIO m)
        => Env a -> ID 'Unsafe k -> m (ID 'Safe k)
checkID env i@(PrivateID tn n t d) = do
  now <- liftIO getCurrentTime
  when (tn /= symbolVal i) $
    throwError $ err401 { errBody = "Invalid identifier type name" }
  when (now > addUTCTime (env ^. env_settings . env_duration) t) $
    throwError $ err410 { errBody = "Expired identifier" }
  when (d /= macID tn (env ^. env_secret_key) t n) $
    throwError $ err401 { errBody = "Invalid identifier authentication code" }
  pure $ PrivateID tn n t d

newItem :: forall a k. (KnownSymbol k, k ~ SymbolOf a) => Env a -> IO a -> IO (ID 'Safe k, EnvItem a)
newItem env mkItem = do
  now <- getCurrentTime
  item <- EnvItem (addUTCTime (env ^. env_settings . env_duration) now) <$> mkItem
  ident <- modifyMVar (env ^. env_state_mvar) $ \items ->
    let n = items ^. env_next in
    pure (items & env_map . at n ?~ item
                & env_next +~ 1,
          newID (Proxy :: Proxy k) (env ^. env_secret_key) now n)
  pure (ident, item)

getItem :: (KnownSymbol k, k ~ SymbolOf a) => Env a -> ID 'Safe k -> Handler (EnvItem a)
getItem env ident = do
  m <- liftIO . readMVar $ env ^. env_state_mvar
  maybe notFound pure $ m ^. env_map . at (ident ^. id_number)

  where
    msg = "Not Found: " ++ symbolVal ident
    notFound = throwError $ err404 { errBody = LBS.pack msg, errReasonPhrase = msg }
