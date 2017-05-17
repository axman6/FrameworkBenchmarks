{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TypeOperators         #-}
module ServantBench (run) where

import           Control.Exception          (bracket)
import           Control.Monad              (replicateM)
import           Control.Monad.IO.Class     (liftIO)
import           Data.Aeson                 hiding (json)
import qualified Data.ByteString            as BS
import           Data.ByteString.Lazy
import           Data.Functor.Contravariant (contramap)
import           Data.Int                   (Int32)
import           Data.List                  (sortOn)
import           Data.Maybe                 (fromMaybe)
import           Data.Monoid                ((<>))
import qualified Data.Text                  as Text
import qualified Data.Vector                as Vec
import           GHC.Exts                   (IsList (fromList))
import           GHC.Generics               (Generic)
import qualified Hasql.Decoders             as HasqlDec
import qualified Hasql.Encoders             as HasqlEnc
import           Hasql.Pool                 (Pool, acquire, release, use)
import qualified Hasql.Query                as Hasql
import           Hasql.Session              (Session, query)
import           Lucid
import           Network.HTTP.Media         ((//))
import qualified Network.Wai.Handler.Warp   as Warp
import           Servant
import           Servant.HTML.Lucid         (HTML)
import           System.Random.MWC          (GenIO, createSystemRandom,
                                             uniformR)

type API =
       "json"      :> Get '[JSON] Value
  :<|> "db"        :> Get '[JSON] World
  :<|> "queries"   :> QueryParam "queries" Int :> Get '[JSON] [World]
  :<|> "fortune"   :> Get '[HTML] (Html ())
  :<|> "updates"   :> QueryParam "queries" Int :> Get '[JSON] [World]
  :<|> "plaintext" :> Get '[Plain] ByteString

api :: Proxy API
api = Proxy

server :: Pool -> GenIO -> Server API
server pool gen =
      json
 :<|> clean (liftIO $ use pool $ singleDb gen)
 :<|> multipleDb pool gen
 :<|> fortunes pool
 :<|> updates pool gen
 :<|> plaintext

run :: Warp.Port -> BS.ByteString -> IO ()
run port dbSettings = do
  gen <- createSystemRandom
  bracket (acquire settings) release $ \pool ->
    Warp.run port $ serve api $ server pool gen
  where
    halfSecond = 0.5
    settings = (30, halfSecond, dbSettings)


data World = World { wId :: !Int32 , wRandomNumber :: !Int32 }
  deriving (Show, Generic)

instance ToJSON World where
  toEncoding w =
    pairs (  "id"           .= wId w
          <> "randomNumber" .= wRandomNumber w
          )

data Fortune = Fortune { fId :: !Int32 , fMessage :: Text.Text }
  deriving (Show, Generic)

instance ToJSON Fortune where
  toEncoding f =
    pairs (  "id"      .= fId f
          <> "message" .= fMessage f
          )

intValEnc :: HasqlEnc.Params Int32
intValEnc = HasqlEnc.value HasqlEnc.int4
intValDec :: HasqlDec.Row Int32
intValDec = HasqlDec.value HasqlDec.int4

-- Helper to handle Hasql's Either results
clean :: Show e => Handler (Either e a) -> Handler a
clean m = m >>= either (\_ -> throwError err500) pure

-- * PlainText without charset

data Plain
instance Accept Plain where contentType _ = "text" // "plain"
instance MimeRender Plain ByteString where
  mimeRender _ = id
  {-# INLINE mimeRender #-}

------------------------------------------------------------------------------

-- * Test 1: JSON serialization

json :: Handler Value
json = return . Object $ fromList [("message", "Hello, World!")]
{-# INLINE json #-}


-- * Test 2: Single database query

selectSingle :: Hasql.Query Int32 World
selectSingle = Hasql.statement q intValEnc decoder True
  where
   q = "SELECT * FROM World WHERE (id = $1)"
   decoder = HasqlDec.singleRow $ World <$> intValDec <*> intValDec
{-# INLINE selectSingle #-}



singleDb :: GenIO -> Session World
singleDb gen = do
  v <- liftIO $ uniformR (1, 10000) gen
  query v selectSingle
{-# INLINE singleDb #-}


-- * Test 3: Multiple database query

multipleDb :: Pool -> GenIO -> Maybe Int -> Handler [World]
multipleDb pool gen mcount = clean $ liftIO $ use pool $ replicateM cnt $ singleDb gen
  where
    cnt = let c = fromMaybe 1 mcount in max 1 (min c 500)
{-# INLINE multipleDb #-}


-- * Test 4: Fortunes

selectFortunes :: Hasql.Query () (Vec.Vector Fortune)
selectFortunes = Hasql.statement q encoder decoder True
  where
   q = "SELECT * FROM Fortune"
   encoder = HasqlEnc.unit
   -- TODO: investigate whether 'rowsList' is worth the more expensive 'cons'.
   decoder = HasqlDec.rowsVector $ Fortune <$> intValDec <*> HasqlDec.value HasqlDec.text
{-# INLINE selectFortunes #-}

fortunes :: Pool -> Handler (Html ())
fortunes pool = do
  fs <- clean $ liftIO $ use pool (query () selectFortunes)
  pure $ do
    let new = Fortune 0 "Additional fortune added at request time."
    doctypehtml_ $ do
      head_ $ title_ "Fortunes"
      body_ $
        table_ $ do
          tr_ $ do
            th_ "id"
            th_ "message"
          mapM_ (\f -> tr_ $ do
              td_ (toHtml . show $ fId f)
              td_ (toHtml $ fMessage f))
              (sortOn fMessage (new : Vec.toList fs))
{-# INLINE fortunes #-}


-- * Test 5: Updates

updateSingle :: Hasql.Query World World
updateSingle = Hasql.statement q encoder decoder True
  where
    q = "UPDATE World SET randomNumber = $1 WHERE id = $2 RETURNING id, randomNumber"
    encoder = contramap wId intValEnc <> contramap wRandomNumber intValEnc
    decoder = HasqlDec.singleRow $ World <$> intValDec <*> intValDec
{-# INLINE updateSingle #-}

-- Once Postgres > 9.3 is used in the benchmark, because unnest(<array>,<array>) is necesary with
-- hasql
-- updateAll :: Hasql.Query (Vec.Vector World) (Vec.Vector World)
-- updateAll = Hasql.statement q encoder decoder True
--   where
--     q = "UPDATE World SET randomNumber = rand FROM (SELECT newid, rand FROM unnest($1,$2)) AS sub WHERE id = newid"
--     encoder = contramap (fmap wId) (vector HasqlEnc.int4) <> contramap (fmap wRandomNumber) (vector HasqlEnc.int4)
--     decoder = HasqlDec.rowsVector $ World <$> intValDec <*> intValDec
--     vector value =
--              HasqlEnc.value (HasqlEnc.array (HasqlEnc.arrayDimension Vec.foldl' (HasqlEnc.arrayValue value)))
-- {-# INLINE updateAll #-}

updates :: Pool -> GenIO -> Maybe Int -> Handler [World]
updates pool gen mcount = clean $ liftIO $ use pool $ do
    rs <- replicateM cnt $ do
      res <- singleDb gen
      v <- liftIO $ uniformR (1, 10000) gen
      -- _ <- query (wId res, v) updateSingle
      pure $ res { wRandomNumber = v }
    Vec.forM_ (Vec.fromList rs) $ \v -> query v updateSingle
    --     query (Vec.fromList rs) updateAll -- For postgres > 9.3
    pure rs
  where
    cnt = let c = fromMaybe 1 mcount in max 1 (min c 500)
{-# INLINE updates #-}

-- * Test 6: Plaintext endpoint

plaintext :: Handler ByteString
plaintext = return "Hello, World!"
{-# INLINE plaintext #-}
