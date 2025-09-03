-- Base reference tables
CREATE TABLE IF NOT EXISTS tokens (
  mint                TEXT PRIMARY KEY,
  symbol              TEXT,
  decimals            SMALLINT NOT NULL,
  metadata            JSONB,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS markets (
  id                  BIGSERIAL PRIMARY KEY,
  base_mint           TEXT NOT NULL REFERENCES tokens(mint),
  quote_mint          TEXT NOT NULL REFERENCES tokens(mint),
  symbol              TEXT NOT NULL,
  metadata            JSONB,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (base_mint, quote_mint),
  UNIQUE (symbol)
);

-- Raw transactions we observe (optional but useful for retro indexer)
CREATE TABLE IF NOT EXISTS transactions (
  time                TIMESTAMPTZ NOT NULL,
  slot                BIGINT,
  tx_sig              TEXT PRIMARY KEY,
  succeeded           BOOLEAN,
  fee_lamports        BIGINT,
  program_id          TEXT,
  payer               TEXT,
  meta                JSONB
);

-- Trades (fills) extracted by grpc_ingestion
-- Use numeric for price and amounts to preserve precision
CREATE TABLE IF NOT EXISTS trades (
  time                TIMESTAMPTZ NOT NULL,
  market_id           BIGINT NOT NULL REFERENCES markets(id),
  base_mint           TEXT NOT NULL REFERENCES tokens(mint),
  quote_mint          TEXT NOT NULL REFERENCES tokens(mint),
  side                SMALLINT, -- 1=buy, -1=sell, null=unknown
  price               NUMERIC(38,18) NOT NULL,
  amount_base         NUMERIC(38,18) NOT NULL,
  amount_quote        NUMERIC(38,18) NOT NULL,
  tx_sig              TEXT,
  slot                BIGINT,
  meta                JSONB,
  PRIMARY KEY (time, market_id, tx_sig)
);
