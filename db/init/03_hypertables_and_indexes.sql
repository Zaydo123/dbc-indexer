-- Convert base tables to hypertables and add indexes
-- Transactions hypertable
SELECT create_hypertable('transactions', 'time', if_not_exists => TRUE);

-- Trades hypertable with space partitioning by market_id
SELECT create_hypertable(
  'trades',
  'time',
  chunk_time_interval => INTERVAL '1 day',
  partitioning_column => 'market_id',
  number_partitions => 32,
  if_not_exists => TRUE
);
-- Fallback btree index to aid pruning

-- Indexes to speed up queries
CREATE INDEX IF NOT EXISTS idx_trades_market_time ON trades (market_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_time ON trades (time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_tx ON trades (tx_sig);

CREATE INDEX IF NOT EXISTS idx_tx_time ON transactions (time DESC);
CREATE INDEX IF NOT EXISTS idx_tx_slot ON transactions (slot);
