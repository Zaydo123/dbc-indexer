-- Continuous aggregates for OHLCV across multiple intervals
-- Helper: creates a CAgg for a given bucket
-- Note: We use array_agg ordered to compute open/close without toolkit

-- 1s
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_trades_1s
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 second', time) AS bucket,
  market_id,
  (array_agg(price ORDER BY time ASC))[1]  AS open,
  (array_agg(price ORDER BY time DESC))[1] AS close,
  max(price) AS high,
  min(price) AS low,
  sum(amount_base)  AS volume_base,
  sum(amount_quote) AS volume_quote,
  count(*) AS trades
FROM trades
GROUP BY bucket, market_id
WITH NO DATA;
CREATE INDEX IF NOT EXISTS idx_cagg_1s_market_bucket ON cagg_trades_1s (market_id, bucket);
SELECT add_continuous_aggregate_policy('cagg_trades_1s', start_offset => INTERVAL '2 days', end_offset => INTERVAL '30 seconds', schedule_interval => INTERVAL '30 seconds');

-- 5s
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_trades_5s
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('5 seconds', time) AS bucket,
  market_id,
  (array_agg(price ORDER BY time ASC))[1]  AS open,
  (array_agg(price ORDER BY time DESC))[1] AS close,
  max(price) AS high,
  min(price) AS low,
  sum(amount_base)  AS volume_base,
  sum(amount_quote) AS volume_quote,
  count(*) AS trades
FROM trades
GROUP BY bucket, market_id
WITH NO DATA;
CREATE INDEX IF NOT EXISTS idx_cagg_5s_market_bucket ON cagg_trades_5s (market_id, bucket);
SELECT add_continuous_aggregate_policy('cagg_trades_5s', start_offset => INTERVAL '7 days', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 minute');

-- 15s
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_trades_15s
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('15 seconds', time) AS bucket,
  market_id,
  (array_agg(price ORDER BY time ASC))[1]  AS open,
  (array_agg(price ORDER BY time DESC))[1] AS close,
  max(price) AS high,
  min(price) AS low,
  sum(amount_base)  AS volume_base,
  sum(amount_quote) AS volume_quote,
  count(*) AS trades
FROM trades
GROUP BY bucket, market_id
WITH NO DATA;
CREATE INDEX IF NOT EXISTS idx_cagg_15s_market_bucket ON cagg_trades_15s (market_id, bucket);
SELECT add_continuous_aggregate_policy('cagg_trades_15s', start_offset => INTERVAL '14 days', end_offset => INTERVAL '2 minutes', schedule_interval => INTERVAL '2 minutes');

-- 30s
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_trades_30s
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('30 seconds', time) AS bucket,
  market_id,
  (array_agg(price ORDER BY time ASC))[1]  AS open,
  (array_agg(price ORDER BY time DESC))[1] AS close,
  max(price) AS high,
  min(price) AS low,
  sum(amount_base)  AS volume_base,
  sum(amount_quote) AS volume_quote,
  count(*) AS trades
FROM trades
GROUP BY bucket, market_id
WITH NO DATA;
CREATE INDEX IF NOT EXISTS idx_cagg_30s_market_bucket ON cagg_trades_30s (market_id, bucket);
SELECT add_continuous_aggregate_policy('cagg_trades_30s', start_offset => INTERVAL '21 days', end_offset => INTERVAL '5 minutes', schedule_interval => INTERVAL '5 minutes');

-- 1m
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_trades_1m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 minute', time) AS bucket,
  market_id,
  (array_agg(price ORDER BY time ASC))[1]  AS open,
  (array_agg(price ORDER BY time DESC))[1] AS close,
  max(price) AS high,
  min(price) AS low,
  sum(amount_base)  AS volume_base,
  sum(amount_quote) AS volume_quote,
  count(*) AS trades
FROM trades
GROUP BY bucket, market_id
WITH NO DATA;
CREATE INDEX IF NOT EXISTS idx_cagg_1m_market_bucket ON cagg_trades_1m (market_id, bucket);
SELECT add_continuous_aggregate_policy('cagg_trades_1m', start_offset => INTERVAL '30 days', end_offset => INTERVAL '10 minutes', schedule_interval => INTERVAL '5 minutes');

-- 5m
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_trades_5m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('5 minutes', time) AS bucket,
  market_id,
  (array_agg(price ORDER BY time ASC))[1]  AS open,
  (array_agg(price ORDER BY time DESC))[1] AS close,
  max(price) AS high,
  min(price) AS low,
  sum(amount_base)  AS volume_base,
  sum(amount_quote) AS volume_quote,
  count(*) AS trades
FROM trades
GROUP BY bucket, market_id
WITH NO DATA;
CREATE INDEX IF NOT EXISTS idx_cagg_5m_market_bucket ON cagg_trades_5m (market_id, bucket);
SELECT add_continuous_aggregate_policy('cagg_trades_5m', start_offset => INTERVAL '90 days', end_offset => INTERVAL '30 minutes', schedule_interval => INTERVAL '15 minutes');

-- 15m
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_trades_15m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('15 minutes', time) AS bucket,
  market_id,
  (array_agg(price ORDER BY time ASC))[1]  AS open,
  (array_agg(price ORDER BY time DESC))[1] AS close,
  max(price) AS high,
  min(price) AS low,
  sum(amount_base)  AS volume_base,
  sum(amount_quote) AS volume_quote,
  count(*) AS trades
FROM trades
GROUP BY bucket, market_id
WITH NO DATA;
CREATE INDEX IF NOT EXISTS idx_cagg_15m_market_bucket ON cagg_trades_15m (market_id, bucket);
SELECT add_continuous_aggregate_policy('cagg_trades_15m', start_offset => INTERVAL '120 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '30 minutes');

-- 30m
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_trades_30m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('30 minutes', time) AS bucket,
  market_id,
  (array_agg(price ORDER BY time ASC))[1]  AS open,
  (array_agg(price ORDER BY time DESC))[1] AS close,
  max(price) AS high,
  min(price) AS low,
  sum(amount_base)  AS volume_base,
  sum(amount_quote) AS volume_quote,
  count(*) AS trades
FROM trades
GROUP BY bucket, market_id
WITH NO DATA;
CREATE INDEX IF NOT EXISTS idx_cagg_30m_market_bucket ON cagg_trades_30m (market_id, bucket);
SELECT add_continuous_aggregate_policy('cagg_trades_30m', start_offset => INTERVAL '180 days', end_offset => INTERVAL '2 hours', schedule_interval => INTERVAL '1 hour');

-- 1h
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_trades_1h
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS bucket,
  market_id,
  (array_agg(price ORDER BY time ASC))[1]  AS open,
  (array_agg(price ORDER BY time DESC))[1] AS close,
  max(price) AS high,
  min(price) AS low,
  sum(amount_base)  AS volume_base,
  sum(amount_quote) AS volume_quote,
  count(*) AS trades
FROM trades
GROUP BY bucket, market_id
WITH NO DATA;
CREATE INDEX IF NOT EXISTS idx_cagg_1h_market_bucket ON cagg_trades_1h (market_id, bucket);
SELECT add_continuous_aggregate_policy('cagg_trades_1h', start_offset => INTERVAL '365 days', end_offset => INTERVAL '6 hours', schedule_interval => INTERVAL '2 hours');

-- 1d
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_trades_1d
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', time) AS bucket,
  market_id,
  (array_agg(price ORDER BY time ASC))[1]  AS open,
  (array_agg(price ORDER BY time DESC))[1] AS close,
  max(price) AS high,
  min(price) AS low,
  sum(amount_base)  AS volume_base,
  sum(amount_quote) AS volume_quote,
  count(*) AS trades
FROM trades
GROUP BY bucket, market_id
WITH NO DATA;
CREATE INDEX IF NOT EXISTS idx_cagg_1d_market_bucket ON cagg_trades_1d (market_id, bucket);
SELECT add_continuous_aggregate_policy('cagg_trades_1d', start_offset => INTERVAL '1095 days', end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '12 hours');
