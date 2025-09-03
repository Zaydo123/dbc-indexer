-- Ensure compression options and policies exist (idempotent)
-- Trades table compression settings
DO $$
BEGIN
  EXECUTE 'ALTER TABLE trades SET (timescaledb.compress)';
  EXECUTE 'ALTER TABLE trades SET (timescaledb.compress_segmentby = ''market_id'')';
  EXECUTE 'ALTER TABLE trades SET (timescaledb.compress_orderby = ''time DESC'')';
EXCEPTION WHEN others THEN
  -- ignore if already set or table missing during initial runs
  NULL;
END$$;

-- Add compression policy for trades if missing
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM timescaledb_information.jobs j
    WHERE j.proc_name = 'policy_compression' AND j.hypertable_name = 'trades'
  ) THEN
    PERFORM add_compression_policy('trades', INTERVAL '3 days');
  END IF;
END$$;

-- Transactions table compression
DO $$
BEGIN
  EXECUTE 'ALTER TABLE transactions SET (timescaledb.compress)';
  EXECUTE 'ALTER TABLE transactions SET (timescaledb.compress_orderby = ''time DESC'')';
EXCEPTION WHEN others THEN
  NULL;
END$$;

-- Add compression policy for transactions if missing
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM timescaledb_information.jobs j
    WHERE j.proc_name = 'policy_compression' AND j.hypertable_name = 'transactions'
  ) THEN
    PERFORM add_compression_policy('transactions', INTERVAL '7 days');
  END IF;
END$$;
