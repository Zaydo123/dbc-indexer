-- Enable compression for hypertables and add policies
ALTER TABLE trades SET (timescaledb.compress, timescaledb.compress_segmentby = 'market_id', timescaledb.compress_orderby = 'time DESC');
SELECT add_compression_policy('trades', INTERVAL '3 days');

ALTER TABLE transactions SET (timescaledb.compress, timescaledb.compress_orderby = 'time DESC');
SELECT add_compression_policy('transactions', INTERVAL '7 days');
