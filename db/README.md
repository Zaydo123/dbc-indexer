# TimescaleDB schema

This sets up a TimescaleDB instance with hypertables for raw transactions and trades, plus continuous aggregates for OHLCV across multiple intervals.

- Init SQL lives in `db/init/` and is auto-applied by Docker on first start.
- Hypertables: `transactions`, `trades`.
- Reference tables: `tokens`, `markets`.
- CAggs: `cagg_trades_{1s,5s,15s,30s,1m,5m,15m,30m,1h,1d}`.
- Compression and policies included.

## Run
```
docker compose up -d timescaledb
```

Connect:
```
psql postgresql://postgres:postgres@localhost:5432/grpc
```

## Schema overview
- `tokens(mint PK, symbol, decimals, metadata jsonb)`
- `markets(id PK, base_mint FK, quote_mint FK, symbol unique, metadata jsonb)`
- `transactions(time, slot, tx_sig PK, succeeded, fee_lamports, program_id, payer, meta jsonb)`
- `trades(time, market_id FK, base_mint, quote_mint, side, price, amount_base, amount_quote, tx_sig, slot, meta jsonb)` hypertable

## Continuous Aggregates (OHLCV)
Each view groups by `time_bucket(INTERVAL, time)` and `market_id` and computes:
- open = first price by time
- close = last price by time
- high/low = max/min price
- volume_base, volume_quote = sums

Materialized views are created with `WITH NO DATA` initially. After TimescaleDB starts, run:
```
CALL refresh_continuous_aggregate('cagg_trades_1s', NULL, NULL);
CALL refresh_continuous_aggregate('cagg_trades_5s', NULL, NULL);
-- ... repeat for other CAggs as needed
```

## Example queries
- Latest 1m candle for market 42:
```
SELECT * FROM cagg_trades_1m
WHERE market_id = 42
ORDER BY bucket DESC
LIMIT 1;
```

- Insert a market + trade:
```
INSERT INTO tokens(mint, symbol, decimals) VALUES ('So11111111111111111111111111111111111111112','SOL',9)
ON CONFLICT (mint) DO NOTHING;
INSERT INTO tokens(mint, symbol, decimals) VALUES ('USDCMint','USDC',6)
ON CONFLICT (mint) DO NOTHING;
INSERT INTO markets(base_mint, quote_mint, symbol) VALUES ('So11111111111111111111111111111111111111112','USDCMint','SOL/USDC')
RETURNING id;
-- Suppose id=42
INSERT INTO trades(time, market_id, base_mint, quote_mint, side, price, amount_base, amount_quote, tx_sig)
VALUES (now(), 42, 'So11111111111111111111111111111111111111112','USDCMint', 1, 155.25, 0.5, 77.625, 'txsig1');
```

## Notes
- For even better open/close accuracy under high concurrency, consider using `timescaledb_toolkit` hyperfunctions. The current definitions use `array_agg ORDER BY` which is portable.
- Adjust compression/refresh horizons to your retention needs.
- The retroactive indexer can backfill `transactions` and `trades`; CAgg policies will incrementally materialize.
