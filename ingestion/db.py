import asyncio
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

import asyncpg

DEFAULT_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/grpc")


@dataclass
class TradeRow:
    time: float  # unix seconds
    base_mint: str
    quote_mint: str
    side: Optional[int]  # 1=buy, -1=sell
    price: Optional[float]
    amount_base: Optional[float]
    amount_quote: Optional[float]
    tx_sig: str
    slot: int
    meta: Dict[str, Any]


class DBWriter:
    def __init__(
        self,
        dsn: str | None = None,
        *,
        queue_maxsize: int = 20000,
        batch_size: int = 200,
        flush_interval_sec: float = 0.5,
        pool_min_size: int = 1,
        pool_max_size: int = 8,
    ) -> None:
        self._dsn = dsn or DEFAULT_DB_URL
        self._queue: asyncio.Queue[Tuple[str, Any]] = asyncio.Queue(maxsize=queue_maxsize)
        self._pool: Optional[asyncpg.Pool] = None
        self._task: Optional[asyncio.Task] = None
        self._batch_size = batch_size
        self._flush_interval = flush_interval_sec
        self._tokens_seen: set[str] = set()
        self._market_cache: dict[Tuple[str, str], int] = {}
        self._pool_min = pool_min_size
        self._pool_max = pool_max_size

    async def start(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn, min_size=self._pool_min, max_size=self._pool_max)
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        # drain remaining
        await self._flush_remaining()
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def _flush_remaining(self) -> None:
        items: list[Tuple[str, Any]] = []
        while not self._queue.empty():
            try:
                items.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if items:
            await self._write_batch(items)

    async def enqueue_trade(self, row: TradeRow) -> None:
        await self._queue.put(("trade", row))

    async def _run(self) -> None:
        try:
            while True:
                items: list[Tuple[str, Any]] = []
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=self._flush_interval)
                    items.append(item)
                except asyncio.TimeoutError:
                    pass
                # Pull more up to batch size
                while len(items) < self._batch_size:
                    try:
                        items.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                if items:
                    await self._write_batch(items)
        except asyncio.CancelledError:
            return

    async def _write_batch(self, items: list[Tuple[str, Any]]) -> None:
        trades: list[TradeRow] = [it[1] for it in items if it[0] == "trade"]
        if not trades:
            return
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Ensure tokens and markets then insert trades
                for tr in trades:
                    await self._ensure_token(conn, tr.base_mint, self._decimals_from_meta(tr.meta, tr.base_mint))
                    await self._ensure_token(conn, tr.quote_mint, self._decimals_from_meta(tr.meta, tr.quote_mint))
                    market_id = await self._get_or_create_market(conn, tr.base_mint, tr.quote_mint)
                    await conn.execute(
                        """
                        INSERT INTO trades(time, market_id, base_mint, quote_mint, side, price, amount_base, amount_quote, tx_sig, slot, meta)
                        VALUES (to_timestamp($1), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT (time, market_id, tx_sig) DO NOTHING
                        """,
                        tr.time,
                        market_id,
                        tr.base_mint,
                        tr.quote_mint,
                        tr.side,
                        tr.price,
                        tr.amount_base,
                        tr.amount_quote,
                        tr.tx_sig,
                        tr.slot,
                        tr.meta,
                    )

    async def _ensure_token(self, conn: asyncpg.Connection, mint: str, decimals: Optional[int]) -> None:
        if mint in self._tokens_seen:
            return
        # decimals is required by schema; default to 0 if unknown
        d = int(decimals) if decimals is not None else 0
        await conn.execute(
            """
            INSERT INTO tokens(mint, decimals)
            VALUES ($1, $2)
            ON CONFLICT (mint) DO NOTHING
            """,
            mint,
            d,
        )
        self._tokens_seen.add(mint)

    async def _get_or_create_market(self, conn: asyncpg.Connection, base: str, quote: str) -> int:
        key = (base, quote)
        if key in self._market_cache:
            return self._market_cache[key]
        # Try select
        row = await conn.fetchrow("SELECT id FROM markets WHERE base_mint=$1 AND quote_mint=$2", base, quote)
        if row:
            self._market_cache[key] = int(row[0])
            return int(row[0])
        symbol = f"{base[:4]}/{quote[:4]}"
        row = await conn.fetchrow(
            """
            INSERT INTO markets(base_mint, quote_mint, symbol)
            VALUES ($1, $2, $3)
            ON CONFLICT (base_mint, quote_mint) DO UPDATE SET symbol=EXCLUDED.symbol
            RETURNING id
            """,
            base,
            quote,
            symbol,
        )
        mid = int(row[0])
        self._market_cache[key] = mid
        return mid

    @staticmethod
    def _decimals_from_meta(meta: Dict[str, Any], mint: str) -> Optional[int]:
        try:
            pre = meta.get("preTokenBalances") or []
            post = meta.get("postTokenBalances") or []
            for seq in (pre, post):
                for tb in seq:
                    # RPC dict style
                    if isinstance(tb, dict) and tb.get("mint") == mint:
                        uta = tb.get("uiTokenAmount")
                        if isinstance(uta, dict) and "decimals" in uta:
                            return int(uta["decimals"])  # type: ignore
                    else:
                        # Geyser object style
                        try:
                            tb_mint = getattr(tb, "mint", None)
                            if tb_mint == mint:
                                uta = getattr(tb, "ui_token_amount", None)
                                if uta is not None and hasattr(uta, "decimals"):
                                    return int(getattr(uta, "decimals"))
                        except Exception:
                            pass
        except Exception:
            return None
        return None


_db_writer_singleton: Optional[DBWriter] = None


async def get_db_writer() -> DBWriter:
    global _db_writer_singleton
    if _db_writer_singleton is None:
        _db_writer_singleton = DBWriter()
        await _db_writer_singleton.start()
    return _db_writer_singleton
