import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from typing import Optional
from decimal import Decimal
import datetime
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv()

import grpc
from solders.pubkey import Pubkey
from solders.signature import Signature

from anchorpy import Idl, InstructionCoder

sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))
# Ensure project root is importable to reach sibling package `common/`
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from common import from_geyser, parse_swap_normalized  # type: ignore
from generated import geyser_pb2_grpc
from generated import geyser_pb2
from grpc import aio
from ingestion.db import get_db_writer, TradeRow  # type: ignore

@dataclass
class Swap:
	signature: str
	ix_name: str
	slot_landed: int
	received_at: float
	fee_payer: str
	base_mint: str
	quote_mint: str
	payer: str
	amount_in: float
	min_out_or_amount1: float
	swap_mode: str
	direction_for_tracked_mint: str
	tracked_mint_decimals: int
	delta_tracked: float
	
idl = None
with open("idls/dbc.json", "r") as f:
	idl_raw = f.read()
idl = Idl.from_json(idl_raw)
instruction_coder = InstructionCoder(idl)

# Precompute IDL account order for swap instructions
def _get_idl_accounts_order(ix_name: str) -> list[str]:
	for ix in idl.instructions:
		if ix.name == ix_name:
			return [a.name for a in ix.accounts]
	return []

SWAP_ACCOUNTS_ORDER = _get_idl_accounts_order("swap")
SWAP2_ACCOUNTS_ORDER = _get_idl_accounts_order("swap2")

DBC_PROGRAM_PK = Pubkey.from_string("dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN")
DEFAULT_ENDPOINT = os.getenv("GRPC_ENDPOINT")
assert DEFAULT_ENDPOINT is not None, "GRPC_ENDPOINT environment variable is not set"

if "://" in DEFAULT_ENDPOINT:
	DEFAULT_ENDPOINT = DEFAULT_ENDPOINT.split("://")[1]

def _token_amount_to_decimal(ui_token_amount) -> Decimal:
	"""Return human-unit amount as Decimal.
	Prefers ui_amount_string if available; otherwise uses amount / 10**decimals.
	"""
	try:
		# ui_amount_string keeps precision as string if present
		if hasattr(ui_token_amount, "ui_amount_string") and ui_token_amount.ui_amount_string:
			return Decimal(str(ui_token_amount.ui_amount_string))
		# Fallback to amount/decimals
		amt = getattr(ui_token_amount, "amount", None)
		dec = getattr(ui_token_amount, "decimals", None)
		if amt is not None and dec is not None:
			return (Decimal(int(amt)) / (Decimal(10) ** int(dec)))
	except Exception:
		pass
	return Decimal(0)

def create_subscription_request(mint: str, failed: bool, commitment: str):
	request = geyser_pb2.SubscribeRequest()
	request.transactions["dbcfilter"].account_include.append(mint)
	request.transactions["dbcfilter"].failed = failed
	# Map commitment string to enum
	commit_map = {
		"processed": geyser_pb2.CommitmentLevel.PROCESSED,
		"confirmed": geyser_pb2.CommitmentLevel.CONFIRMED,
		"finalized": geyser_pb2.CommitmentLevel.FINALIZED,
	}
	request.commitment = commit_map.get(commitment.lower(), geyser_pb2.CommitmentLevel.PROCESSED)
	return request

async def parse_swap(mint: Pubkey, update: geyser_pb2.SubscribeUpdate):
    ntx = from_geyser(update)
    if not ntx:
        return None
    return parse_swap_normalized(mint, ntx)

def _serialize_tb_list(seq):
    out = []
    if not seq:
        return out
    for tb in seq:
        if isinstance(tb, dict):
            out.append(tb)
        else:
            try:
                uta = getattr(tb, "ui_token_amount", None)
                out.append({
                    "accountIndex": getattr(tb, "account_index", None),
                    "mint": getattr(tb, "mint", None),
                    "owner": getattr(tb, "owner", None),
                    "uiTokenAmount": {
                        "uiAmountString": str(getattr(uta, "ui_amount_string", None)) if getattr(uta, "ui_amount_string", None) is not None else None,
                        "amount": getattr(uta, "amount", None),
                        "decimals": getattr(uta, "decimals", None),
                    } if uta is not None else None,
                })
            except Exception:
                # Best-effort: skip malformed entries
                pass
    return out

async def listen(endpoint: str, mint: str, output: str, logger: logging.Logger, stop: asyncio.Event):
	counter = 0
	start_time = time.time()
	logger.info("Listening for transactions... endpoint=%s mint=%s", endpoint, mint)
	mint_pk = Pubkey.from_string(mint)
	writer = await get_db_writer()

	async with aio.insecure_channel(endpoint) as channel:
		stub = geyser_pb2_grpc.GeyserStub(channel)
		request = create_subscription_request(mint, failed=False, commitment="processed")

		async def request_stream():
			yield request
			while not stop.is_set():
				await asyncio.sleep(3600)
			return

		call = stub.Subscribe(request_stream())
		async for update in call:
			if stop.is_set():
				break
			counter += 1
			parsed = await parse_swap(mint_pk, update)
			if parsed:
				logger.info(json.dumps(parsed, indent=4))
				# Enqueue to DB ingestion
				try:
					ntx = from_geyser(update)
					if ntx:
						base_mint = parsed.get("base_mint")
						quote_mint = parsed.get("quote_mint")
						price = parsed.get("price")
						d_base = parsed.get("delta_base")
						d_quote = parsed.get("delta_quote")
						# Enforce NOT NULL constraints for price/amounts
						if base_mint and quote_mint and price is not None and d_base is not None and d_quote is not None:
							ts = parsed.get("received_at")
							if not ts and getattr(ntx, "received_at", None):
								try:
									ts = int(ntx.received_at.timestamp())
								except Exception:
									ts = None
							ts = float(ts or time.time())
							trow = TradeRow(
								time=ts,
								base_mint=str(base_mint),
								quote_mint=str(quote_mint),
								side=int(parsed.get("side")) if parsed.get("side") is not None else None,
								price=float(price),
								amount_base=abs(float(d_base)),
								amount_quote=abs(float(d_quote)),
								tx_sig=str(parsed.get("signature")),
								slot=int(parsed.get("slot_landed") or 0),
								meta={
									"preTokenBalances": _serialize_tb_list(getattr(ntx.meta, "pre_token_balances", None)),
									"postTokenBalances": _serialize_tb_list(getattr(ntx.meta, "post_token_balances", None)),
								},
							)
							await writer.enqueue_trade(trow)
				except Exception as e:
					logger.debug("ingestion enqueue error: %s", e)

async def _run_with_retries(args):
	logger = logging.getLogger("swap_parser")
	stop_event = asyncio.Event()

	# Graceful shutdown
	def _handle_signal():
		logger.info("Shutdown signal received, stopping...")
		stop_event.set()

	loop = asyncio.get_running_loop()
	for sig in (signal.SIGINT, signal.SIGTERM):
		try:
			loop.add_signal_handler(sig, _handle_signal)
		except NotImplementedError:
			# Windows or restricted env
			pass

	backoff = 1.0
	while not stop_event.is_set():
		try:
			await listen(
				endpoint=args.endpoint,
				mint=args.mint,
				output=args.output,
				logger=logger,
				stop=stop_event,
			)
			# normal exit
			break
		except (grpc.RpcError, aio.AioRpcError) as e:
			logger.warning("Stream error: %s; reconnecting in %.1fs", e, backoff)
			try:
				await asyncio.wait_for(stop_event.wait(), timeout=backoff)
			except asyncio.TimeoutError:
				pass
			backoff = min(backoff * 2, 30.0)


def _build_arg_parser():
	parser = argparse.ArgumentParser(description="Real-time swap parser (quote-agnostic)")
	parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="gRPC endpoint host:port")
	parser.add_argument("--mint", required=True, help="Mint to track (e.g., JUP mint)")
	parser.add_argument("--output", choices=["text", "json"], default="text", help="Output mode")
	parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
	return parser


def _configure_logging(level: str):
	logging.basicConfig(
		level=getattr(logging, level.upper(), logging.INFO),
		format="%(asctime)s %(levelname)s %(message)s",
	)

def main():
	parser = _build_arg_parser()
	args = parser.parse_args()
	_configure_logging(args.log_level)
	asyncio.run(_run_with_retries(args))


if __name__ == '__main__':
	main()
