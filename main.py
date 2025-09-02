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

import base58
import grpc
from solders.pubkey import Pubkey
from solders.signature import Signature

from anchorpy import Idl, InstructionCoder

sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))
from generated import geyser_pb2_grpc
from generated import geyser_pb2
from grpc import aio

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
	if not update.transaction:
		return None
	else:
		slot_landed = update.transaction.slot
		try:
			tx_sig = Signature.from_bytes(update.transaction.transaction.signature)
		except Exception:
			return None
		account_keys = update.transaction.transaction.transaction.message.account_keys
		# Fee payer is the first signer; in Solana messages it's the first account key.
		fee_payer_pk = str(Pubkey.from_bytes(account_keys[0])) if account_keys else None
		meta = update.transaction.transaction.meta

		# Convert update.created_at (protobuf Timestamp) to RFC3339
		received_at = None
		try:
			if hasattr(update, "created_at") and update.created_at:
				dt = update.created_at.ToDatetime(tzinfo=datetime.timezone.utc)
				received_at = dt.timestamp()
		except Exception:
			pass

		# Build lookup for token balances by account_index
		pre_tb = {tb.account_index: tb for tb in getattr(meta, "pre_token_balances", [])}
		post_tb = {tb.account_index: tb for tb in getattr(meta, "post_token_balances", [])}

		def _balance_for_idx(idx: int):
			pre = pre_tb.get(idx)
			post = post_tb.get(idx)
			return pre, post

		def _ui_amount(tb) -> Decimal:
			return _token_amount_to_decimal(tb.ui_token_amount) if tb else Decimal(0)

		for instruction in update.transaction.transaction.transaction.message.instructions:
			if Pubkey.from_bytes(account_keys[instruction.program_id_index]) != DBC_PROGRAM_PK:
				continue
			parsed_ix = instruction_coder.parse(instruction.data)
			ix_name = parsed_ix.name
			if ix_name not in ("swap", "swap2"):
				continue

			# Map instruction account indices -> names using IDL order
			acc_indices = list(instruction.accounts)
			order = SWAP_ACCOUNTS_ORDER if ix_name == "swap" else SWAP2_ACCOUNTS_ORDER
			name_to_index = {}
			for i, name in enumerate(order):
				if i < len(acc_indices):
					name_to_index[name] = acc_indices[i]

			input_idx = name_to_index.get("input_token_account")
			output_idx = name_to_index.get("output_token_account")
			base_mint_idx = name_to_index.get("base_mint")
			quote_mint_idx = name_to_index.get("quote_mint")
			payer_idx = name_to_index.get("payer")
			payer_pk = (
				str(Pubkey.from_bytes(account_keys[payer_idx]))
				if (payer_idx is not None and payer_idx < len(account_keys))
				else None
			)

			# Resolve the tracked mint as str for comparisons against balance entries
			tracked_mint_str = str(mint)

			def _delta_for_account(idx: int) -> Decimal:
				pre, post = _balance_for_idx(idx)
				if not pre and not post:
					return Decimal(0)
				# Only consider entries matching the tracked mint if present
				pre_amt = _ui_amount(pre) if (pre and pre.mint == tracked_mint_str) else Decimal(0)
				post_amt = _ui_amount(post) if (post and post.mint == tracked_mint_str) else Decimal(0)
				return post_amt - pre_amt

			direction = None
			amt_change: Decimal = Decimal(0)
			if input_idx is not None:
				amt_change += _delta_for_account(input_idx)
			if output_idx is not None:
				amt_change += _delta_for_account(output_idx)

			if amt_change > 0:
				direction = "BUY"
			elif amt_change < 0:
				direction = "SELL"

			# Determine decimals for the tracked mint if present in token balances
			tracked_decimals = None
			for tb in list(pre_tb.values()) + list(post_tb.values()):
				if tb.mint == tracked_mint_str and hasattr(tb, "ui_token_amount"):
					uta = tb.ui_token_amount
					if hasattr(uta, "decimals"):
						tracked_decimals = int(uta.decimals)
						break

			try:
				params = parsed_ix.data.params
			except Exception:
				params = parsed_ix.data

			return Swap(
				signature=str(tx_sig),
				ix_name=ix_name,
				slot_landed=slot_landed,
				received_at=received_at,
				fee_payer=fee_payer_pk,
				payer=payer_pk,
				base_mint=str(Pubkey.from_bytes(account_keys[base_mint_idx])),
				quote_mint=str(Pubkey.from_bytes(account_keys[quote_mint_idx])),
				amount_in=getattr(params, "amount_in", getattr(params, "amount_0", None)),
				min_out_or_amount1=getattr(params, "minimum_amount_out", getattr(params, "amount_1", None)),
				swap_mode=getattr(params, "swap_mode", None),
				direction_for_tracked_mint=direction,
				tracked_mint_decimals=tracked_decimals,
				delta_tracked=float(amt_change),
			)

async def listen(endpoint: str, mint: str, output: str, logger: logging.Logger, stop: asyncio.Event):
	counter = 0
	start_time = time.time()
	logger.info("Listening for transactions... endpoint=%s mint=%s", endpoint, mint)
	mint_pk = Pubkey.from_string(mint)

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
				logger.info(parsed.__dict__)

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
