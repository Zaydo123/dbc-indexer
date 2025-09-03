from solana.rpc.async_api import AsyncClient
from solders.transaction import Transaction
from solders.signature import Signature
from solders.pubkey import Pubkey
from dotenv import load_dotenv
import os
import json
import asyncio
import sys
from pathlib import Path

load_dotenv()

import threading
import time
import logging
from typing import Optional, List

logging.basicConfig(
	level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
	format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("retroactive_indexer")
logging.getLogger('httpx').setLevel(logging.WARNING)
logger.setLevel(logging.INFO)

rpc_client = AsyncClient(
	endpoint=os.getenv("RPC_URL"),
	commitment="confirmed"
)

# Ensure project root is on sys.path to import common/
PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if PROJECT_ROOT not in sys.path:
	sys.path.append(PROJECT_ROOT)

from common import from_rpc, parse_swap_normalized  # type: ignore
from ingestion.db import get_db_writer, TradeRow  # type: ignore

async def fetch_all_tx_for_mint_between(mint: str, start_ts: Optional[int] = None, end_ts: Optional[int] = None) -> List[dict]:
	"""Fetch signatures for address, filtered to [start_ts, end_ts] inclusive.
	Pagination uses 'before' with last signature until we pass start_ts.
	"""
	results: List[dict] = []
	before_sig: Optional[Signature] = None
	while True:
		rq = await rpc_client.get_signatures_for_address(
			Pubkey.from_string(mint),
			limit=1000,
			before=before_sig,
		)
		page = json.loads(rq.to_json()).get("result", [])
		if not page:
			break
		logger.info(f"Fetched {len(page)} signatures for {mint}")
		# Filter by time
		for entry in page:
			bt = entry.get("blockTime")
			if bt is None:
				continue  # skip if unknown time
			if end_ts is not None and bt > end_ts:
				# newer than window: skip
				continue
			if start_ts is not None and bt < start_ts:
				# We've paged past the window start; stop outer loop
				page = None
				break
			results.append(entry)
		if page is None or len(page) < 1000:
			break
		before_sig = Signature.from_string(page[-1].get("signature"))
	return results

async def fetch_tx_by_sig(sig: str):
	tx = await rpc_client.get_transaction(
		Signature.from_string(sig),
		encoding="jsonParsed",
		max_supported_transaction_version=0,
		commitment="confirmed",
	)
	return tx

async def main(mint: str, start_ts: Optional[int] = None, end_ts: Optional[int] = None):
	logger.info("Starting retroactive indexer...")
	txids = await fetch_all_tx_for_mint_between(mint, start_ts, end_ts)
	logger.info(f"Fetched {len(txids)} signatures for mint") 
	# Process in parallel with bounded concurrency
	sem = asyncio.Semaphore(int(os.getenv("CONCURRENCY", "16")))
	total = len(txids)
	counters = {
		"no_sig": 0,
		"no_tx": 0,
		"not_parsed": 0,
		"parsed": 0,
		"errors": 0,
		# diagnostics for not_parsed
		"no_dbc": 0,
		"dbc_present_parse_fail": 0,
		"no_balances": 0,
	}

	DBC_PROGRAM_ID = "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN"

	def _has_balances(ntx):
		meta = getattr(ntx, "meta", None)
		if not meta:
			return False
		return bool(getattr(meta, "pre_token_balances", None)) or bool(getattr(meta, "post_token_balances", None))

	def _dbc_present(ntx):
		try:
			msg = getattr(ntx.message, "raw", {}) or {}
			ak = msg.get("accountKeys", []) if isinstance(msg, dict) else []
			# top-level
			for ix in (msg.get("instructions", []) if isinstance(msg, dict) else []):
				pid = ix.get("programId")
				if isinstance(pid, str) and pid == DBC_PROGRAM_ID:
					return True
				pid_idx = ix.get("programIdIndex")
				if isinstance(pid_idx, int) and 0 <= pid_idx < len(ak) and ak[pid_idx] == DBC_PROGRAM_ID:
					return True
			# inner
			inner = getattr(ntx.meta, "inner_instructions", None)
			if isinstance(inner, list):
				for g in inner:
					ins = g.get("instructions") if isinstance(g, dict) else None
					if isinstance(ins, list):
						for ix in ins:
							pid = ix.get("programId")
							if isinstance(pid, str) and pid == DBC_PROGRAM_ID:
								return True
			return False
		except Exception:
			return False

	writer = await get_db_writer()

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
					pass
		return out

	async def process(entry):
		sig = entry.get("signature")
		if not sig:
			counters["no_sig"] += 1
			return
		async with sem:
			try:
				tx = await fetch_tx_by_sig(sig)
				ntx = from_rpc(tx)
				# print(sig)
				if not ntx:
					counters["no_tx"] += 1
					return
				parsed = parse_swap_normalized(Pubkey.from_string(mint), ntx)
				if parsed:
					counters["parsed"] += 1
					# Enqueue to DB
					try:
						base_mint = parsed.get("base_mint")
						quote_mint = parsed.get("quote_mint")
						price = parsed.get("price")
						d_base = parsed.get("delta_base")
						d_quote = parsed.get("delta_quote")
						if base_mint and quote_mint and price is not None and d_base is not None and d_quote is not None:
							ts = parsed.get("received_at") or entry.get("blockTime")
							trow = TradeRow(
								time=float(ts or 0),
								base_mint=str(base_mint),
								quote_mint=str(quote_mint),
								side=int(parsed.get("side")) if parsed.get("side") is not None else None,
								price=float(price),
								amount_base=abs(float(d_base)),
								amount_quote=abs(float(d_quote)),
								tx_sig=str(parsed.get("signature")),
								slot=int(parsed.get("slot_landed") or 0),
								meta={
									"preTokenBalances": _serialize_tb_list(ntx.meta.pre_token_balances),
									"postTokenBalances": _serialize_tb_list(ntx.meta.post_token_balances),
								},
							)
							await writer.enqueue_trade(trow)
					except Exception as _:
						pass
				else:
					counters["not_parsed"] += 1
					if not _has_balances(ntx):
						counters["no_balances"] += 1
					elif not _dbc_present(ntx):
						counters["no_dbc"] += 1
					else:
						counters["dbc_present_parse_fail"] += 1
			except Exception as e:
				counters["errors"] += 1
				logger.debug("parse error for %s: %s", sig, e)

	# Kick off tasks and report progress periodically
	tasks = [asyncio.create_task(process(entry)) for entry in txids]
	last_report = 0
	for i, t in enumerate(asyncio.as_completed(tasks), 1):
		await t
		if i - last_report >= 200:
			last_report = i
			logger.info(
				f"Progress: {i}/{total} | parsed={counters['parsed']} not_parsed={counters['not_parsed']} no_tx={counters['no_tx']} errors={counters['errors']}"
			)

	logger.info(
		f"Done: {total} txs | parsed={counters['parsed']} not_parsed={counters['not_parsed']} no_tx={counters['no_tx']} no_sig={counters['no_sig']} errors={counters['errors']}"
	)
	logger.info(
		f"Reasons: no_dbc={counters['no_dbc']} dbc_present_parse_fail={counters['dbc_present_parse_fail']} no_balances={counters['no_balances']}"
	)
	return counters

if __name__ == "__main__":
	# Optional time window via environment variables (unix seconds)
	start_ts_env = os.getenv("START_TS")
	end_ts_env = os.getenv("END_TS")
	start_ts = int(start_ts_env) if start_ts_env else None
	end_ts = int(end_ts_env) if end_ts_env else None
	asyncio.run(main("6koymnMWNS7WLeVv4yo1kLRXEQ2ZFAsW2YjgMjtwcSFZ", start_ts=start_ts, end_ts=end_ts))