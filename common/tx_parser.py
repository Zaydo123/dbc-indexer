from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence, List, Dict
from datetime import datetime, timezone
from decimal import Decimal

from solders.signature import Signature
from solders.pubkey import Pubkey
from pathlib import Path
import os
import base58

# anchorpy for IDL-based instruction decoding
try:
	from anchorpy import Idl, InstructionCoder
except Exception:  # optional dependency for environments not needing parsing
	Idl = None  # type: ignore
	InstructionCoder = None  # type: ignore

# Types normalized across Geyser SubscribeUpdate and RPC getTransaction

@dataclass
class NormalizedMeta:
	pre_token_balances: Any | None
	post_token_balances: Any | None
	log_messages: Optional[Sequence[str]]
	err: Any | None
	inner_instructions: Any | None
	# raw for callers who need extra fields
	raw: Any

@dataclass
class NormalizedMessage:
	account_keys: List[Pubkey]
	instructions: Any  # program instructions array in original form
	raw: Any

@dataclass
class NormalizedTx:
	slot: int
	signature: Signature
	message: NormalizedMessage
	meta: NormalizedMeta
	received_at: Optional[datetime]
	raw: Any

# ----------------- Adapters -----------------

def from_geyser(update: Any) -> Optional[NormalizedTx]:
	"""Adapt geyser_pb2.SubscribeUpdate to NormalizedTx.
	Returns None if the update doesn't carry a transaction.
	"""
	if not getattr(update, "transaction", None):
		return None

	txu = update.transaction
	# signature: bytes in protobuf field
	try:
		sig = Signature.from_bytes(txu.transaction.signature)
	except Exception:
		return None

	# account keys
	ak = getattr(txu.transaction.transaction.message, "account_keys", [])
	account_keys: List[Pubkey] = [Pubkey.from_bytes(b) for b in ak]

	# message & meta pass-through
	message = NormalizedMessage(
		account_keys=account_keys,
		instructions=getattr(txu.transaction.transaction.message, "instructions", None),
		raw=txu.transaction.transaction.message,
	)
	meta = NormalizedMeta(
		pre_token_balances=getattr(txu.transaction, "meta", None).pre_token_balances if getattr(txu.transaction, "meta", None) else None,
		post_token_balances=getattr(txu.transaction, "meta", None).post_token_balances if getattr(txu.transaction, "meta", None) else None,
		log_messages=(getattr(txu.transaction, "meta", None).log_messages if getattr(txu.transaction, "meta", None) else None),
		err=(getattr(txu.transaction, "meta", None).err if getattr(txu.transaction, "meta", None) else None),
		inner_instructions=(getattr(txu.transaction, "meta", None).inner_instructions if getattr(txu.transaction, "meta", None) else None),
		raw=getattr(txu, "transaction", None).meta if getattr(txu, "transaction", None) else None,
	)

	# received_at from created_at Timestamp if present
	received_at_dt: Optional[datetime] = None
	try:
		if hasattr(update, "created_at") and update.created_at:
			received_at_dt = update.created_at.ToDatetime(tzinfo=timezone.utc)
	except Exception:
		received_at_dt = None

	return NormalizedTx(
		slot=getattr(txu, "slot", 0),
		signature=sig,
		message=message,
		meta=meta,
		received_at=received_at_dt,
		raw=update,
	)


def from_rpc(tx: Any) -> Optional[NormalizedTx]:
	"""Adapt RPC getTransaction response (solders or json/dict) to NormalizedTx."""
	# Handle solders style: object with value or direct fields
	obj = tx
	try:
		# solders RpcResponse
		if hasattr(tx, "value") and tx.value is not None:
			obj = tx.value
		# sometimes .to_json()
		if hasattr(obj, "to_json"):
			obj = obj.to_json()
		if isinstance(obj, str):
			import json as _json
			obj = _json.loads(obj)
	except Exception:
		pass

	# Expect a dict-like with keys: transaction, meta, slot
	try:
		# Unwrap common JSON-RPC envelopes
		if isinstance(obj, dict) and "result" in obj:
			obj = obj.get("result")
		# Some clients wrap another layer as {"value": {...}}
		if isinstance(obj, dict) and "value" in obj and not obj.get("transaction"):
			obj = obj.get("value")
		if obj is None:
			return None
		slot = obj.get("slot") if isinstance(obj, dict) else getattr(obj, "slot", 0)
		if isinstance(slot, dict):  # some clients nest it
			slot = slot.get("slot", 0)
		tx_dict = obj["transaction"] if isinstance(obj, dict) else getattr(obj, "transaction")
		meta = obj.get("meta") if isinstance(obj, dict) else getattr(obj, "meta")
		sig_str = tx_dict.get("signatures", [None])[0] if isinstance(tx_dict, dict) else None
		sig = Signature.from_string(sig_str) if sig_str else None
		msg = tx_dict.get("message") if isinstance(tx_dict, dict) else getattr(tx_dict, "message")
		account_keys_raw = msg.get("accountKeys", []) if isinstance(msg, dict) else []
		account_keys: List[Pubkey] = []
		for k in account_keys_raw:
			if isinstance(k, str):
				account_keys.append(Pubkey.from_string(k))
			elif isinstance(k, dict):
				pk = k.get("pubkey")
				if isinstance(pk, str):
					account_keys.append(Pubkey.from_string(pk))
			else:
				try:
					account_keys.append(Pubkey.from_bytes(k))
				except Exception:
					pass
		message = NormalizedMessage(
			account_keys=account_keys,
			instructions=msg.get("instructions") if isinstance(msg, dict) else None,
			raw=msg,
		)
		nmeta = NormalizedMeta(
			pre_token_balances=(meta.get("preTokenBalances") if isinstance(meta, dict) else None),
			post_token_balances=(meta.get("postTokenBalances") if isinstance(meta, dict) else None),
			log_messages=(meta.get("logMessages") if isinstance(meta, dict) else None),
			err=(meta.get("err") if isinstance(meta, dict) else None),
			inner_instructions=(meta.get("innerInstructions") if isinstance(meta, dict) else None),
			raw=meta,
		)
		# Prefer RPC blockTime for received_at
		recv_at = None
		try:
			bt = obj.get("blockTime") if isinstance(obj, dict) else None
			if isinstance(bt, int) and bt > 0:
				recv_at = datetime.fromtimestamp(bt, tz=timezone.utc)
		except Exception:
			recv_at = None
		return NormalizedTx(
			slot=int(slot or 0),
			signature=sig if isinstance(sig, Signature) else Signature.from_string(tx_dict["signatures"][0]),
			message=message,
			meta=nmeta,
			received_at=recv_at,
			raw=obj,
		)
	except Exception:
		return None


# ----------------- Parsing entrypoint -----------------

###########################
# IDL and instruction coder
###########################

_INSTRUCTION_CODER: Any = None
_SWAP_ACCOUNTS_ORDER: List[str] = []
_SWAP2_ACCOUNTS_ORDER: List[str] = []
DBC_PROGRAM_PK = Pubkey.from_string("dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN")

def _load_coder():
	global _INSTRUCTION_CODER, _SWAP_ACCOUNTS_ORDER, _SWAP2_ACCOUNTS_ORDER
	if _INSTRUCTION_CODER is not None:
		return
	if Idl is None or InstructionCoder is None:
		return
	# Resolve IDL path relative to project root: ../grpc_index/idls/dbc.json
	here = Path(__file__).resolve()
	candidate = here.parent.parent / "grpc_index" / "idls" / "dbc.json"
	# Fallback: try cwd/grpc_index/idls/dbc.json
	if not candidate.exists():
		candidate = Path(os.getcwd()) / "grpc_index" / "idls" / "dbc.json"
	if not candidate.exists():
		return
	try:
		idl_raw = candidate.read_text()
		idl = Idl.from_json(idl_raw)
		_INSTRUCTION_CODER = InstructionCoder(idl)
		# Precompute account orders
		def _order(ix_name: str) -> List[str]:
			for ix in idl.instructions:
				if ix.name == ix_name:
					return [a.name for a in ix.accounts]
			return []
		_SWAP_ACCOUNTS_ORDER = _order("swap")
		_SWAP2_ACCOUNTS_ORDER = _order("swap2")
	except Exception:
		_INSTRUCTION_CODER = None
		_SWAP_ACCOUNTS_ORDER = []
		_SWAP2_ACCOUNTS_ORDER = []


def _token_amount_to_decimal(ui_token_amount) -> Decimal:
	"""Return human-unit amount as Decimal for both RPC dict and Geyser types."""
	try:
		# RPC dict style
		if isinstance(ui_token_amount, dict):
			if ui_token_amount.get("uiAmountString") is not None:
				return Decimal(str(ui_token_amount["uiAmountString"]))
			amt = ui_token_amount.get("amount")
			dec = ui_token_amount.get("decimals")
			if amt is not None and dec is not None:
				return Decimal(int(amt)) / (Decimal(10) ** int(dec))
		# Geyser object style
		if hasattr(ui_token_amount, "ui_amount_string") and ui_token_amount.ui_amount_string:
			return Decimal(str(ui_token_amount.ui_amount_string))
		amt = getattr(ui_token_amount, "amount", None)
		dec = getattr(ui_token_amount, "decimals", None)
		if amt is not None and dec is not None:
			return Decimal(int(amt)) / (Decimal(10) ** int(dec))
	except Exception:
		pass
	return Decimal(0)


def _balance_maps(meta: NormalizedMeta) -> tuple[Dict[int, Any], Dict[int, Any]]:
	"""Return maps account_index -> balance-entry for pre and post.
	Supports both RPC dict entries and Geyser objects.
	"""
	pre_tb_list = meta.pre_token_balances or []
	post_tb_list = meta.post_token_balances or []
	pre: Dict[int, Any] = {}
	post: Dict[int, Any] = {}
	for tb in pre_tb_list:
		idx = tb.get("accountIndex") if isinstance(tb, dict) else getattr(tb, "account_index", None)
		if idx is not None:
			pre[int(idx)] = tb
	for tb in post_tb_list:
		idx = tb.get("accountIndex") if isinstance(tb, dict) else getattr(tb, "account_index", None)
		if idx is not None:
			post[int(idx)] = tb
	return pre, post


def _entry_matches_mint(tb: Any, mint_str: str) -> bool:
	if tb is None:
		return False
	if isinstance(tb, dict):
		return tb.get("mint") == mint_str
	return getattr(tb, "mint", None) == mint_str


def _ui_amount_from_entry(tb: Any) -> Decimal:
	if tb is None:
		return Decimal(0)
	if isinstance(tb, dict):
		return _token_amount_to_decimal(tb.get("uiTokenAmount"))
	return _token_amount_to_decimal(getattr(tb, "ui_token_amount", None))


def _tracked_decimals(meta: NormalizedMeta, mint_str: str) -> Optional[int]:
	# Iterate pre and post separately to support protobuf repeated containers
	for seq in (meta.pre_token_balances or []), (meta.post_token_balances or []):
		for tb in seq:
			if _entry_matches_mint(tb, mint_str):
				# RPC dict
				if isinstance(tb, dict):
					uta = tb.get("uiTokenAmount")
					if isinstance(uta, dict) and "decimals" in uta:
						return int(uta["decimals"])  # type: ignore
				else:
					uta = getattr(tb, "ui_token_amount", None)
					if uta is not None and hasattr(uta, "decimals"):
						return int(uta.decimals)
	return None


def _decode_ix_data(data_field: Any) -> Optional[bytes]:
	# RPC dict: base58 string
	if isinstance(data_field, str):
		try:
			return base58.b58decode(data_field)
		except Exception:
			return None
	# Geyser: bytes
	if isinstance(data_field, (bytes, bytearray)):
		return bytes(data_field)
	# Some RPCs wrap as {"data": "..", "type": "base58"} or {"data": ["..", "base64"]}
	if isinstance(data_field, dict) and data_field.get("data") is not None:
		inner = data_field.get("data")
		if isinstance(inner, str):
			try:
				return base58.b58decode(inner)
			except Exception:
				pass
		if isinstance(inner, list) and len(inner) >= 1 and isinstance(inner[0], str):
			payload = inner[0]
			enc = inner[1] if len(inner) > 1 else None
			try:
				if enc == "base64":
					import base64 as _b64
					return _b64.b64decode(payload)
				# default assume base58
				return base58.b58decode(payload)
			except Exception:
				return None
	# Direct list [data, encoding]
	if isinstance(data_field, list) and len(data_field) >= 1 and isinstance(data_field[0], str):
		payload = data_field[0]
		enc = data_field[1] if len(data_field) > 1 else None
		try:
			if enc == "base64":
				import base64 as _b64
				return _b64.b64decode(payload)
			return base58.b58decode(payload)
		except Exception:
			return None
	return None


def parse_swap_normalized(mint: Pubkey, ntx: NormalizedTx) -> Optional[dict]:
	if ntx is None or ntx.message is None:
		return None

	_load_coder()
	coder = _INSTRUCTION_CODER
	if coder is None:
		# Without IDL coder we cannot parse instruction params; return minimal envelope
		return None

	account_keys = ntx.message.account_keys
	instructions = ntx.message.instructions or []
	# Build account key -> index map for resolving inner-instruction accounts if needed
	key_to_index: Dict[str, int] = {str(pk): i for i, pk in enumerate(account_keys)}
	mint_str = str(mint)

	# Build token balance maps
	pre_tb, post_tb = _balance_maps(ntx.meta)

	def _delta_for_account(idx: int) -> Decimal:
		pre = pre_tb.get(idx)
		post = post_tb.get(idx)
		if not pre and not post:
			return Decimal(0)
		pre_amt = _ui_amount_from_entry(pre) if _entry_matches_mint(pre, mint_str) else Decimal(0)
		post_amt = _ui_amount_from_entry(post) if _entry_matches_mint(post, mint_str) else Decimal(0)
		return post_amt - pre_amt

	# Helper to iterate both top-level and inner instructions
	all_instructions: List[Any] = []
	# top-level
	for ix in instructions:
		all_instructions.append(ix)
	# inner: RPC meta.innerInstructions is a list of {index, instructions}
	try:
		inner = getattr(ntx.meta, "inner_instructions", None)
		if isinstance(inner, list):
			for group in inner:
				ins = group.get("instructions") if isinstance(group, dict) else None
				if isinstance(ins, list):
					all_instructions.extend(ins)
	except Exception:
		pass

	# Iterate candidate instructions
	for ix in all_instructions:
		# Normalize fields across RPC dict and Geyser objects
		if isinstance(ix, dict):
			pid_idx = int(ix.get("programIdIndex", ix.get("program_id_index", -1)))
			# Allow either indices or pubkey strings for accounts
			raw_accounts = ix.get("accounts", [])
			acct_indices: List[int] = []
			for a in raw_accounts:
				if isinstance(a, int):
					acct_indices.append(int(a))
				elif isinstance(a, str):
					idx = key_to_index.get(a)
					if idx is not None:
						acct_indices.append(idx)
			data_bytes = _decode_ix_data(ix.get("data"))
			# Some RPC shapes include programId as a string instead of index
			pid_str = ix.get("programId") if isinstance(ix.get("programId"), str) else None
		else:
			pid_idx = int(getattr(ix, "program_id_index", -1))
			acct_indices = list(getattr(ix, "accounts", []))
			data_bytes = _decode_ix_data(getattr(ix, "data", None))
			pid_str = None

		# Determine if this instruction targets the DBC program
		is_dbc = False
		if pid_str:
			try:
				is_dbc = (pid_str == str(DBC_PROGRAM_PK))
			except Exception:
				is_dbc = False
		else:
			if pid_idx < 0 or pid_idx >= len(account_keys):
				continue
			is_dbc = (account_keys[pid_idx] == DBC_PROGRAM_PK)
		if not is_dbc:
			continue
		if not data_bytes:
			continue

		try:
			parsed_ix = coder.parse(data_bytes)
		except Exception:
			continue

		ix_name = getattr(parsed_ix, "name", None)
		if ix_name not in ("swap", "swap2"):
			continue

		order = _SWAP_ACCOUNTS_ORDER if ix_name == "swap" else _SWAP2_ACCOUNTS_ORDER
		name_to_index: Dict[str, int] = {}
		for i, name in enumerate(order):
			if i < len(acct_indices):
				name_to_index[name] = acct_indices[i]

		input_idx = name_to_index.get("input_token_account")
		output_idx = name_to_index.get("output_token_account")
		base_mint_idx = name_to_index.get("base_mint")
		quote_mint_idx = name_to_index.get("quote_mint")
		payer_idx = name_to_index.get("payer")

		# Compute direction for tracked mint (legacy field)
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

		tracked_decimals = _tracked_decimals(ntx.meta, mint_str)

		# Compute deltas for base/quote across all referenced accounts in this ix
		base_mint_str = str(account_keys[base_mint_idx]) if base_mint_idx is not None else None
		quote_mint_str = str(account_keys[quote_mint_idx]) if quote_mint_idx is not None else None

		def _delta_for_mint(mint_s: Optional[str]) -> Decimal:
			if not mint_s:
				return Decimal(0)
			total = Decimal(0)
			for idx in acct_indices:
				pre = pre_tb.get(idx)
				post = post_tb.get(idx)
				pre_amt = _ui_amount_from_entry(pre) if _entry_matches_mint(pre, mint_s) else Decimal(0)
				post_amt = _ui_amount_from_entry(post) if _entry_matches_mint(post, mint_s) else Decimal(0)
				total += (post_amt - pre_amt)
			return total

		delta_base: Decimal = _delta_for_mint(base_mint_str)
		delta_quote: Decimal = _delta_for_mint(quote_mint_str)
		# Derive side and price
		side: Optional[int] = None
		price: Optional[Decimal] = None
		if delta_base != 0:
			side = 1 if delta_base > 0 else -1
			if abs(delta_base) > 0 and abs(delta_quote) > 0:
				try:
					price = abs(delta_quote) / abs(delta_base)
				except Exception:
					price = None

		# Params structure may be .data.params or .data depending on IDL version
		try:
			params = parsed_ix.data.params
		except Exception:
			params = parsed_ix.data

		fee_payer_pk = str(account_keys[0]) if account_keys else None
		payer_pk = str(account_keys[payer_idx]) if (payer_idx is not None and payer_idx < len(account_keys)) else None

		received_at_ts = int(ntx.received_at.timestamp()) if ntx.received_at else None

		return {
			"signature": str(ntx.signature),
			"ix_name": ix_name,
			"slot_landed": ntx.slot,
			"received_at": received_at_ts,
			"fee_payer": fee_payer_pk,
			"payer": payer_pk,
			"base_mint": str(account_keys[base_mint_idx]) if base_mint_idx is not None else None,
			"quote_mint": str(account_keys[quote_mint_idx]) if quote_mint_idx is not None else None,
			"amount_in": getattr(params, "amount_in", getattr(params, "amount_0", None)),
			"min_out_or_amount1": getattr(params, "minimum_amount_out", getattr(params, "amount_1", None)),
			"swap_mode": getattr(params, "swap_mode", None),
			"direction_for_tracked_mint": direction,
			"tracked_mint_decimals": tracked_decimals,
			"delta_tracked": float(amt_change),
			"delta_base": float(delta_base) if delta_base is not None else None,
			"delta_quote": float(delta_quote) if delta_quote is not None else None,
			"side": side,
			"price": float(price) if price is not None else None,
		}

	return None
