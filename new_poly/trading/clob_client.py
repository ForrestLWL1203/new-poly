"""CLOB client, auth, and order metadata helpers."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from new_poly import config

_client = None
_tick_size_cache: dict[str, float] = {}
_order_params_cache: dict[str, tuple[str, bool]] = {}


def _configure_proxy() -> None:
    from py_clob_client_v2.http_helpers import helpers as _http_helpers

    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if proxy and hasattr(_http_helpers, "_http_client"):
        _http_helpers._http_client = _http_helpers._http_client.__class__(http2=True, proxy=proxy)


def load_polymarket_config(path: Path | None = None) -> dict[str, Any]:
    candidates = []
    if path is not None:
        candidates.append(path)
    if os.environ.get("POLYMARKET_CONFIG"):
        candidates.append(Path(os.environ["POLYMARKET_CONFIG"]))
    candidates.append(Path.home() / ".config" / "polymarket" / "config.json")
    for candidate in candidates:
        if candidate.exists():
            return json.loads(candidate.read_text())
    raise FileNotFoundError("Polymarket config not found; set POLYMARKET_CONFIG or run polymarket setup")


def signature_type(value: Any) -> int:
    if isinstance(value, int):
        return value
    return {"eoa": 0, "proxy": 1, "gnosis-safe": 2}.get(str(value or "proxy"), config.SIGNATURE_TYPE)


def create_clob_client(config_path: Path | None = None):
    from eth_account import Account
    from py_clob_client_v2 import ClobClient

    _configure_proxy()
    cfg = load_polymarket_config(config_path)
    key = cfg.get("private_key") or os.environ.get("PK")
    if not key:
        raise ValueError("missing private_key")
    sig_type = signature_type(cfg.get("signature_type", "proxy"))
    if sig_type == 0:
        funder = Account.from_key(key).address
    else:
        funder = cfg.get("proxy_address") or cfg.get("funder") or os.environ.get("FUNDER")
        if not funder:
            funder = Account.from_key(key).address
            sys.stderr.write("Warning: proxy wallet config has no proxy_address/funder; using EOA as funder\n")
    client = ClobClient(
        host=cfg.get("clob_host", config.CLOB_HOST),
        key=key,
        chain_id=int(cfg.get("chain_id", config.CHAIN_ID)),
        signature_type=sig_type,
        funder=funder,
    )
    creds = client.derive_api_key()
    if creds is None:
        creds = client.create_api_key()
    client.set_api_creds(creds)
    return client


def get_client(config_path: Path | None = None):
    global _client
    if _client is None:
        _client = create_clob_client(config_path)
    return _client


def get_tick_size(token_id: str) -> float:
    cached = _tick_size_cache.get(token_id)
    if cached is not None:
        return cached
    try:
        value = float(get_client().get_tick_size(token_id))
    except Exception:
        value = 0.001
    _tick_size_cache[token_id] = value
    return value


def prefetch_order_params(token_id: str) -> None:
    if token_id in _order_params_cache:
        return
    client = get_client()
    tick_str = client.get_tick_size(token_id)
    _tick_size_cache[token_id] = float(tick_str)
    neg_risk = bool(client.get_neg_risk(token_id))
    _order_params_cache[token_id] = (tick_str, neg_risk)


def get_order_options(token_id: str):
    from py_clob_client_v2 import PartialCreateOrderOptions

    cached = _order_params_cache.get(token_id)
    if cached is None:
        return None
    tick_str, neg_risk = cached
    return PartialCreateOrderOptions(tick_size=tick_str, neg_risk=neg_risk)


def get_token_balance(token_id: str, *, safe: bool = True) -> float | None:
    from py_clob_client_v2 import AssetType, BalanceAllowanceParams

    try:
        params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        resp = get_client().get_balance_allowance(params)
    except Exception:
        return None
    if not resp or "balance" not in resp:
        return None
    raw = float(resp["balance"]) / 1_000_000.0
    if not safe:
        return raw
    tick = max(get_tick_size(token_id), 0.0001)
    truncated = int(raw * 10_000) / 10_000
    return max(0.0, truncated - min(tick, raw * 0.01))
