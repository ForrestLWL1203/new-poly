#!/usr/bin/env python3
"""Lightweight Polymarket CLOB and Binance WS smoke tests.

Default mode is safe: discover the live BTC 5m market, authenticate, read CLOB
metadata, and create a signed FAK order locally without posting it.

Use --post-impossible only when you explicitly want to hit POST /order with a
guarded, intentionally non-marketable FAK limit order.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import statistics
import time
from pathlib import Path
from typing import Any

import httpx
import websockets
from eth_account import Account
from py_clob_client_v2 import (
    AssetType,
    BalanceAllowanceParams,
    ClobClient,
    OrderArgs,
    OrderType,
)
from py_clob_client_v2.order_builder.constants import BUY, SELL


CLOB_HOST = "https://clob.polymarket.com"
GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"
BINANCE_BTC_TRADE_WS = "wss://stream.binance.com:9443/ws/btcusdt@trade"
BTC_5M_PREFIX = "btc-updown-5m"
BTC_5M_STEP = 300


def load_config(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text())
    if not data.get("private_key"):
        raise ValueError(f"missing private_key in {path}")
    return data


def signature_type(value: Any) -> int:
    if isinstance(value, int):
        return value
    return {"eoa": 0, "proxy": 1, "gnosis-safe": 2}.get(str(value or "proxy"), 1)


def create_client(config: dict[str, Any]) -> ClobClient:
    key = config["private_key"]
    sig_type = signature_type(config.get("signature_type", "proxy"))
    if sig_type == 0:
        funder = Account.from_key(key).address
    else:
        funder = config.get("proxy_address") or config.get("funder") or Account.from_key(key).address

    client = ClobClient(
        host=config.get("clob_host", CLOB_HOST),
        key=key,
        chain_id=int(config.get("chain_id", 137)),
        signature_type=sig_type,
        funder=funder,
    )
    creds = client.derive_api_key()
    if creds is None:
        creds = client.create_api_key()
    client.set_api_creds(creds)
    return client


def parse_tokens(raw: Any) -> list[str]:
    if isinstance(raw, str):
        return list(json.loads(raw))
    return list(raw or [])


def parse_time(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def discover_btc_5m(max_windows: int = 8) -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc)
    base = int(now.timestamp()) // BTC_5M_STEP * BTC_5M_STEP
    with httpx.Client(timeout=8.0) as client:
        for offset in range(-1, max_windows):
            epoch = base + offset * BTC_5M_STEP
            slug = f"{BTC_5M_PREFIX}-{epoch}"
            resp = client.get(GAMMA_MARKETS, params={"slug": slug})
            resp.raise_for_status()
            rows = resp.json()
            if not isinstance(rows, list):
                continue
            for market in rows:
                if market.get("slug") != slug or market.get("closed"):
                    continue
                end_time = parse_time(market.get("endDate"))
                if end_time is not None and end_time <= now:
                    continue
                tokens = parse_tokens(market.get("clobTokenIds"))
                if len(tokens) >= 2:
                    return {
                        "slug": slug,
                        "question": market.get("question"),
                        "active": market.get("active"),
                        "closed": market.get("closed"),
                        "start_epoch": epoch,
                        "end_date": market.get("endDate"),
                        "up_token": tokens[0],
                        "down_token": tokens[1],
                    }
    raise RuntimeError("no live/future BTC 5m market found")


def safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    idx = int(round((len(values) - 1) * pct))
    return round(values[idx], 3)


async def probe_binance_ws(seconds: float, max_messages: int = 10000) -> dict[str, Any]:
    """Sample Binance BTC trade WS and summarize receive interval frequency."""
    start_wall = time.time()
    start = time.perf_counter()
    recv_times: list[float] = []
    event_lags_ms: list[float] = []
    prices: list[float] = []

    async with websockets.connect(BINANCE_BTC_TRADE_WS, ping_interval=20, ping_timeout=10) as ws:
        while time.perf_counter() - start < seconds and len(recv_times) < max_messages:
            timeout = max(0.001, seconds - (time.perf_counter() - start))
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                break
            received_perf = time.perf_counter()
            received_wall = time.time()
            data = json.loads(raw)
            recv_times.append(received_perf)
            try:
                prices.append(float(data["p"]))
            except (KeyError, TypeError, ValueError):
                pass
            event_ms = data.get("E") or data.get("T")
            if event_ms is not None:
                try:
                    event_lags_ms.append((received_wall - (float(event_ms) / 1000.0)) * 1000.0)
                except (TypeError, ValueError):
                    pass

    intervals_ms = [
        (recv_times[i] - recv_times[i - 1]) * 1000.0
        for i in range(1, len(recv_times))
    ]
    elapsed = time.perf_counter() - start
    summary = {
        "action": "binance_ws_probe",
        "url": BINANCE_BTC_TRADE_WS,
        "started_at": dt.datetime.fromtimestamp(start_wall, dt.timezone.utc).isoformat(),
        "sample_seconds": round(elapsed, 3),
        "message_count": len(recv_times),
        "messages_per_sec": round(len(recv_times) / elapsed, 3) if elapsed > 0 else None,
        "interval_ms_min": round(min(intervals_ms), 3) if intervals_ms else None,
        "interval_ms_p50": percentile(intervals_ms, 0.50),
        "interval_ms_p90": percentile(intervals_ms, 0.90),
        "interval_ms_p99": percentile(intervals_ms, 0.99),
        "interval_ms_max": round(max(intervals_ms), 3) if intervals_ms else None,
        "event_lag_ms_p50": percentile(event_lags_ms, 0.50),
        "event_lag_ms_p90": percentile(event_lags_ms, 0.90),
        "event_lag_ms_max": round(max(event_lags_ms), 3) if event_lags_ms else None,
        "first_price": prices[0] if prices else None,
        "last_price": prices[-1] if prices else None,
        "unique_price_count": len(set(prices)),
    }
    return summary


def levels(book: dict[str, Any], side: str) -> list[tuple[float, float]]:
    parsed: list[tuple[float, float]] = []
    for item in book.get(side, []) or []:
        try:
            price = float(item["price"])
            size = float(item.get("size", 0))
        except (KeyError, TypeError, ValueError):
            continue
        if size > 0:
            parsed.append((price, size))
    parsed.sort(key=lambda item: item[0], reverse=(side == "bids"))
    return parsed


def book_summary(client: ClobClient, token_id: str) -> dict[str, Any]:
    t0 = time.perf_counter()
    book = client.get_order_book(token_id)
    elapsed_ms = round((time.perf_counter() - t0) * 1000)
    bids = levels(book, "bids")
    asks = levels(book, "asks")
    return {
        "token_id": token_id,
        "book_ms": elapsed_ms,
        "best_bid": bids[0][0] if bids else None,
        "best_bid_size": bids[0][1] if bids else None,
        "best_ask": asks[0][0] if asks else None,
        "best_ask_size": asks[0][1] if asks else None,
        "bid_levels": len(bids),
        "ask_levels": len(asks),
        "min_order_size": book.get("min_order_size"),
        "tick_size": book.get("tick_size"),
        "hash": book.get("hash"),
    }


def choose_impossible_token(
    client: ClobClient,
    market: dict[str, Any],
    order_side: str,
    price: float,
) -> tuple[str, str, dict[str, Any]]:
    """Pick UP/DOWN token whose current book proves the FAK cannot match."""
    candidates = [
        ("up", market["up_token"], book_summary(client, market["up_token"])),
        ("down", market["down_token"], book_summary(client, market["down_token"])),
    ]
    if order_side == "buy":
        safe = [
            item for item in candidates
            if item[2]["best_ask"] is None or price < float(item[2]["best_ask"])
        ]
        if not safe:
            raise RuntimeError(f"buy price {price} could cross current best ask on both tokens")
        # Prefer a book with a visible ask, because it proves price is below ask.
        safe.sort(key=lambda item: (item[2]["best_ask"] is None, -(item[2]["best_ask"] or 0)))
    else:
        safe = [
            item for item in candidates
            if item[2]["best_bid"] is None or price > float(item[2]["best_bid"])
        ]
        if not safe:
            raise RuntimeError(f"sell price {price} could cross current best bid on both tokens")
        safe.sort(key=lambda item: (item[2]["best_bid"] is None, item[2]["best_bid"] or 0))
    side, token_id, summary = safe[0]
    return side, token_id, {"selected": summary, "all_candidates": [item[2] for item in candidates]}


def token_summary(client: ClobClient, token_id: str) -> dict[str, Any]:
    out: dict[str, Any] = {"token_id": token_id}
    for name, fn in (
        ("midpoint", lambda: client.get_midpoint(token_id)),
        ("tick_size", lambda: client.get_tick_size(token_id)),
        ("neg_risk", lambda: client.get_neg_risk(token_id)),
    ):
        t0 = time.perf_counter()
        try:
            out[name] = fn()
            out[f"{name}_ms"] = round((time.perf_counter() - t0) * 1000)
        except Exception as exc:
            out[name] = {"error": type(exc).__name__, "message": str(exc)}
    try:
        params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        balance = client.get_balance_allowance(params)
        out["balance_shares"] = float(balance.get("balance", 0)) / 1_000_000
    except Exception as exc:
        out["balance_shares"] = {"error": type(exc).__name__, "message": str(exc)}
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default="/opt/new-poly/shared/polymarket_config.json",
        help="Polymarket account config JSON",
    )
    parser.add_argument("--side", choices=["up", "down"], default="down")
    parser.add_argument("--order-side", choices=["buy", "sell"], default="buy")
    parser.add_argument("--price", type=float, default=0.01)
    parser.add_argument("--size", type=float, default=5.0)
    parser.add_argument(
        "--binance-ws-seconds",
        type=float,
        default=0.0,
        help="Sample Binance btcusdt@trade WS for this many seconds before CLOB probing",
    )
    parser.add_argument(
        "--post-impossible",
        action="store_true",
        help="Actually call POST /order after book guard proves the FAK limit cannot match",
    )
    parser.add_argument(
        "--post-intentional-fail",
        action="store_true",
        help="Deprecated alias for --post-impossible",
    )
    args = parser.parse_args()

    if args.binance_ws_seconds > 0:
        print(safe_json(asyncio.run(probe_binance_ws(args.binance_ws_seconds))))

    should_post = args.post_impossible or args.post_intentional_fail

    config = load_config(Path(args.config))
    market = discover_btc_5m()

    t0 = time.perf_counter()
    client = create_client(config)
    auth_ms = round((time.perf_counter() - t0) * 1000)

    if should_post:
        selected_side, token_id, guard = choose_impossible_token(
            client,
            market,
            args.order_side,
            args.price,
        )
    else:
        selected_side = args.side
        token_id = market["up_token"] if args.side == "up" else market["down_token"]
        guard = {"selected": book_summary(client, token_id)}

    print(safe_json({
        "action": "discover_and_auth",
        "auth_ms": auth_ms,
        "market": market,
    }))

    print(safe_json({
        "action": "token_summary",
        "up": token_summary(client, market["up_token"]),
        "down": token_summary(client, market["down_token"]),
    }))

    order_args = OrderArgs(
        token_id=token_id,
        price=args.price,
        size=args.size,
        side=BUY if args.order_side == "buy" else SELL,
    )
    t1 = time.perf_counter()
    signed = client.create_order(order_args)
    create_order_ms = round((time.perf_counter() - t1) * 1000)
    print(safe_json({
        "action": "signed_order_created",
        "create_order_ms": create_order_ms,
        "posted": False,
        "book_guard": guard,
        "token_id": token_id,
        "side": selected_side,
        "order_side": args.order_side,
        "price": args.price,
        "size": args.size,
    }))

    if not should_post:
        return 0

    t2 = time.perf_counter()
    try:
        resp = client.post_order(signed, OrderType.FAK)
        post_ms = round((time.perf_counter() - t2) * 1000)
        print(safe_json({
            "action": "post_order",
            "post_order_ms": post_ms,
            "expected": "unmatched_or_rejected_without_fill",
            "response": resp,
        }))
    except Exception as exc:
        post_ms = round((time.perf_counter() - t2) * 1000)
        print(safe_json({
            "action": "post_order_error",
            "post_order_ms": post_ms,
            "error_type": type(exc).__name__,
            "message": str(exc),
            "status_code": getattr(exc, "status_code", None),
            "error_msg": getattr(exc, "error_msg", None),
        }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
