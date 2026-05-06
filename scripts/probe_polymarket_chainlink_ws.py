#!/usr/bin/env python3
"""Probe Polymarket live BTC crypto price WebSocket.

This is a research probe, not a trading data source yet. It subscribes to the
Polymarket live-data Chainlink BTC/USD stream, records update cadence, and
compares completed 5-minute windows against the public crypto-price API.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import math
import statistics
import sys
import time
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import websockets

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.market.prob_edge_data import POLYMARKET_CRYPTO_PRICE_API, compact_float, iso_z

WS_URL = "wss://ws-live-data.polymarket.com"
WINDOW_SECONDS = 300


@dataclass
class PriceTick:
    source_ts_ms: int
    value: float
    recv_monotonic: float
    recv_iso: str


@dataclass
class WindowProbe:
    start_epoch: int
    end_epoch: int
    open_price: float | None = None
    close_price: float | None = None
    api_completed: bool | None = None
    api_raw_keys: list[str] = field(default_factory=list)
    finalized: bool = False

    @property
    def slug(self) -> str:
        return f"btc-updown-5m-{self.start_epoch}"

    @property
    def start_dt(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self.start_epoch, tz=dt.timezone.utc)

    @property
    def end_dt(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self.end_epoch, tz=dt.timezone.utc)


class JsonlWriter:
    def __init__(self, path: Path | None) -> None:
        self.handle = None
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.handle = path.open("a", encoding="utf-8")

    def write(self, row: dict[str, Any]) -> None:
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
        print(line, flush=True)
        if self.handle is not None:
            self.handle.write(line + "\n")
            self.handle.flush()

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()


def _float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def crypto_price_api_url(start_epoch: int, end_epoch: int) -> str:
    start = dt.datetime.fromtimestamp(start_epoch, tz=dt.timezone.utc)
    end = dt.datetime.fromtimestamp(end_epoch, tz=dt.timezone.utc)
    return POLYMARKET_CRYPTO_PRICE_API + "?" + urllib.parse.urlencode({
        "symbol": "BTC",
        "eventStartTime": iso_z(start),
        "variant": "fiveminute",
        "endDate": iso_z(end),
    })


def fetch_crypto_price_api(start_epoch: int, end_epoch: int) -> dict[str, Any] | None:
    try:
        req = urllib.request.Request(
            crypto_price_api_url(start_epoch, end_epoch),
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def parse_crypto_api(raw: dict[str, Any] | None) -> dict[str, Any]:
    raw = raw if isinstance(raw, dict) else {}
    return {
        "open_price": _float(raw.get("openPrice")),
        "close_price": _float(raw.get("closePrice")),
        "completed": bool(raw.get("completed")) if "completed" in raw else None,
        "incomplete": bool(raw.get("incomplete")) if "incomplete" in raw else None,
        "cached": bool(raw.get("cached")) if "cached" in raw else None,
        "keys": sorted(str(key) for key in raw.keys()),
    }


def floor_window(epoch: int) -> int:
    return (epoch // WINDOW_SECONDS) * WINDOW_SECONDS


def subscribe_message() -> dict[str, Any]:
    return {
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices_chainlink",
                "type": "update",
                "filters": json.dumps({"symbol": "btc/usd"}, separators=(",", ":")),
            }
        ],
    }


def extract_ticks(message: dict[str, Any], recv_monotonic: float, recv_iso: str) -> list[PriceTick]:
    payload = message.get("payload") if isinstance(message, dict) else None
    if isinstance(payload, dict):
        ts = payload.get("timestamp")
        value = _float(payload.get("value"))
        if isinstance(ts, (int, float)) and value is not None:
            return [PriceTick(int(ts), value, recv_monotonic, recv_iso)]
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        return []
    ticks: list[PriceTick] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        ts = item.get("timestamp")
        value = _float(item.get("value"))
        if isinstance(ts, (int, float)) and value is not None:
            ticks.append(PriceTick(int(ts), value, recv_monotonic, recv_iso))
    return ticks


def nearest_tick(ticks: deque[PriceTick], target_ms: int, *, max_distance_ms: int) -> PriceTick | None:
    best: PriceTick | None = None
    best_dist: int | None = None
    for tick in ticks:
        dist = abs(tick.source_ts_ms - target_ms)
        if best_dist is None or dist < best_dist:
            best = tick
            best_dist = dist
    if best is None or best_dist is None or best_dist > max_distance_ms:
        return None
    return best


def frequency_summary(ticks: list[PriceTick]) -> dict[str, Any]:
    if len(ticks) < 2:
        return {"tick_count": len(ticks)}
    sorted_ticks = sorted(ticks, key=lambda tick: tick.source_ts_ms)
    intervals = [
        (right.source_ts_ms - left.source_ts_ms) / 1000.0
        for left, right in zip(sorted_ticks, sorted_ticks[1:])
        if right.source_ts_ms > left.source_ts_ms
    ]
    if not intervals:
        return {"tick_count": len(ticks)}
    return {
        "tick_count": len(ticks),
        "interval_avg_sec": compact_float(statistics.fmean(intervals), 3),
        "interval_median_sec": compact_float(statistics.median(intervals), 3),
        "interval_min_sec": compact_float(min(intervals), 3),
        "interval_max_sec": compact_float(max(intervals), 3),
    }


async def run_probe(args: argparse.Namespace) -> int:
    writer = JsonlWriter(args.jsonl)
    recent_ticks: deque[PriceTick] = deque(maxlen=20_000)
    all_ticks: list[PriceTick] = []
    windows: dict[int, WindowProbe] = {}
    completed: list[WindowProbe] = []
    last_summary = time.monotonic()
    started_at = time.monotonic()

    def ensure_windows(now_epoch: int) -> None:
        base = floor_window(now_epoch)
        for offset in range(0, args.windows + 2):
            start = base + offset * WINDOW_SECONDS
            if start not in windows:
                windows[start] = WindowProbe(start, start + WINDOW_SECONDS)

    async def refresh_window_api(window: WindowProbe) -> None:
        raw = await asyncio.to_thread(fetch_crypto_price_api, window.start_epoch, window.end_epoch)
        parsed = parse_crypto_api(raw)
        if parsed["open_price"] is not None:
            window.open_price = parsed["open_price"]
        if parsed["close_price"] is not None:
            window.close_price = parsed["close_price"]
        window.api_completed = parsed["completed"]
        window.api_raw_keys = parsed["keys"]

    async def finalize_ready_windows(now_epoch: int) -> None:
        for window in sorted(windows.values(), key=lambda item: item.start_epoch):
            if window.finalized or now_epoch < window.end_epoch + args.finalize_delay_sec:
                continue
            await refresh_window_api(window)
            if window.close_price is None and now_epoch < window.end_epoch + args.close_wait_sec:
                continue
            end_tick = nearest_tick(recent_ticks, window.end_epoch * 1000, max_distance_ms=args.nearest_ms)
            ws_close = end_tick.value if end_tick is not None else None
            ws_distance_ms = abs(end_tick.source_ts_ms - window.end_epoch * 1000) if end_tick is not None else None
            open_price = window.open_price
            api_close = window.close_price
            ws_side = None if open_price is None or ws_close is None else ("up" if ws_close >= open_price else "down")
            api_side = None if open_price is None or api_close is None else ("up" if api_close >= open_price else "down")
            close_diff = None if api_close is None or ws_close is None else ws_close - api_close
            row = {
                "event": "window_finalized",
                "slug": window.slug,
                "window_start": iso_z(window.start_dt),
                "window_end": iso_z(window.end_dt),
                "open_price": compact_float(open_price, 6),
                "api_close_price": compact_float(api_close, 6),
                "ws_close_price": compact_float(ws_close, 6),
                "ws_close_distance_ms": ws_distance_ms,
                "close_diff": compact_float(close_diff, 6),
                "api_side": api_side,
                "ws_side": ws_side,
                "side_match": None if api_side is None or ws_side is None else api_side == ws_side,
                "api_completed": window.api_completed,
                "api_raw_keys": window.api_raw_keys,
            }
            writer.write(row)
            window.finalized = True
            completed.append(window)

    try:
        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps(subscribe_message(), separators=(",", ":")))
            writer.write({"event": "subscribed", "url": WS_URL, "topic": "crypto_prices_chainlink", "symbol": "btc/usd"})
            while len(completed) < args.windows:
                if args.max_seconds is not None and time.monotonic() - started_at >= args.max_seconds:
                    writer.write({"event": "timeout", "completed_windows": len(completed), "max_seconds": args.max_seconds})
                    return 2
                raw_message = await asyncio.wait_for(ws.recv(), timeout=args.ws_timeout_sec)
                recv_monotonic = time.monotonic()
                recv_iso = dt.datetime.now(dt.timezone.utc).isoformat()
                try:
                    message = json.loads(raw_message)
                except json.JSONDecodeError:
                    if raw_message:
                        writer.write({"event": "non_json_message", "message": str(raw_message)[:300]})
                    continue
                ticks = extract_ticks(message, recv_monotonic, recv_iso)
                now_epoch = int(dt.datetime.now(dt.timezone.utc).timestamp())
                ensure_windows(now_epoch)
                for tick in ticks:
                    recent_ticks.append(tick)
                    all_ticks.append(tick)
                if ticks and args.log_ticks:
                    writer.write({
                        "event": "ticks",
                        "count": len(ticks),
                        "first_ts": ticks[0].source_ts_ms,
                        "last_ts": ticks[-1].source_ts_ms,
                        "last_value": compact_float(ticks[-1].value, 6),
                    })
                if ticks and time.monotonic() - last_summary >= args.summary_sec:
                    last_summary = time.monotonic()
                    writer.write({
                        "event": "frequency_summary",
                        **frequency_summary(all_ticks[-args.summary_sample:]),
                        "last_value": compact_float(ticks[-1].value, 6),
                        "completed_windows": len(completed),
                    })
                await finalize_ready_windows(now_epoch)
    finally:
        writer.write({"event": "probe_summary", **frequency_summary(all_ticks), "completed_windows": len(completed)})
        writer.close()
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe Polymarket Chainlink BTC live-data WebSocket")
    parser.add_argument("--windows", type=int, default=3, help="Number of completed 5m windows to compare")
    parser.add_argument("--jsonl", type=Path)
    parser.add_argument("--log-ticks", action="store_true", help="Log compact tick batches")
    parser.add_argument("--summary-sec", type=float, default=30.0)
    parser.add_argument("--summary-sample", type=int, default=500)
    parser.add_argument("--nearest-ms", type=int, default=5_000, help="Max distance between window end and WS tick")
    parser.add_argument("--finalize-delay-sec", type=int, default=8, help="Delay after window end before fetching API close")
    parser.add_argument("--close-wait-sec", type=int, default=90, help="Max seconds after window end to wait for API closePrice")
    parser.add_argument("--ws-timeout-sec", type=float, default=30.0)
    parser.add_argument("--max-seconds", type=float, help="Optional wall-clock cap for smoke tests")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    if args.windows <= 0:
        raise SystemExit("--windows must be positive")
    return asyncio.run(run_probe(args))


if __name__ == "__main__":
    raise SystemExit(main())
