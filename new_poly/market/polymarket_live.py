"""Polymarket live BTC/USD price feed used by crypto 5m markets."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from bisect import bisect_left, bisect_right
from collections import deque
from typing import Any, Optional

import websockets

log = logging.getLogger(__name__)

POLYMARKET_LIVE_DATA_WS_URL = "wss://ws-live-data.polymarket.com"


def subscribe_message(symbol: str = "btc/usd") -> dict[str, Any]:
    """Build the Polymarket live-data subscription payload.

    The live-data WS appears sensitive to the exact `filters` string shape:
    compact JSON matches the working Polymarket UI/probe subscription and
    receives continuous updates, while default `json.dumps` spacing can receive
    only an initial batch on some connections.
    """
    return {
        "action": "subscribe",
        "subscriptions": [{
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "filters": json.dumps({"symbol": symbol.lower()}, separators=(",", ":")),
        }],
    }


def _parse_tick(raw: dict[str, Any]) -> tuple[float, float] | None:
    try:
        timestamp_ms = float(raw["timestamp"])
        value = float(raw["value"])
    except (KeyError, TypeError, ValueError):
        return None
    if timestamp_ms <= 0 or value <= 0:
        return None
    return timestamp_ms / 1000.0, value


def price_ticks_from_message(data: dict[str, Any]) -> list[tuple[float, float]]:
    """Extract `(source_epoch_sec, price)` ticks from Polymarket live-data messages."""
    payload = data.get("payload") if isinstance(data, dict) else None
    if not isinstance(payload, dict):
        return []
    ticks: list[tuple[float, float]] = []
    batch = payload.get("data")
    if isinstance(batch, list):
        for item in batch:
            if isinstance(item, dict):
                tick = _parse_tick(item)
                if tick is not None:
                    ticks.append(tick)
    else:
        tick = _parse_tick(payload)
        if tick is not None:
            ticks.append(tick)
    return ticks


class PolymarketChainlinkBtcPriceFeed:
    """Keep a rolling stream of Polymarket's live BTC/USD Chainlink prices."""

    def __init__(self, symbol: str = "btc/usd", max_history_sec: float = 15.0, stale_reconnect_sec: float = 5.0):
        self._symbol = symbol.lower()
        self._max_history_sec = max_history_sec
        self._stale_reconnect_sec = max(1.0, float(stale_reconnect_sec))
        self._history: deque[tuple[float, float]] = deque()
        self._running = False
        self._recv_task: Optional[asyncio.Task] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    @property
    def latest_price(self) -> Optional[float]:
        return self._history[-1][1] if self._history else None

    def latest_age_sec(self, now: float | None = None) -> Optional[float]:
        if not self._history:
            return None
        return max(0.0, (now if now is not None else time.time()) - self._history[-1][0])

    @property
    def stale_reconnect_sec(self) -> float:
        return self._stale_reconnect_sec

    @property
    def max_history_sec(self) -> float:
        return self._max_history_sec

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._recv_task = asyncio.create_task(self._recv_loop())

    async def stop(self) -> None:
        self._running = False
        if self._recv_task:
            self._recv_task.cancel()
            await asyncio.gather(self._recv_task, return_exceptions=True)
            self._recv_task = None
        if self._ws:
            try:
                await asyncio.wait_for(self._ws.close(), timeout=3.0)
            except Exception:
                pass
            self._ws = None

    def price_at_or_before(self, ts: float, max_backward_sec: Optional[float] = None) -> Optional[float]:
        if not self._history:
            return None
        ts_values = [t for t, _ in self._history]
        idx = bisect_right(ts_values, ts) - 1
        if idx < 0:
            return None
        price_ts, price = self._history[idx]
        if max_backward_sec is not None and ts - price_ts > max_backward_sec:
            return None
        return price

    def first_price_at_or_after(self, ts: float, max_forward_sec: float = 30.0) -> Optional[float]:
        if not self._history:
            return None
        ts_values = [t for t, _ in self._history]
        idx = bisect_left(ts_values, ts)
        if idx >= len(self._history):
            return None
        first_ts, first_price = self._history[idx]
        if first_ts - ts > max_forward_sec:
            return None
        return first_price

    def _inject(self, ts: float, price: float) -> None:
        if not self._history or ts > self._history[-1][0]:
            self._history.append((ts, price))
            return
        if ts == self._history[-1][0]:
            self._history[-1] = (ts, price)
            return
        ts_values = [t for t, _ in self._history]
        idx = bisect_left(ts_values, ts)
        if idx < len(self._history) and self._history[idx][0] == ts:
            self._history[idx] = (ts, price)
        else:
            self._history.insert(idx, (ts, price))

    async def _recv_loop(self) -> None:
        subscribe = subscribe_message(self._symbol)
        backoff = 1.0
        while self._running:
            try:
                if self._ws is None:
                    self._ws = await websockets.connect(
                        POLYMARKET_LIVE_DATA_WS_URL,
                        ping_interval=20,
                        ping_timeout=20,
                    )
                    await self._ws.send(json.dumps(subscribe, separators=(",", ":")))
                    log.debug("PolymarketChainlinkBtcPriceFeed connected: %s %s", POLYMARKET_LIVE_DATA_WS_URL, self._symbol)
                    backoff = 1.0
                    last_tick_monotonic = time.monotonic()

                while self._running and self._ws is not None:
                    elapsed_since_tick = time.monotonic() - last_tick_monotonic
                    timeout = max(0.1, self._stale_reconnect_sec - elapsed_since_tick)
                    try:
                        msg = await asyncio.wait_for(self._ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        log.warning("PolymarketChainlinkBtcPriceFeed stale for %.1fs, reconnecting...", self._stale_reconnect_sec)
                        try:
                            await asyncio.wait_for(self._ws.close(), timeout=3.0)
                        except Exception:
                            pass
                        self._ws = None
                        break
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue
                    ticks = price_ticks_from_message(data)
                    for ts, price in ticks:
                        self._inject(ts, price)
                    if ticks:
                        last_tick_monotonic = time.monotonic()
                        self._prune(time.time())
                    elif time.monotonic() - last_tick_monotonic >= self._stale_reconnect_sec:
                        log.warning("PolymarketChainlinkBtcPriceFeed received no valid ticks for %.1fs, reconnecting...", self._stale_reconnect_sec)
                        try:
                            await asyncio.wait_for(self._ws.close(), timeout=3.0)
                        except Exception:
                            pass
                        self._ws = None
                        break
            except asyncio.CancelledError:
                raise
            except websockets.ConnectionClosed:
                log.warning("PolymarketChainlinkBtcPriceFeed WS closed, reconnecting...")
                self._ws = None
            except Exception as e:
                log.warning("PolymarketChainlinkBtcPriceFeed error: %s", e)
                self._ws = None
            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    def _prune(self, now: float) -> None:
        cutoff = now - self._max_history_sec
        while self._history and self._history[0][0] < cutoff:
            self._history.popleft()
