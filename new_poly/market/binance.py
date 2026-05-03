"""Minimal Binance trade feed for runtime strategies."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.parse
import urllib.request
from bisect import bisect_left, bisect_right
from collections import deque
from typing import Optional

import websockets

log = logging.getLogger(__name__)

BINANCE_WS_TEMPLATE = "wss://stream.binance.com:9443/ws/{}@trade"


class BinancePriceFeed:
    """Keep a rolling stream of Binance trade prices for one symbol."""

    def __init__(self, symbol: str, max_history_sec: float = 900.0):
        self._symbol = symbol.lower()
        self._ws_url = BINANCE_WS_TEMPLATE.format(self._symbol)
        self._max_history_sec = max_history_sec
        self._history: deque[tuple[float, float]] = deque()
        self._running = False
        self._recv_task: Optional[asyncio.Task] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    @property
    def latest_price(self) -> Optional[float]:
        return self._history[-1][1] if self._history else None

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
                transport = getattr(self._ws, "transport", None)
                if transport is not None:
                    transport.abort()
                else:
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

    async def fetch_open_at(self, epoch: float) -> Optional[float]:
        """Fetch BTC open price at epoch via Binance 1m klines REST API.

        Used as fallback when the WS feed has no data covering window start
        (cold-start mid-window attach). Result is injected into history so
        subsequent first_price_at_or_after calls find it without re-fetching.
        """
        data = await asyncio.to_thread(self._fetch_open_at_sync, epoch)
        if data is None:
            return None
        self._inject(epoch, data)
        log.debug("BinancePriceFeed REST fallback: epoch=%.0f open=%.2f", epoch, data)
        return data

    def _fetch_open_at_sync(self, epoch: float) -> Optional[float]:
        url = "https://api.binance.com/api/v3/klines?" + urllib.parse.urlencode({
            "symbol": self._symbol.upper(),
            "interval": "1m",
            "startTime": int(epoch * 1000),
            "limit": 1,
        })
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "new-poly/0.1"})
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if data:
                return float(data[0][1])
        except Exception as e:
            log.debug("BinancePriceFeed REST klines failed: %s", e)
        return None

    def _inject(self, ts: float, price: float) -> None:
        """Insert a (ts, price) point into the sorted history deque."""
        ts_values = [t for t, _ in self._history]
        idx = bisect_left(ts_values, ts)
        self._history.insert(idx, (ts, price))

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

    async def _recv_loop(self) -> None:
        while self._running:
            try:
                if self._ws is None:
                    self._ws = await websockets.connect(self._ws_url)
                    log.debug("BinancePriceFeed connected: %s", self._ws_url)

                async for msg in self._ws:
                    data = json.loads(msg)
                    now = time.time()
                    price = float(data["p"])
                    self._history.append((now, price))
                    self._prune(now)
            except asyncio.CancelledError:
                raise
            except websockets.ConnectionClosed:
                log.warning("BinancePriceFeed WS closed, reconnecting...")
                self._ws = None
            except Exception as e:
                log.warning("BinancePriceFeed error: %s", e)
                self._ws = None
            if self._running:
                await asyncio.sleep(1.0)

    def _prune(self, now: float) -> None:
        cutoff = now - self._max_history_sec
        while self._history and self._history[0][0] < cutoff:
            self._history.popleft()
