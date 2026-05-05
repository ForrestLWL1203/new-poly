"""Minimal Coinbase BTC/USD trade feed for runtime strategies."""

from __future__ import annotations

import asyncio
import datetime as dt
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

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"


class CoinbaseBtcPriceFeed:
    """Keep a rolling stream of Coinbase BTC-USD match prices."""

    def __init__(self, product_id: str = "BTC-USD", max_history_sec: float = 900.0):
        self._product_id = product_id.upper()
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

    async def fetch_open_at(self, epoch: float) -> Optional[float]:
        data = await asyncio.to_thread(self._fetch_open_at_sync, epoch)
        if data is None:
            return None
        self._inject(epoch, data)
        log.debug("CoinbaseBtcPriceFeed REST fallback: epoch=%.0f open=%.2f", epoch, data)
        return data

    def _fetch_open_at_sync(self, epoch: float) -> Optional[float]:
        start = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
        end = start + dt.timedelta(seconds=60)
        url = "https://api.exchange.coinbase.com/products/{}/candles?".format(urllib.parse.quote(self._product_id)) + urllib.parse.urlencode({
            "granularity": 60,
            "start": start.isoformat().replace("+00:00", "Z"),
            "end": end.isoformat().replace("+00:00", "Z"),
        })
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "new-poly/0.1", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if data:
                # Coinbase candle shape: [time, low, high, open, close, volume].
                return float(sorted(data, key=lambda item: item[0])[0][3])
        except Exception as e:
            log.debug("CoinbaseBtcPriceFeed REST candles failed: %s", e)
        return None

    def _inject(self, ts: float, price: float) -> None:
        ts_values = [t for t, _ in self._history]
        idx = bisect_left(ts_values, ts)
        self._history.insert(idx, (ts, price))

    def _price_from_message(self, data: dict[str, object]) -> Optional[float]:
        if data.get("type") not in {"match", "last_match"}:
            return None
        try:
            return float(data["price"])  # type: ignore[index]
        except (KeyError, TypeError, ValueError):
            return None

    async def _recv_loop(self) -> None:
        subscribe = {
            "type": "subscribe",
            "product_ids": [self._product_id],
            "channels": ["matches"],
        }
        while self._running:
            try:
                if self._ws is None:
                    self._ws = await websockets.connect(COINBASE_WS_URL)
                    await self._ws.send(json.dumps(subscribe))
                    log.debug("CoinbaseBtcPriceFeed connected: %s %s", COINBASE_WS_URL, self._product_id)

                async for msg in self._ws:
                    data = json.loads(msg)
                    price = self._price_from_message(data)
                    if price is None:
                        continue
                    now = time.time()
                    self._history.append((now, price))
                    self._prune(now)
            except asyncio.CancelledError:
                raise
            except websockets.ConnectionClosed:
                log.warning("CoinbaseBtcPriceFeed WS closed, reconnecting...")
                self._ws = None
            except Exception as e:
                log.warning("CoinbaseBtcPriceFeed error: %s", e)
                self._ws = None
            if self._running:
                await asyncio.sleep(1.0)

    def _prune(self, now: float) -> None:
        cutoff = now - self._max_history_sec
        while self._history and self._history[0][0] < cutoff:
            self._history.popleft()
