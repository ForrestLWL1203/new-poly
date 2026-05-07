from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.market import stream as stream_module
from new_poly.market.binance import BinancePriceFeed
from new_poly.market.coinbase import CoinbaseBtcPriceFeed
from new_poly.market.deribit import DvolSnapshot
from new_poly.market.prob_edge_data import WindowPrices, effective_price
from new_poly.market.series import MarketSeries
from new_poly.market.stream import PriceStream
from new_poly.trading import clob_client as clob_client_module
from new_poly.trading.clob_client import _build_http_client_kwargs


def test_market_series_builds_btc_5m_slugs() -> None:
    series = MarketSeries.from_known("btc-updown-5m")

    assert series.slug_step == 300
    assert series.epoch_to_slug(1777749300) == "btc-updown-5m-1777749300"


def test_binance_price_feed_history_lookup_helpers() -> None:
    feed = BinancePriceFeed("btcusdt")

    feed._inject(100.0, 10.0)
    feed._inject(105.0, 10.5)

    assert feed.latest_price == 10.5
    assert feed.price_at_or_before(102.0) == 10.0
    assert feed.price_at_or_before(104.0, max_backward_sec=3.0) is None
    assert feed.first_price_at_or_after(101.0, max_forward_sec=10.0) == 10.5
    assert feed.first_price_at_or_after(101.0, max_forward_sec=1.0) is None


def test_coinbase_price_feed_parses_match_messages() -> None:
    feed = CoinbaseBtcPriceFeed()

    assert feed._price_from_message({"type": "match", "price": "101234.56"}) == 101234.56
    assert feed._price_from_message({"type": "subscriptions", "channels": []}) is None


def test_coinbase_price_feed_history_lookup_helpers() -> None:
    feed = CoinbaseBtcPriceFeed()

    feed._inject(100.0, 10.0)
    feed._inject(105.0, 10.5)

    assert feed.latest_price == 10.5
    assert feed.price_at_or_before(102.0) == 10.0
    assert feed.first_price_at_or_after(101.0, max_forward_sec=10.0) == 10.5


def test_prob_edge_data_effective_price_basis_adjustment_is_shared() -> None:
    feed = BinancePriceFeed("btcusdt")
    feed._inject(100.0, 105.0)
    prices = WindowPrices(k_price=102.0, binance_open_price=100.0)

    price = effective_price(feed, None, prices, coinbase_enabled=False)

    assert price.source == "proxy_binance_basis_adjusted"
    assert price.proxy == 105.0
    assert price.proxy_open == 100.0
    assert price.effective == 107.0


def test_price_stream_updates_order_book_from_events() -> None:
    async def on_price(_update):
        return None

    stream = PriceStream(on_price=on_price)
    token_id = "token-a"
    stream._handle_event({
        "event_type": "book",
        "asset_id": token_id,
        "bids": [{"price": "0.49", "size": "10"}],
        "asks": [{"price": "0.51", "size": "12"}],
    })

    assert stream.get_latest_bid_levels_with_size(token_id) == [(0.49, 10.0)]
    assert stream.get_latest_ask_levels_with_size(token_id) == [(0.51, 12.0)]
    assert stream.get_latest_best_ask_age(token_id) is not None

    stream._handle_event({
        "event_type": "price_change",
        "asset_id": token_id,
        "price_changes": [{"asset_id": token_id, "side": "SELL", "price": "0.50", "size": "5"}],
    })

    asks = stream.get_latest_ask_levels_with_size(token_id)
    assert asks[0] == (0.5, 5.0)

    diag = stream.diagnostics(reset_counts=True)
    assert diag["event_counts_since_read"]["book"] == 1
    assert diag["event_counts_since_read"]["price_change"] == 1
    assert diag["depth_events_since_read"] == 2
    assert stream.diagnostics()["event_counts_since_read"] == {}


def test_price_stream_level_one_prefers_newer_best_bid_ask_but_keeps_depth_age() -> None:
    async def on_price(_update):
        return None

    stream = PriceStream(on_price=on_price)
    token_id = "token-a"
    stream._handle_event({
        "event_type": "book",
        "asset_id": token_id,
        "bids": [{"price": "0.49", "size": "10"}],
        "asks": [{"price": "0.51", "size": "12"}],
    })
    stream._books[token_id]["received_at"] = 1.0
    stream._handle_event({
        "event_type": "best_bid_ask",
        "asset_id": token_id,
        "best_bid": "0.52",
        "best_ask": "0.53",
    })

    assert stream.get_latest_best_bid(token_id) == 0.52
    assert stream.get_latest_best_ask(token_id) == 0.53
    assert stream.get_latest_best_ask_age(token_id) > 1.0


def test_prefetch_order_params_reports_failed_api_without_raising(monkeypatch) -> None:
    class Client:
        def get_tick_size(self, _token_id: str) -> str:
            return "0.01"

        def get_neg_risk(self, _token_id: str) -> bool:
            raise TimeoutError("neg risk timeout")

    monkeypatch.setattr(clob_client_module, "get_client", lambda: Client())
    clob_client_module._tick_size_cache.clear()
    clob_client_module._order_params_cache.clear()

    result = clob_client_module.prefetch_order_params("token-a", raise_on_error=False)

    assert result["ok"] is False
    assert result["failed_operation"] == "get_neg_risk"
    assert result["tick_size"] == "0.01"
    assert "neg risk timeout" in result["error"]
    assert clob_client_module.get_tick_size("token-a") == 0.01


@pytest.mark.asyncio
async def test_price_stream_ping_failure_closes_ws_for_reconnect(monkeypatch) -> None:
    async def on_price(_update):
        return None

    class BrokenPingWs:
        def __init__(self) -> None:
            self.closed = False
            self.transport = None

        async def send(self, _message: str) -> None:
            raise ConnectionError("send failed")

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(stream_module, "PING_INTERVAL", 0.001)
    stream = PriceStream(on_price=on_price)
    ws = BrokenPingWs()
    stream._ws = ws
    stream._running = True
    task = asyncio.create_task(stream._ping_loop())

    try:
        async with asyncio.timeout(1.0):
            while stream._ws is not None:
                await asyncio.sleep(0.01)
    finally:
        stream._running = False
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert ws.closed is True
    assert stream._ws is None


@pytest.mark.asyncio
async def test_price_stream_idle_watchdog_closes_silent_ws(monkeypatch) -> None:
    async def on_price(_update):
        return None

    class SilentWs:
        def __init__(self) -> None:
            self.closed = False
            self.transport = None

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(stream_module, "IDLE_CHECK_INTERVAL", 0.001)
    monkeypatch.setattr(stream_module.config, "CLOB_WS_IDLE_RECONNECT_SEC", 0.001)
    stream = PriceStream(on_price=on_price)
    ws = SilentWs()
    stream._ws = ws
    stream._running = True
    stream._last_message_at = stream_module.time.monotonic() - 1.0
    task = asyncio.create_task(stream._idle_watchdog_loop())

    try:
        async with asyncio.timeout(1.0):
            while stream._ws is not None:
                await asyncio.sleep(0.01)
    finally:
        stream._running = False
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert ws.closed is True
    assert stream._ws is None


@pytest.mark.asyncio
async def test_price_stream_depth_watchdog_closes_bbo_only_ws(monkeypatch) -> None:
    async def on_price(_update):
        return None

    class BboOnlyWs:
        def __init__(self) -> None:
            self.closed = False
            self.transport = None

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(stream_module, "IDLE_CHECK_INTERVAL", 0.001)
    monkeypatch.setattr(stream_module.config, "CLOB_WS_IDLE_RECONNECT_SEC", 100.0)
    monkeypatch.setattr(stream_module.config, "CLOB_DEPTH_IDLE_RECONNECT_SEC", 0.001)
    stream = PriceStream(on_price=on_price)
    ws = BboOnlyWs()
    stream._ws = ws
    stream._running = True
    stream._connected_tokens = ["token-a", "token-b"]
    stream._last_message_at = stream_module.time.monotonic()
    stream._last_depth_update_at = stream_module.time.monotonic() - 1.0
    task = asyncio.create_task(stream._idle_watchdog_loop())

    try:
        async with asyncio.timeout(1.0):
            while stream._ws is not None:
                await asyncio.sleep(0.01)
    finally:
        stream._running = False
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert ws.closed is True
    assert stream._ws is None


def test_dvol_snapshot_serializes_sigma_and_age() -> None:
    snap = DvolSnapshot(
        source="deribit_dvol",
        currency="BTC",
        dvol=39.48,
        sigma=0.3948,
        timestamp_ms=1_000_000,
        fetched_at=1_030.0,
    )

    assert snap.to_json() == {
        "source": "deribit_dvol",
        "currency": "BTC",
        "dvol": 39.48,
        "sigma": 0.3948,
        "timestamp_ms": 1_000_000,
        "age_sec": 30.0,
    }


def test_clob_http_client_kwargs_enable_keepalive_pool(monkeypatch) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.local:8080")

    kwargs = _build_http_client_kwargs()

    assert kwargs["http2"] is True
    assert kwargs["proxy"] == "http://proxy.local:8080"
    assert kwargs["limits"].max_connections == 100
    assert kwargs["limits"].max_keepalive_connections == 20
    assert kwargs["timeout"].connect == 2.0

    monkeypatch.delenv("HTTPS_PROXY", raising=False)
    monkeypatch.delenv("https_proxy", raising=False)
    direct_kwargs = _build_http_client_kwargs()
    assert "proxy" not in direct_kwargs
    assert direct_kwargs["limits"].max_connections == 100
