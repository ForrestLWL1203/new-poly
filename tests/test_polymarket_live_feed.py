from __future__ import annotations

import sys
import asyncio
import logging
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.market import polymarket_live
from new_poly.market.polymarket_live import PolymarketChainlinkBtcPriceFeed, price_ticks_from_message, subscribe_message


def test_subscribe_message_uses_compact_filters() -> None:
    message = subscribe_message("BTC/USD")

    assert message == {
        "action": "subscribe",
        "subscriptions": [{
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "filters": '{"symbol":"btc/usd"}',
        }],
    }


def test_price_ticks_from_initial_batch_message() -> None:
    ticks = price_ticks_from_message({
        "payload": {
            "data": [
                {"timestamp": 1777993859000, "value": 81431.54814901785},
                {"timestamp": 1777993860000, "value": "81433.46390443733"},
            ]
        }
    })

    assert ticks == [
        (1777993859.0, 81431.54814901785),
        (1777993860.0, 81433.46390443733),
    ]


def test_price_ticks_from_live_update_message() -> None:
    ticks = price_ticks_from_message({
        "payload": {
            "timestamp": 1777993861000,
            "value": 81436.25,
        }
    })

    assert ticks == [(1777993861.0, 81436.25)]


def test_price_ticks_ignores_non_price_messages() -> None:
    assert price_ticks_from_message({"event": "subscribed"}) == []
    assert price_ticks_from_message({"payload": {"data": [{"timestamp": None, "value": "bad"}]}}) == []


def test_feed_inject_handles_append_update_and_out_of_order_ticks() -> None:
    feed = PolymarketChainlinkBtcPriceFeed()

    feed._inject(100.0, 10.0)
    feed._inject(101.0, 11.0)
    feed._inject(99.0, 9.0)
    feed._inject(101.0, 11.5)

    assert feed.price_at_or_before(99.0) == 9.0
    assert feed.first_price_at_or_after(100.0) == 10.0
    assert feed.latest_price == 11.5


def test_feed_defaults_to_fast_stale_reconnect() -> None:
    feed = PolymarketChainlinkBtcPriceFeed()

    assert feed.stale_reconnect_sec == 5.0
    assert feed.max_history_sec == 15.0


@pytest.mark.asyncio
async def test_feed_reconnects_when_messages_have_no_valid_ticks(monkeypatch, caplog) -> None:
    instances = []

    class EmptyPriceWs:
        def __init__(self) -> None:
            self.closed = False
            instances.append(self)

        async def send(self, _message: str) -> None:
            return None

        async def recv(self) -> str:
            await asyncio.sleep(0.05)
            return '{"event":"subscribed"}'

        async def close(self) -> None:
            self.closed = True

    async def fake_connect(*_args, **_kwargs):
        return EmptyPriceWs()

    caplog.set_level(logging.WARNING, logger="new_poly.market.polymarket_live")
    monkeypatch.setattr(polymarket_live.websockets, "connect", fake_connect)
    feed = PolymarketChainlinkBtcPriceFeed(stale_reconnect_sec=1.0)

    await feed.start()
    try:
        async with asyncio.timeout(3.5):
            while len(instances) < 2:
                await asyncio.sleep(0.05)
    finally:
        await feed.stop()

    assert instances[0].closed is True
    assert not [record for record in caplog.records if "reconnecting" in record.getMessage()]
