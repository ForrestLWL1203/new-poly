from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.market.binance import BinancePriceFeed
from new_poly.market.deribit import DvolSnapshot
from new_poly.market.series import MarketSeries
from new_poly.market.stream import PriceStream


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
