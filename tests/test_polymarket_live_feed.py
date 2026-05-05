from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.market.polymarket_live import PolymarketChainlinkBtcPriceFeed, price_ticks_from_message


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
