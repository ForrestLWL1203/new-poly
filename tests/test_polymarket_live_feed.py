from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.market.polymarket_live import price_ticks_from_message


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
