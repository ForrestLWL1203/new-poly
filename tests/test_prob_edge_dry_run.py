from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "collect_prob_edge_data.py"
sys.path.insert(0, str(SCRIPT.parents[1]))
spec = importlib.util.spec_from_file_location("collect_prob_edge_data", SCRIPT)
collector = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = collector
spec.loader.exec_module(collector)


def test_extract_crypto_prices_from_hydration_html() -> None:
    html = """
    <script>
    {"dehydratedAt":1,"state":{"data":{"openPrice":78417.388005,"closePrice":78461.2}},
    "queryKey":["crypto-prices","price","BTC","2026-05-02T17:45:00Z","fiveminute","2026-05-02T17:50:00Z"]}
    </script>
    """

    result = collector.extract_crypto_prices_from_html(
        html,
        start_iso="2026-05-02T17:45:00Z",
        end_iso="2026-05-02T17:50:00Z",
    )

    assert result == {"openPrice": 78417.388005, "closePrice": 78461.2}


def test_window_bucket() -> None:
    assert collector.window_bucket(age_sec=-1, remaining_sec=301) == "warmup"
    assert collector.window_bucket(age_sec=25, remaining_sec=275) == "early"
    assert collector.window_bucket(age_sec=180, remaining_sec=120) == "core"
    assert collector.window_bucket(age_sec=250, remaining_sec=50) == "late"
    assert collector.window_bucket(age_sec=275, remaining_sec=25) == "no_entry"
    assert collector.window_bucket(age_sec=301, remaining_sec=-1) == "closed"


def test_avg_price_for_notional() -> None:
    avg, ok, notional = collector.avg_price_for_notional([(0.4, 10), (0.42, 20)], 8.0)

    assert ok is True
    assert avg == 0.409756
    assert notional == 8.0


def test_window_tracker_counts_only_valid_windows() -> None:
    tracker = collector.WindowLimitTracker(limit=2)

    assert tracker.observe("late-window", count=False) is False
    assert tracker.observe("w1", count=True) is False
    assert tracker.observe("w2", count=True) is False
    assert tracker.observe("w3", count=True) is True


def test_initial_window_defaults_to_next_full_window(monkeypatch) -> None:
    current = collector.MarketWindow(
        question="current",
        up_token="up1",
        down_token="down1",
        start_time=collector.dt.datetime(2026, 5, 3, 0, 0, tzinfo=collector.dt.timezone.utc),
        end_time=collector.dt.datetime(2026, 5, 3, 0, 5, tzinfo=collector.dt.timezone.utc),
        slug="btc-updown-5m-1",
    )
    following = collector.MarketWindow(
        question="following",
        up_token="up2",
        down_token="down2",
        start_time=collector.dt.datetime(2026, 5, 3, 0, 5, tzinfo=collector.dt.timezone.utc),
        end_time=collector.dt.datetime(2026, 5, 3, 0, 10, tzinfo=collector.dt.timezone.utc),
        slug="btc-updown-5m-2",
    )

    monkeypatch.setattr(collector, "find_next_window", lambda series: current)
    monkeypatch.setattr(collector, "find_following_window", lambda window, series: following)

    selected = collector.find_initial_window(
        collector.MarketSeries.from_known("btc-updown-5m"),
        include_current=False,
        now=collector.dt.datetime(2026, 5, 3, 0, 1, tzinfo=collector.dt.timezone.utc),
    )

    assert selected is following


def test_collector_row_is_strategy_neutral() -> None:
    class FakeFeed:
        latest_price = 100_120.0

    class FakeStream:
        def get_latest_ask_levels_with_size(self, token_id):
            return [(0.51, 100.0)] if token_id == "up" else [(0.49, 100.0)]

        def get_latest_bid_levels_with_size(self, token_id):
            return [(0.50, 100.0)] if token_id == "up" else [(0.48, 100.0)]

        def get_latest_best_bid(self, token_id):
            return self.get_latest_bid_levels_with_size(token_id)[0][0]

        def get_latest_best_ask(self, token_id):
            return self.get_latest_ask_levels_with_size(token_id)[0][0]

        def get_latest_best_ask_age(self, token_id):
            return 0.25

    window = collector.MarketWindow(
        question="Bitcoin Up or Down",
        up_token="up",
        down_token="down",
        start_time=collector.dt.datetime(2026, 5, 3, 0, 0, tzinfo=collector.dt.timezone.utc),
        end_time=collector.dt.datetime(2026, 5, 3, 0, 5, tzinfo=collector.dt.timezone.utc),
        slug="btc-updown-5m-1",
        resolution_source="https://data.chain.link/streams/btc-usd",
    )
    prices = collector.WindowPrices(k_price=100_000.0, binance_open_price=100_050.0, k_source="polymarket_html_crypto_prices")

    row = collector.build_row(
        window=window,
        prices=prices,
        feed=FakeFeed(),
        stream=FakeStream(),
        now=collector.dt.datetime(2026, 5, 3, 0, 1, tzinfo=collector.dt.timezone.utc),
        depth_notional=5.0,
        sigma_eff=0.6,
        sigma_source="manual",
        paired_buffer=0.01,
        volatility=collector.DvolSnapshot(
            source="deribit_dvol",
            currency="BTC",
            dvol=40.0,
            sigma=0.4,
            timestamp_ms=1_000_000,
            fetched_at=1_000.0,
        ),
    )

    assert row["price_source"] == "proxy_binance_basis_adjusted"
    assert row["resolution_source"] == "https://data.chain.link/streams/btc-usd"
    assert row["s_price"] == 100070.0
    assert row["basis_bps"] == 5.0
    assert row["volatility"]["sigma"] == 0.4
    for forbidden in ("decision", "candidate_side", "skip_reason", "required_edge", "edge_components", "up_prob", "down_prob"):
        assert forbidden not in row
    assert "edge" not in row["up"]
    assert "private" not in json.dumps(row).lower()
