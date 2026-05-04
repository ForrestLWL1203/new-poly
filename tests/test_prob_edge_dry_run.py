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


def test_extract_crypto_prices_from_api_response() -> None:
    data = collector.extract_crypto_prices_from_api_response({
        "openPrice": "78409.37",
        "closePrice": 78379.9,
        "completed": True,
        "incomplete": False,
        "cached": True,
    })

    assert data == {
        "openPrice": 78409.37,
        "completed": True,
        "incomplete": False,
        "cached": True,
    }


def test_api_url_uses_exact_window_iso() -> None:
    window = collector.MarketWindow(
        question="Bitcoin Up or Down",
        up_token="up",
        down_token="down",
        start_time=collector.dt.datetime(2026, 5, 3, 8, 55, tzinfo=collector.dt.timezone.utc),
        end_time=collector.dt.datetime(2026, 5, 3, 9, 0, tzinfo=collector.dt.timezone.utc),
        slug="btc-updown-5m-1777798500",
    )

    url = collector.crypto_price_api_url(window)

    assert "symbol=BTC" in url
    assert "eventStartTime=2026-05-03T08%3A55%3A00Z" in url
    assert "endDate=2026-05-03T09%3A00%3A00Z" in url
    assert "variant=fiveminute" in url


def test_window_bucket() -> None:
    assert collector.window_bucket(age_sec=-1, remaining_sec=301) == "warmup"
    assert collector.window_bucket(age_sec=25, remaining_sec=275) == "early"
    assert collector.window_bucket(age_sec=180, remaining_sec=120) == "core"
    assert collector.window_bucket(age_sec=250, remaining_sec=50) == "late"
    assert collector.window_bucket(age_sec=275, remaining_sec=25) == "no_entry"
    assert collector.window_bucket(age_sec=301, remaining_sec=-1) == "closed"


def test_avg_price_for_notional() -> None:
    avg, ok, notional, limit_price = collector.avg_price_for_notional([(0.4, 10), (0.42, 20)], 8.0)

    assert ok is True
    assert avg == 0.409756
    assert notional == 8.0
    assert limit_price == 0.42


def test_token_state_uses_safety_multiplier_without_changing_trade_average() -> None:
    class FakeStream:
        def get_latest_ask_levels_with_size(self, token_id):
            return [(0.50, 2.0), (0.52, 1.0)]

        def get_latest_bid_levels_with_size(self, token_id):
            return [(0.49, 10.0)]

        def get_latest_best_bid(self, token_id):
            return 0.49

        def get_latest_best_ask(self, token_id):
            return 0.50

        def get_latest_best_ask_age(self, token_id):
            return 0.01

    state = collector.token_state(FakeStream(), "up", depth_notional=1.0, depth_safety_multiplier=1.5)

    assert state["ask_avg"] == 0.5
    assert state["ask_limit"] == 0.5
    assert state["ask_safety_limit"] == 0.52
    assert state["ask_depth_ok"] is True


def test_window_tracker_counts_only_valid_windows() -> None:
    tracker = collector.WindowLimitTracker(limit=2)

    assert tracker.observe("late-window", count=False) is False
    assert tracker.reached() is False
    assert tracker.observe("w1", count=True) is False
    assert tracker.reached() is False
    assert tracker.observe("w2", count=True) is False
    assert tracker.reached() is True
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


def test_binance_open_waits_until_lookaround_window() -> None:
    class FakeFeed:
        latest_price = 100.0
        calls = 0

        def first_price_at_or_after(self, *args, **kwargs):
            self.calls += 1
            return 100.0

        def price_at_or_before(self, *args, **kwargs):
            self.calls += 1
            return 99.0

    window = collector.MarketWindow(
        question="future",
        up_token="up",
        down_token="down",
        start_time=collector.dt.datetime(2026, 5, 3, 0, 5, tzinfo=collector.dt.timezone.utc),
        end_time=collector.dt.datetime(2026, 5, 3, 0, 10, tzinfo=collector.dt.timezone.utc),
        slug="btc-updown-5m-2",
    )
    prices = collector.WindowPrices()
    feed = FakeFeed()

    collector.asyncio.run(collector.refresh_binance_open(feed, window, prices, age_sec=-6.0))

    assert prices.binance_open_price is None
    assert feed.calls == 0


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
    prices = collector.WindowPrices(k_price=100_000.0, binance_open_price=100_050.0, k_source="polymarket_crypto_price_api")

    row = collector.build_row(
        window=window,
        prices=prices,
        feed=FakeFeed(),
        stream=FakeStream(),
        now=collector.dt.datetime(2026, 5, 3, 0, 1, tzinfo=collector.dt.timezone.utc),
        depth_notional=5.0,
        sigma_eff=0.6,
        sigma_source="manual",
        volatility_stale=False,
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
    assert row["volatility_stale"] is False
    assert row["up"]["ask_limit"] == 0.51
    assert "close_price" not in row
    for forbidden in ("decision", "candidate_side", "skip_reason", "required_edge", "edge_components", "up_prob", "down_prob"):
        assert forbidden not in row
    assert "edge" not in row["up"]
    assert "private" not in json.dumps(row).lower()


def test_collector_row_marks_stale_volatility() -> None:
    class FakeFeed:
        latest_price = 100_120.0

    class FakeStream:
        def get_latest_ask_levels_with_size(self, token_id):
            return [(0.51, 100.0)]

        def get_latest_bid_levels_with_size(self, token_id):
            return [(0.50, 100.0)]

        def get_latest_best_bid(self, token_id):
            return 0.50

        def get_latest_best_ask(self, token_id):
            return 0.51

        def get_latest_best_ask_age(self, token_id):
            return 0.25

    window = collector.MarketWindow(
        question="Bitcoin Up or Down",
        up_token="up",
        down_token="down",
        start_time=collector.dt.datetime(2026, 5, 3, 0, 0, tzinfo=collector.dt.timezone.utc),
        end_time=collector.dt.datetime(2026, 5, 3, 0, 5, tzinfo=collector.dt.timezone.utc),
        slug="btc-updown-5m-1",
    )

    row = collector.build_row(
        window=window,
        prices=collector.WindowPrices(k_price=100_000.0),
        feed=FakeFeed(),
        stream=FakeStream(),
        now=collector.dt.datetime(2026, 5, 3, 0, 1, tzinfo=collector.dt.timezone.utc),
        depth_notional=5.0,
        sigma_eff=None,
        sigma_source="missing",
        volatility_stale=True,
        paired_buffer=0.01,
        volatility=None,
    )

    assert row["sigma_source"] == "missing"
    assert row["sigma_eff"] is None
    assert row["volatility_stale"] is True
