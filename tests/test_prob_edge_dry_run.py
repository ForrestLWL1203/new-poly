from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "prob_edge_dry_run.py"
spec = importlib.util.spec_from_file_location("prob_edge_dry_run", SCRIPT)
dry_run = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(dry_run)


def test_parse_tokens_accepts_json_string_and_list() -> None:
    assert dry_run.parse_tokens('["up","down"]') == ["up", "down"]
    assert dry_run.parse_tokens(["up", "down"]) == ["up", "down"]


def test_black_scholes_probabilities_are_complementary() -> None:
    up, down = dry_run.binary_probs(s_price=100_100, k_price=100_000, sigma_eff=0.6, remaining_sec=120)

    assert 0 < up < 1
    assert 0 < down < 1
    assert up > down
    assert abs((up + down) - 1.0) < 1e-12


def test_avg_price_for_notional_stops_when_target_is_filled() -> None:
    levels = [(0.40, 10.0), (0.42, 20.0), (0.60, 1000.0)]

    result = dry_run.avg_price_for_notional(levels, target_notional=8.0)

    assert result["ok"] is True
    assert result["avg"] == 0.409756
    assert result["shares"] == 19.52381
    assert result["notional"] == 8.0


def test_avg_price_for_notional_reports_insufficient_depth() -> None:
    result = dry_run.avg_price_for_notional([(0.50, 2.0)], target_notional=5.0)

    assert result["ok"] is False
    assert result["avg"] == 0.5
    assert result["shares"] == 2.0
    assert result["notional"] == 1.0


def test_phase_for_window() -> None:
    assert dry_run.phase_for_window(age_sec=-1, remaining_sec=301) == "warmup"
    assert dry_run.phase_for_window(age_sec=10, remaining_sec=290) == "warmup"
    assert dry_run.phase_for_window(age_sec=24, remaining_sec=276) == "warmup"
    assert dry_run.phase_for_window(age_sec=25, remaining_sec=275) == "early"
    assert dry_run.phase_for_window(age_sec=60, remaining_sec=240) == "early"
    assert dry_run.phase_for_window(age_sec=180, remaining_sec=120) == "core"
    assert dry_run.phase_for_window(age_sec=250, remaining_sec=50) == "late"
    assert dry_run.phase_for_window(age_sec=275, remaining_sec=25) == "no_entry"
    assert dry_run.phase_for_window(age_sec=301, remaining_sec=-1) == "closed"


def test_chainlink_resolution_validator() -> None:
    assert dry_run.is_chainlink_btc_resolution("https://data.chain.link/streams/btc-usd")
    assert dry_run.is_chainlink_btc_resolution(
        "The resolution source is Chainlink BTC/USD data stream."
    )
    assert not dry_run.is_chainlink_btc_resolution("https://example.com/btc")


def test_compact_row_contains_stable_schema_and_no_secrets() -> None:
    row = dry_run.build_log_row(
        market={
            "slug": "btc-updown-5m-1",
            "start": dry_run.dt.datetime(2026, 5, 3, 0, 0, tzinfo=dry_run.dt.timezone.utc),
            "end": dry_run.dt.datetime(2026, 5, 3, 0, 5, tzinfo=dry_run.dt.timezone.utc),
            "resolution_source": "https://data.chain.link/streams/btc-usd",
        },
        now=dry_run.dt.datetime(2026, 5, 3, 0, 1, tzinfo=dry_run.dt.timezone.utc),
        order_notional=5.0,
        sigma_source="manual",
        sigma_eff=0.6,
        price_state=dry_run.PriceState(source="missing"),
        up_state=dry_run.TokenBookState(token_id="up"),
        down_state=dry_run.TokenBookState(token_id="down"),
        required_edge=0.07,
        edge_components={"base": 0.07, "fee": 0.0},
    )

    expected = {
        "ts",
        "market_slug",
        "window_start",
        "window_end",
        "age_sec",
        "remaining_sec",
        "phase",
        "resolution_source",
        "settlement_aligned",
        "live_ready",
        "sigma_source",
        "sigma_eff",
        "price_source",
        "s_price",
        "k_price",
        "basis_bps",
        "up_prob",
        "down_prob",
        "required_edge",
        "edge_components",
        "order_notional",
        "up",
        "down",
        "yes_no_sum",
        "decision",
        "candidate_side",
        "skip_reason",
    }
    assert expected <= row.keys()
    assert row["settlement_aligned"] is False
    assert row["live_ready"] is False
    assert row["skip_reason"] in {"missing_chainlink_price", "paper_proxy_only", "missing_k"}
    assert "private" not in str(row).lower()


def test_extract_crypto_prices_from_hydration_html() -> None:
    html = """
    <script>
    {"dehydratedAt":1,"state":{"data":{"openPrice":78417.388005,"closePrice":78461.2}},
    "queryKey":["crypto-prices","price","BTC","2026-05-02T17:45:00Z","fiveminute","2026-05-02T17:50:00Z"]}
    </script>
    """

    result = dry_run.extract_crypto_prices_from_html(
        html,
        start_iso="2026-05-02T17:45:00Z",
        end_iso="2026-05-02T17:50:00Z",
    )

    assert result == {"openPrice": 78417.388005, "closePrice": 78461.2}


def test_basis_adjusted_price_state_uses_polymarket_k_and_binance_basis() -> None:
    shared = dry_run.PriceState(
        source="proxy_binance",
        binance_price=100_120.0,
        binance_updated_at=123.0,
    )

    state = dry_run.basis_adjusted_price_state(
        shared,
        k_price=100_000.0,
        binance_open_price=100_050.0,
    )

    assert state.source == "proxy_binance_basis_adjusted"
    assert state.k_price == 100_000.0
    assert state.s_price == 100_070.0
    assert state.basis_bps == 5.0


def test_price_state_falls_back_to_live_binance_when_open_basis_missing() -> None:
    shared = dry_run.PriceState(
        source="proxy_binance",
        binance_price=100_120.0,
        binance_updated_at=123.0,
    )

    state = dry_run.basis_adjusted_price_state(
        shared,
        k_price=100_000.0,
        binance_open_price=None,
    )

    assert state.source == "proxy_binance"
    assert state.k_price == 100_000.0
    assert state.s_price == 100_120.0
    assert state.basis_bps is None


def test_boundary_open_sampler_prefers_first_trade_at_or_after_start() -> None:
    start = dry_run.dt.datetime.fromtimestamp(1_000, dry_run.dt.timezone.utc)
    sampler = dry_run.BoundaryOpenSampler()
    sampler.set_target(start)

    sampler.record_trade(event_ts_ms=999_800, price=99.8, recv_mono=1.0)
    sampler.record_trade(event_ts_ms=1_000_020, price=100.02, recv_mono=2.0)
    sampler.record_trade(event_ts_ms=1_000_100, price=100.10, recv_mono=3.0)

    result = sampler.open_price()

    assert result is not None
    assert result["price"] == 100.02
    assert result["source"] == "ws_first_after"
    assert result["delta_ms"] == 20


def test_boundary_open_sampler_uses_last_trade_before_start_when_no_after() -> None:
    start = dry_run.dt.datetime.fromtimestamp(1_000, dry_run.dt.timezone.utc)
    sampler = dry_run.BoundaryOpenSampler()
    sampler.set_target(start)

    sampler.record_trade(event_ts_ms=994_999, price=94.999, recv_mono=1.0)
    sampler.record_trade(event_ts_ms=999_700, price=99.7, recv_mono=2.0)
    sampler.record_trade(event_ts_ms=999_950, price=99.95, recv_mono=3.0)

    result = sampler.open_price()

    assert result is not None
    assert result["price"] == 99.95
    assert result["source"] == "ws_last_before"
    assert result["delta_ms"] == -50


def test_window_tracker_stops_after_n_distinct_windows() -> None:
    tracker = dry_run.WindowLimitTracker(limit=2)

    assert tracker.observe("btc-updown-5m-1") is False
    assert tracker.observe("btc-updown-5m-1") is False
    assert tracker.observe("btc-updown-5m-2") is False
    assert tracker.observe("btc-updown-5m-2") is False
    assert tracker.observe("btc-updown-5m-3") is True


def test_window_tracker_does_not_count_skipped_windows() -> None:
    tracker = dry_run.WindowLimitTracker(limit=2)

    assert tracker.observe("btc-updown-5m-late", count=False) is False
    assert tracker.observe("btc-updown-5m-1") is False
    assert tracker.observe("btc-updown-5m-2") is False
    assert tracker.observe("btc-updown-5m-3") is True
    assert tracker.seen == ["btc-updown-5m-1", "btc-updown-5m-2", "btc-updown-5m-3"]


def test_window_tracker_without_limit_never_stops() -> None:
    tracker = dry_run.WindowLimitTracker(limit=None)

    assert tracker.observe("btc-updown-5m-1") is False
    assert tracker.observe("btc-updown-5m-2") is False


def test_candidate_epochs_start_from_next_window_by_default() -> None:
    now = dry_run.dt.datetime.fromtimestamp(1777749001, dry_run.dt.timezone.utc)

    epochs = dry_run.candidate_btc_5m_epochs(now, max_windows=3)

    assert epochs == [1777749300, 1777749600, 1777749900]


def test_candidate_epochs_can_include_current_window_for_diagnostics() -> None:
    now = dry_run.dt.datetime.fromtimestamp(1777749001, dry_run.dt.timezone.utc)

    epochs = dry_run.candidate_btc_5m_epochs(now, max_windows=3, include_current=True)

    assert epochs == [1777749000, 1777749300, 1777749600]


def test_candidate_epochs_can_require_newer_than_old_start() -> None:
    now = dry_run.dt.datetime.fromtimestamp(1777749001, dry_run.dt.timezone.utc)
    old_start = dry_run.dt.datetime.fromtimestamp(1777749000, dry_run.dt.timezone.utc)

    epochs = dry_run.candidate_btc_5m_epochs(now, max_windows=3, min_start=old_start)

    assert epochs == [1777749300, 1777749600, 1777749900]


def test_sanitize_next_market_rejects_same_or_older_window() -> None:
    current = {"slug": "btc-updown-5m-1000", "start": dry_run.dt.datetime.fromtimestamp(1000, dry_run.dt.timezone.utc)}
    same = {"slug": "btc-updown-5m-1000", "start": dry_run.dt.datetime.fromtimestamp(1000, dry_run.dt.timezone.utc)}
    older = {"slug": "btc-updown-5m-700", "start": dry_run.dt.datetime.fromtimestamp(700, dry_run.dt.timezone.utc)}
    newer = {"slug": "btc-updown-5m-1300", "start": dry_run.dt.datetime.fromtimestamp(1300, dry_run.dt.timezone.utc)}

    assert dry_run.sanitize_next_market(current, same) is None
    assert dry_run.sanitize_next_market(current, older) is None
    assert dry_run.sanitize_next_market(current, newer) is newer


def test_should_prefetch_next_market_once_near_window_end() -> None:
    assert dry_run.should_prefetch_next_market(remaining_sec=31.0, has_task=False) is False
    assert dry_run.should_prefetch_next_market(remaining_sec=30.0, has_task=False) is True
    assert dry_run.should_prefetch_next_market(remaining_sec=10.0, has_task=True) is False


def test_k_retry_schedule_starts_at_25s_then_every_5s_until_40s() -> None:
    state = dry_run.KRetryState()

    assert dry_run.should_retry_k_price(state, age_sec=24.9) is False
    assert dry_run.should_retry_k_price(state, age_sec=25.0) is True
    state.record_attempt(25.0)
    assert dry_run.should_retry_k_price(state, age_sec=30.0) is True
    state.record_attempt(30.0)
    assert dry_run.should_retry_k_price(state, age_sec=35.0) is True
    state.record_attempt(35.0)
    assert dry_run.should_retry_k_price(state, age_sec=40.0) is True
    state.record_attempt(40.0)
    assert state.timed_out is False
    assert dry_run.should_retry_k_price(state, age_sec=45.0) is False
    assert state.timed_out is True


def test_missing_window_k_retries_and_preserves_binance_open(monkeypatch) -> None:
    market = {
        "slug": "btc-updown-5m-1",
        "start": dry_run.dt.datetime(2026, 5, 3, 0, 0, tzinfo=dry_run.dt.timezone.utc),
        "end": dry_run.dt.datetime(2026, 5, 3, 0, 5, tzinfo=dry_run.dt.timezone.utc),
    }
    state = dry_run.WindowPriceState(binance_open_price=100_050.0)

    async def fake_fetch_window_prices(_market):
        return dry_run.WindowPriceState(
            k_price=100_000.0,
            close_price=None,
            k_source="polymarket_html_crypto_prices",
        )

    monkeypatch.setattr(dry_run, "fetch_window_prices", fake_fetch_window_prices)

    updated, last_attempt = asyncio.run(
        dry_run.refresh_missing_window_prices(
            market,
            state,
            retry_state=dry_run.KRetryState(),
            age_sec=25.0,
        )
    )

    assert updated.k_price == 100_000.0
    assert updated.k_source == "polymarket_html_crypto_prices"
    assert updated.binance_open_price == 100_050.0
    assert last_attempt.last_attempt_age == 25.0
