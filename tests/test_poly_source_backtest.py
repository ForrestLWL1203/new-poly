from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.backtest.poly_source_replay import BacktestConfig, run_backtest, scan_poly_source_configs
from scripts.backtest_poly_source import _amount_bucket


def _poly_row(
    slug: str,
    age: int,
    *,
    poly_price: float,
    k_price: float = 100.0,
    poly_return_3s: float = 0.4,
    up_ask: float = 0.60,
    down_ask: float = 0.60,
    up_bid: float = 0.59,
    down_bid: float = 0.59,
    final_s_price: float | None = None,
) -> dict:
    price = final_s_price if final_s_price is not None else poly_price
    return {
        "market_slug": slug,
        "age_sec": age,
        "remaining_sec": 300 - age,
        "s_price": price,
        "k_price": k_price,
        "polymarket_price": poly_price,
        "lead_polymarket_return_1s_bps": poly_return_3s,
        "lead_polymarket_return_3s_bps": poly_return_3s,
        "lead_polymarket_return_5s_bps": poly_return_3s,
        "up": {"ask": up_ask, "ask_avg": up_ask, "ask_limit": up_ask, "bid_avg": up_bid, "bid_limit": up_bid, "bid_depth_ok": True, "book_age_ms": 20.0},
        "down": {"ask": down_ask, "ask_avg": down_ask, "ask_limit": down_ask, "bid_avg": down_bid, "bid_limit": down_bid, "bid_depth_ok": True, "book_age_ms": 20.0},
    }


def test_poly_single_source_backtest_uses_poly_direction_and_reports_settle_only() -> None:
    rows = [
        _poly_row("m1", 100, poly_price=99.98, poly_return_3s=-0.4, down_ask=0.60, up_ask=0.80, final_s_price=99.0),
        _poly_row("m1", 299, poly_price=99.90, poly_return_3s=-0.1, down_ask=0.20, down_bid=0.80, final_s_price=99.0),
        _poly_row("m2", 100, poly_price=100.02, poly_return_3s=0.4, up_ask=0.60, down_ask=0.80, final_s_price=99.0),
        _poly_row("m2", 130, poly_price=99.97, poly_return_3s=-0.4, up_ask=0.60, up_bid=0.45, final_s_price=99.0),
    ]

    result = run_backtest(rows, BacktestConfig(amount_usd=1.0))

    assert result.summary["strategy_mode"] == "poly_single_source"
    assert result.summary["entries"] == 2
    assert result.summary["direction_accuracy"] == 0.5
    assert result.summary["side_counts"] == {"down": 1, "up": 1}
    assert result.summary["settle_only_total_pnl"] == pytest.approx(0.666667 - 1.0)
    assert result.summary["exit_reason_counts"]["settlement"] == 2
    assert result.trades[0]["entry_side"] == "down"
    assert result.trades[0]["poly_entry_score"] is not None


def test_backtest_settlement_prefers_window_close_open_over_last_tick_price() -> None:
    rows = [
        _poly_row("m1", 120, poly_price=100.03, poly_return_3s=0.4, up_ask=0.60, final_s_price=99.0),
        _poly_row("m1", 299, poly_price=100.10, poly_return_3s=0.1, up_bid=0.90, final_s_price=99.0),
        {
            "event": "window_settlement",
            "market_slug": "m1",
            "age_sec": 300,
            "settlement_open_price": 100.0,
            "settlement_close_price": 101.0,
            "winning_side": "up",
            "settlement_uncertain": False,
        },
    ]

    result = run_backtest(rows, BacktestConfig(amount_usd=1.0, entry_start_age_sec=120.0))

    assert result.summary["settlements"] == 1
    assert result.summary["direction_accuracy"] == 1.0
    assert result.trades[0]["winning_side"] == "up"
    assert result.trades[0]["settlement_price"] == 101.0
    assert result.trades[0]["settlement_k_price"] == 100.0


def test_scan_poly_source_configs_returns_ranked_parameter_results() -> None:
    rows = [
        _poly_row("m1", 100, poly_price=100.02, poly_return_3s=0.4, up_ask=0.60, final_s_price=101.0),
        _poly_row("m1", 299, poly_price=100.10, poly_return_3s=0.1, up_bid=0.90, final_s_price=101.0),
    ]

    results = scan_poly_source_configs(
        rows,
        reference_distances=[0.5, 3.0],
        trend_lookbacks=[3.0],
        return_thresholds=[0.3],
        max_entry_asks=[0.65],
        min_scores=[0.0],
        base_config=BacktestConfig(amount_usd=1.0),
    )

    assert len(results) == 2
    assert results[0]["poly_reference_distance_bps"] == 0.5
    assert results[0]["entries"] == 1
    assert results[0]["total_pnl"] >= results[1]["total_pnl"]


def test_scan_poly_source_configs_can_cap_reference_distance() -> None:
    rows = [
        _poly_row("m1", 120, poly_price=100.05, poly_return_3s=0.8, up_ask=0.60, final_s_price=101.0),
        _poly_row("m1", 150, poly_price=100.02, poly_return_3s=0.8, up_ask=0.60, final_s_price=101.0),
        _poly_row("m1", 299, poly_price=100.10, poly_return_3s=0.1, up_bid=0.90, final_s_price=101.0),
    ]

    results = scan_poly_source_configs(
        rows,
        reference_distances=[0.5],
        max_reference_distances=[4.0],
        trend_lookbacks=[3.0],
        return_thresholds=[0.3],
        max_entry_asks=[0.65],
        min_scores=[0.0],
        base_config=BacktestConfig(amount_usd=1.0, entry_start_age_sec=120.0),
    )

    assert results[0]["max_poly_reference_distance_bps"] == 4.0
    assert results[0]["entries"] == 1
    assert results[0]["total_pnl"] == pytest.approx(0.666667)


def test_poly_single_source_backtest_accepts_compact_tick_rows() -> None:
    rows = [
        _poly_row("m1", 120, poly_price=100.02, poly_return_3s=0.4, up_ask=0.60, final_s_price=101.0),
        _poly_row("m1", 299, poly_price=100.10, poly_return_3s=0.1, up_bid=0.90, final_s_price=101.0),
    ]
    for row in rows:
        row.pop("event", None)
        row.pop("mode", None)
        row.pop("polymarket_price_age_sec", None)
        row.pop("lead_polymarket_side", None)
        for side in ("up", "down"):
            row[side].pop("ask_avg", None)
            row[side].pop("ask_limit", None)
            row[side].pop("bid_age_ms", None)
            row[side].pop("stable_depth_usd", None)

    result = run_backtest(rows, BacktestConfig(amount_usd=1.0, entry_start_age_sec=120.0))

    assert result.summary["entries"] == 1
    assert result.trades[0]["entry_side"] == "up"


def test_poly_single_source_backtest_computes_configured_lookback_from_history() -> None:
    rows = [
        _poly_row("m1", 100, poly_price=100.00, poly_return_3s=0.0, up_ask=0.60, final_s_price=101.0),
        _poly_row("m1", 110, poly_price=100.02, poly_return_3s=0.0, up_ask=0.60, final_s_price=101.0),
        _poly_row("m1", 299, poly_price=100.10, poly_return_3s=0.0, up_bid=0.90, final_s_price=101.0),
    ]
    for row in rows:
        row.pop("lead_polymarket_return_10s_bps", None)

    result = run_backtest(
        rows,
        BacktestConfig(
            amount_usd=1.0,
            entry_start_age_sec=100.0,
            poly_trend_lookback_sec=10.0,
            poly_return_bps=0.1,
        ),
    )

    assert result.summary["entries"] == 1
    assert result.trades[0]["poly_trend_lookback_sec"] == 10.0
    assert result.trades[0]["poly_return_bps"] == pytest.approx(2.0, abs=0.01)
    assert result.trades[0]["poly_return_since_entry_start_bps"] == pytest.approx(2.0, abs=0.01)


def test_backtest_entry_phase_uses_poly_entry_phase() -> None:
    rows = [
        _poly_row("m1", 80, poly_price=100.02, up_ask=0.60, final_s_price=101.0),
        _poly_row("m1", 120, poly_price=100.02, up_ask=0.60, final_s_price=101.0),
    ]

    result = run_backtest(rows, BacktestConfig(entry_start_age_sec=90.0))

    assert result.summary["entries"] == 1
    assert result.trades[0]["entry_age_sec"] == 120
    assert result.summary["skip_reason_counts"]["outside_entry_time"] == 1


def test_backtest_honored_entry_events_use_logged_amount_when_size_missing() -> None:
    rows = [
        {
            "event": "entry",
            "market_slug": "m1",
            "age_sec": 120,
            "remaining_sec": 180,
            "amount_usd": 3.0,
            "analysis": {
                "entry_side": "up",
                "entry_price": 0.60,
                "entry_poly_reference_distance_bps": 2.0,
            },
            "order": {"success": True, "avg_price": 0.60},
        },
        {
            "event": "window_settlement",
            "market_slug": "m1",
            "age_sec": 300,
            "settlement_open_price": 100.0,
            "settlement_close_price": 101.0,
            "winning_side": "up",
            "settlement_uncertain": False,
        },
    ]

    result = run_backtest(rows, BacktestConfig(amount_usd=1.0, honor_order_events=True))

    assert result.trades[0]["entry_amount_usd"] == 3.0
    assert result.trades[0]["shares"] == pytest.approx(5.0)
    assert result.trades[0]["pnl"] == pytest.approx(2.0)


def test_backtest_amount_bucket_uses_actual_usd_amounts() -> None:
    assert _amount_bucket(None) == "unknown"
    assert _amount_bucket(1.0) == "$1"
    assert _amount_bucket(3.0) == "$3"
    assert _amount_bucket(5.0) == "$5"
    assert _amount_bucket(2.75) == "$2.75"
