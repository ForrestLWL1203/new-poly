from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.backtest.prob_edge_replay import BacktestConfig, run_backtest, scan_configs


def _row(slug: str, age: int, s_price: float, k_price: float = 100.0) -> dict:
    return {
        "market_slug": slug,
        "age_sec": age,
        "remaining_sec": 300 - age,
        "s_price": s_price,
        "k_price": k_price,
        "volatility": {"sigma": 0.6},
        "up": {
            "ask": 0.40,
            "ask_avg": 0.40,
            "ask_limit": 0.40,
            "ask_depth_ok": True,
            "bid_avg": 0.40,
            "bid_limit": 0.40,
            "bid_depth_ok": True,
            "book_age_ms": 20.0,
        },
        "down": {
            "ask": 0.80,
            "ask_avg": 0.80,
            "ask_limit": 0.80,
            "ask_depth_ok": True,
            "bid_avg": 0.20,
            "bid_limit": 0.20,
            "bid_depth_ok": True,
            "book_age_ms": 20.0,
        },
    }


def test_backtest_enters_and_settles_open_position() -> None:
    rows = [
        _row("m1", 60, 100.10),
        _row("m1", 120, 101.00),
        _row("m1", 240, 101.00),
    ]

    result = run_backtest(rows, BacktestConfig(amount_usd=10.0))

    assert result.summary["windows"] == 1
    assert result.summary["entries"] == 1
    assert result.summary["settlements"] == 1
    assert result.summary["total_pnl"] > 0
    assert result.trades[0]["entry_side"] == "up"


def test_scan_configs_returns_sorted_results() -> None:
    rows = [
        _row("m1", 60, 100.10),
        _row("m1", 299, 101.00),
    ]

    results = scan_configs(rows, early_edges=[0.08, 0.10], core_edges=[0.06], entry_starts=[40], entry_ends=[240])

    assert len(results) == 2
    assert results[0]["total_pnl"] >= results[1]["total_pnl"]
    assert {"early_required_edge", "core_required_edge", "entry_start_age_sec", "entry_end_age_sec"} <= set(results[0])


def test_backtest_applies_execution_slippage_ticks() -> None:
    rows = [
        _row("m1", 90, 100.10),
        _row("m1", 120, 100.00),
    ]
    rows[0]["up"]["ask_avg"] = 0.39
    rows[0]["up"]["ask_limit"] = 0.40
    rows[1]["up"]["bid_avg"] = 0.70
    rows[1]["up"]["bid_limit"] = 0.68

    result = run_backtest(rows, BacktestConfig(amount_usd=10.0, buy_slippage_ticks=1, sell_slippage_ticks=1))

    assert result.trades[0]["entry_price"] == 0.41
    assert result.trades[0]["exit_price"] == 0.67
