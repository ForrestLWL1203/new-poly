from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.backtest.prob_edge_replay import BacktestConfig, run_backtest, scan_configs


def _row(slug: str, age: int, s_price: float | None, k_price: float | None = 100.0) -> dict:
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


def _hide_settlement_price(row: dict) -> None:
    row["s_price"] = None
    row["k_price"] = None


def _disable_exit_depth(row: dict) -> None:
    row["up"]["bid_depth_ok"] = False
    row["down"]["bid_depth_ok"] = False


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


def test_backtest_applies_buy_slippage_and_live_style_sell_floor() -> None:
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
    assert result.trades[0]["exit_price"] == 0.65


def test_backtest_sell_slippage_can_exceed_live_style_sell_floor() -> None:
    rows = [
        _row("m1", 90, 100.10),
        _row("m1", 120, 100.00),
    ]
    rows[0]["up"]["ask_avg"] = 0.39
    rows[0]["up"]["ask_limit"] = 0.40
    rows[1]["up"]["bid_avg"] = 0.70
    rows[1]["up"]["bid_limit"] = 0.68

    result = run_backtest(rows, BacktestConfig(amount_usd=10.0, sell_slippage_ticks=5))

    assert result.trades[0]["exit_price"] == 0.63


def test_backtest_entry_edge_matches_strategy_edge_and_records_fill_edge() -> None:
    rows = [
        _row("m1", 90, 100.10),
        _row("m1", 120, 100.00),
    ]
    rows[0]["up"]["ask_avg"] = 0.39
    rows[0]["up"]["ask_limit"] = 0.40

    result = run_backtest(rows, BacktestConfig(amount_usd=10.0))

    trade = result.trades[0]
    assert trade["entry_edge"] == trade["entry_model_prob"] - 0.39
    assert trade["entry_edge_at_fill"] == trade["entry_model_prob"] - trade["entry_price"]


def test_backtest_entry_phase_uses_strategy_entry_phase() -> None:
    rows = [
        _row("m1", 80, 100.10),
        _row("m1", 120, 101.00),
    ]

    result = run_backtest(rows, BacktestConfig(entry_start_age_sec=90.0))

    assert result.summary["entries"] == 1
    assert result.trades[0]["entry_age_sec"] == 120
    assert result.trades[0]["entry_phase"] == "core"
    assert result.summary["skip_reason_counts"]["outside_entry_time"] == 1


def test_backtest_records_unsettled_position_when_settlement_side_missing() -> None:
    rows = [
        _row("m1", 90, 100.10),
        _row("m1", 240, None, None),
    ]
    _hide_settlement_price(rows[1])

    result = run_backtest(rows, BacktestConfig(amount_usd=10.0))

    assert result.summary["entries"] == 1
    assert result.summary["unsettled"] == 1
    assert result.trades[0]["exit_reason"] == "unsettled_no_settlement_side"
    assert result.trades[0]["pnl"] == 0.0


def test_backtest_marks_boundary_uncertain_settlement() -> None:
    rows = [
        _row("m1", 90, 100.10),
        _row("m1", 299, 103.00),
    ]
    _disable_exit_depth(rows[1])

    result = run_backtest(rows, BacktestConfig(amount_usd=10.0, settlement_boundary_usd=5.0))

    assert result.summary["settlement_uncertain"] == 1
    assert result.trades[0]["settlement_uncertain"] is True


def test_backtest_treats_stale_volatility_as_missing_model_input() -> None:
    rows = [_row("m1", 90, 100.10)]
    rows[0]["volatility_stale"] = True

    result = run_backtest(rows, BacktestConfig())

    assert result.summary["entries"] == 0
    assert result.summary["skip_reason_counts"]["missing_model_inputs"] == 1


def test_scan_configs_filters_min_entries_and_can_sort_by_win_rate() -> None:
    rows = [
        _row("m1", 90, 100.10),
        _row("m1", 299, 101.00),
    ]

    results = scan_configs(
        rows,
        early_edges=[0.08, 0.50],
        core_edges=[0.06],
        entry_starts=[40],
        entry_ends=[240],
        min_entries=1,
        sort_by="win_rate",
    )

    assert len(results) == 1
    assert results[0]["entries"] >= 1


def test_backtest_config_passes_max_entries_to_strategy() -> None:
    cfg = BacktestConfig(max_entries_per_market=4)

    assert cfg.edge_config().max_entries_per_market == 4


def test_backtest_rejects_entry_when_safety_depth_exceeds_formula_cap() -> None:
    rows = [_row("m1", 120, 100.10)]
    rows[0]["up"]["ask_avg"] = 0.20
    rows[0]["up"]["ask_limit"] = 0.20
    rows[0]["up"]["ask_safety_limit"] = 0.99

    result = run_backtest(rows, BacktestConfig(core_required_edge=0.05))

    assert result.summary["entries"] == 0


def test_backtest_config_passes_fair_cap_margin_to_strategy() -> None:
    cfg = BacktestConfig(min_fair_cap_margin_ticks=1.0, tick_size=0.01)

    edge_cfg = cfg.edge_config()

    assert edge_cfg.min_fair_cap_margin_ticks == 1.0
    assert edge_cfg.entry_tick_size == 0.01
