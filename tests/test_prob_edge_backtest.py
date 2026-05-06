from __future__ import annotations

import sys
import math
from dataclasses import replace
from pathlib import Path

import pytest

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


def test_backtest_honor_order_events_keeps_partial_exit_open_until_final_exit() -> None:
    entry = _row("m1", 100, 101.0)
    entry.update({
        "event": "entry",
        "analysis": {"entry_side": "up", "entry_price": 0.10, "entry_model_prob": 0.70, "entry_edge_signal": 0.60},
        "order": {"success": True, "filled_size": 100.0, "avg_price": 0.10},
    })
    partial = _row("m1", 120, 101.0)
    partial.update({
        "event": "partial_exit",
        "exit_reason": "logic_decay_exit",
        "analysis": {"exit_price": 0.30},
        "order": {"success": True, "filled_size": 40.0, "avg_price": 0.30},
    })
    final = _row("m1", 140, 101.0)
    final.update({
        "event": "exit",
        "exit_reason": "logic_decay_exit",
        "analysis": {"exit_price": 0.20},
        "order": {"success": True, "filled_size": 60.0, "avg_price": 0.20},
    })

    result = run_backtest([entry, partial, final], BacktestConfig(honor_order_events=True))

    assert result.summary["closed_trades"] == 1
    assert result.trades[0]["partial_exits"][0]["age_sec"] == 120.0
    assert result.trades[0]["partial_exits"][0]["reason"] == "logic_decay_exit"
    assert result.trades[0]["partial_exits"][0]["price"] == 0.30
    assert result.trades[0]["partial_exits"][0]["shares"] == 40.0
    assert result.trades[0]["partial_exits"][0]["pnl"] == pytest.approx(8.0)
    assert result.trades[0]["pnl"] == 14.0


def test_scan_configs_returns_sorted_results() -> None:
    rows = [
        _row("m1", 60, 100.10),
        _row("m1", 299, 101.00),
    ]

    results = scan_configs(rows, early_edges=[0.08, 0.10], core_edges=[0.06], entry_starts=[40], entry_ends=[240])

    assert len(results) == 2
    assert results[0]["total_pnl"] >= results[1]["total_pnl"]
    assert {"early_required_edge", "core_required_edge", "entry_start_age_sec", "entry_end_age_sec"} <= set(results[0])


def test_backtest_applies_buy_slippage_and_uses_executable_bid_for_sell() -> None:
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
    assert result.trades[0]["exit_price"] == 0.69


def test_backtest_can_honor_paper_entry_no_fill_event() -> None:
    rows = [
        _row("m1", 90, 100.10),
        _row("m1", 299, 101.00),
    ]
    rows[0]["event"] = "order_no_fill"
    rows[0]["order_intent"] = "entry"

    result = run_backtest(rows, BacktestConfig(amount_usd=10.0, honor_order_events=True))

    assert result.summary["entries"] == 0
    assert result.summary["skip_reason_counts"]["entry_no_fill"] == 1


def test_backtest_can_honor_paper_entry_and_exit_events() -> None:
    rows = [
        _row("m1", 90, 100.10),
        _row("m1", 120, 100.10),
    ]
    rows[0]["event"] = "entry"
    rows[0]["analysis"] = {
        "entry_side": "up",
        "entry_phase": "early",
        "entry_price": 0.40,
        "entry_model_prob": 0.70,
        "entry_edge_signal": 0.30,
    }
    rows[0]["order"] = {"success": True, "filled_size": 25.0, "avg_price": 0.40}
    rows[1]["event"] = "exit"
    rows[1]["exit_reason"] = "market_overprice_exit"
    rows[1]["exit_price"] = 0.69
    rows[1]["order"] = {"success": True, "filled_size": 25.0, "avg_price": 0.69}
    rows[0]["up"]["ask_avg"] = 0.39
    rows[0]["up"]["ask_limit"] = 0.40

    result = run_backtest(rows, BacktestConfig(amount_usd=10.0, honor_order_events=True))

    assert result.summary["entries"] == 1
    assert result.summary["exits"] == 1
    assert result.trades[0]["exit_reason"] == "market_overprice_exit"
    assert result.trades[0]["exit_price"] == 0.69
    assert math.isclose(result.trades[0]["pnl"], 7.25)


def test_backtest_sell_slippage_is_clamped_by_fak_floor() -> None:
    rows = [
        _row("m1", 90, 100.10),
        _row("m1", 120, 100.00),
    ]
    rows[0]["up"]["ask_avg"] = 0.39
    rows[0]["up"]["ask_limit"] = 0.40
    rows[1]["up"]["bid_avg"] = 0.70
    rows[1]["up"]["bid_limit"] = 0.68

    result = run_backtest(rows, BacktestConfig(amount_usd=10.0, sell_slippage_ticks=5))

    assert result.trades[0]["exit_price"] == 0.65


def test_backtest_final_force_exit_uses_emergency_floor() -> None:
    rows = [
        _row("m1", 90, 100.10),
        _row("m1", 286, 100.10),
    ]
    rows[0]["up"]["ask_avg"] = 0.39
    rows[0]["up"]["ask_limit"] = 0.40
    rows[1]["up"]["bid_avg"] = 0.70
    rows[1]["up"]["bid_limit"] = 0.68

    result = run_backtest(rows, BacktestConfig(amount_usd=10.0, sell_slippage_ticks=10))

    assert result.trades[0]["exit_reason"] == "final_force_exit"
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


def test_backtest_skips_polymarket_open_disagreement_rows() -> None:
    rows = [_row("m1", 90, 100.10)]
    rows[0]["warnings"] = ["polymarket_ws_open_disagrees_with_api"]

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


def test_scan_configs_preserves_base_config_fields() -> None:
    base = BacktestConfig(
        early_to_core_age_sec=111.0,
        core_to_late_age_sec=222.0,
        profit_protection_start_remaining_sec=12.0,
        profit_protection_end_remaining_sec=34.0,
        defensive_take_profit_start_remaining_sec=35.0,
        defensive_take_profit_end_remaining_sec=67.0,
    )
    results = scan_configs(
        [],
        early_edges=[0.10],
        core_edges=[0.08],
        entry_starts=[60],
        entry_ends=[250],
        base_config=base,
    )

    assert results[0]["entries"] == 0
    cfg = replace(base, early_required_edge=0.10, core_required_edge=0.08, entry_start_age_sec=60.0, entry_end_age_sec=250.0)
    assert cfg.early_to_core_age_sec == 111.0
    assert cfg.core_to_late_age_sec == 222.0
    assert cfg.defensive_take_profit_end_remaining_sec == 67.0


def test_backtest_config_passes_max_entries_to_strategy() -> None:
    cfg = BacktestConfig(max_entries_per_market=4)

    assert cfg.edge_config().max_entries_per_market == 4


def test_backtest_config_passes_model_decay_buffer_to_strategy() -> None:
    cfg = BacktestConfig(model_decay_buffer=0.03)

    assert cfg.edge_config().model_decay_buffer == 0.03


def test_backtest_config_passes_polymarket_divergence_exit_to_strategy() -> None:
    cfg = BacktestConfig(polymarket_divergence_exit_bps=3.0, polymarket_divergence_exit_min_age_sec=3.0)

    edge_cfg = cfg.edge_config()

    assert edge_cfg.polymarket_divergence_exit_bps == 3.0
    assert edge_cfg.polymarket_divergence_exit_min_age_sec == 3.0


def test_backtest_replays_polymarket_divergence_exit_from_lead_field() -> None:
    rows = [
        _row("m1", 90, 100.10),
        _row("m1", 130, 100.10),
    ]
    rows[0]["up"]["ask_avg"] = 0.35
    rows[0]["up"]["ask_limit"] = 0.35
    rows[1]["up"]["bid_avg"] = 0.36
    rows[1]["up"]["bid_limit"] = 0.35
    rows[1]["lead_binance_vs_polymarket_bps"] = 3.4

    result = run_backtest(rows, BacktestConfig(
        amount_usd=10.0,
        polymarket_divergence_exit_bps=3.0,
        polymarket_divergence_exit_min_age_sec=3.0,
    ))

    assert result.trades[0]["exit_reason"] == "polymarket_divergence_exit"


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
