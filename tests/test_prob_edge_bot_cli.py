from __future__ import annotations

import sys
import datetime as dt
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.run_prob_edge_bot import (
    WindowPrices,
    _config_log_row,
    _entry_analysis,
    _exit_analysis,
    _should_write_row,
    build_arg_parser,
    build_runtime_options,
    choose_settlement,
    is_dvol_stale,
    prune_jsonl_by_retention,
)
from new_poly.strategy.prob_edge import StrategyDecision
from new_poly.trading.execution import ExecutionResult


def test_default_mode_is_paper() -> None:
    args = build_arg_parser().parse_args(["--once"])
    opts = build_runtime_options(args)

    assert opts.mode == "paper"
    assert opts.analysis_logs is True
    assert opts.windows is None
    assert opts.config.amount_usd > 0


def test_live_mode_defaults_analysis_logs_off() -> None:
    args = build_arg_parser().parse_args(["--mode", "live", "--i-understand-live-risk", "--once"])
    opts = build_runtime_options(args)

    assert opts.mode == "live"
    assert opts.analysis_logs is False
    assert opts.dynamic_params is False


def test_analysis_log_flags_override_mode_defaults() -> None:
    paper_args = build_arg_parser().parse_args(["--once", "--no-analysis-logs"])
    live_args = build_arg_parser().parse_args(["--mode", "live", "--i-understand-live-risk", "--once", "--analysis-logs"])

    assert build_runtime_options(paper_args).analysis_logs is False
    assert build_runtime_options(live_args).analysis_logs is True


def test_dynamic_params_cli_options_are_explicit() -> None:
    args = build_arg_parser().parse_args([
        "--once",
        "--dynamic-params",
        "--dynamic-config",
        "configs/prob_edge_dynamic.yaml",
        "--dynamic-state",
        "data/custom-dynamic-state.json",
        "--jsonl",
        "data/run.jsonl",
    ])
    opts = build_runtime_options(args)

    assert opts.dynamic_params is True
    assert str(opts.dynamic_config).endswith("configs/prob_edge_dynamic.yaml")
    assert str(opts.dynamic_state).endswith("data/custom-dynamic-state.json")


def test_dynamic_params_requires_jsonl_at_startup() -> None:
    args = build_arg_parser().parse_args(["--once", "--dynamic-params"])

    with pytest.raises(ValueError, match="dynamic-params requires --jsonl"):
        build_runtime_options(args)


def test_log_retention_defaults_to_24_hours() -> None:
    args = build_arg_parser().parse_args(["--once"])
    opts = build_runtime_options(args)

    assert opts.log_retention_hours == 24.0
    assert opts.log_prune_every_windows == 5


def test_log_prune_every_windows_has_minimum_one() -> None:
    args = build_arg_parser().parse_args(["--once", "--log-prune-every-windows", "0"])
    opts = build_runtime_options(args)

    assert opts.log_prune_every_windows == 1


def test_aggressive_config_has_live_fak_safety_guards() -> None:
    args = build_arg_parser().parse_args(["--config", "configs/prob_edge_aggressive.yaml", "--once"])
    opts = build_runtime_options(args)

    assert opts.config.execution.depth_safety_multiplier == 1.5
    assert opts.config.execution.buy_price_buffer_ticks == 2.0
    assert opts.config.execution.buy_retry_price_buffer_ticks == 4.0
    assert opts.config.execution.sell_price_buffer_ticks == 3.0
    assert opts.config.execution.sell_retry_price_buffer_ticks == 5.0
    assert opts.config.execution.retry_interval_sec == 0.0
    assert opts.config.interval_sec == 0.5
    assert opts.config.edge.min_fair_cap_margin_ticks == 1.0
    assert opts.config.edge.prob_drop_exit_window_sec == 5.0
    assert opts.config.edge.prob_drop_exit_threshold == 0.06
    assert opts.config.edge.early_required_edge == 0.16
    assert opts.config.edge.core_required_edge == 0.14
    assert opts.config.edge.min_entry_model_prob == 0.35


def test_prune_jsonl_by_retention_keeps_recent_and_unparseable_rows(tmp_path: Path) -> None:
    path = tmp_path / "run.jsonl"
    path.write_text(
        "\n".join([
            '{"ts":"2026-05-03T00:00:00+00:00","event":"old"}',
            '{"ts":"2026-05-04T01:00:00+00:00","event":"recent"}',
            '{"event":"no_ts"}',
            "not-json",
        ])
        + "\n",
    )

    removed = prune_jsonl_by_retention(path, retention_hours=24.0, now=dt.datetime(2026, 5, 4, 2, 0, tzinfo=dt.timezone.utc))

    assert removed == 1
    text = path.read_text()
    assert '"event":"old"' not in text
    assert '"event":"recent"' in text
    assert '{"event":"no_ts"}' in text
    assert "not-json" in text


def test_logger_prune_does_not_reopen_when_retention_disabled(tmp_path: Path) -> None:
    from scripts.run_prob_edge_bot import JsonlLogger

    path = tmp_path / "run.jsonl"
    logger = JsonlLogger(path, retention_hours=None)
    handle = logger.handle

    assert logger.prune() == 0
    assert logger.handle is handle
    logger.close()


def test_dynamic_payload_helpers_return_explicit_shapes() -> None:
    from scripts.run_prob_edge_bot import _dynamic_candidate_payload, _dynamic_health_payload

    result = {"health": {"closed_trades": 20}, "candidate_results": [{"profile": "balanced"}]}

    assert _dynamic_health_payload(result) == {"closed_trades": 20}
    assert _dynamic_candidate_payload(result) == [{"profile": "balanced"}]
    assert _dynamic_health_payload({"legacy": True}) is None
    assert _dynamic_candidate_payload({"candidate_results": {"bad": "shape"}}) == []


def test_live_mode_requires_second_guard() -> None:
    args = build_arg_parser().parse_args(["--mode", "live", "--once"])

    with pytest.raises(ValueError, match="i-understand-live-risk"):
        build_runtime_options(args)


def test_amount_override_keeps_depth_check_same_notional() -> None:
    args = build_arg_parser().parse_args(["--once", "--amount-usd", "12.5"])
    opts = build_runtime_options(args)

    assert opts.config.amount_usd == 12.5
    assert opts.config.execution.depth_notional == 12.5
    assert opts.config.execution.retry_count == 1
    assert opts.config.execution.retry_interval_sec == 0.0


def test_config_uses_phase_edges_and_defensive_exit_thresholds() -> None:
    args = build_arg_parser().parse_args(["--once"])
    opts = build_runtime_options(args)

    assert not hasattr(opts.config.edge, "required_edge")
    assert opts.config.edge.early_required_edge == 0.16
    assert opts.config.edge.core_required_edge == 0.14
    assert opts.config.edge.entry_start_age_sec == 90.0
    assert opts.config.edge.late_entry_enabled is False
    assert opts.config.edge.defensive_profit_min == 0.03
    assert opts.config.edge.protection_profit_min == 0.01
    assert opts.config.edge.final_hold_min_prob == 0.98
    assert opts.config.edge.min_entry_model_prob == 0.35


def test_dvol_stale_after_configured_age() -> None:
    assert is_dvol_stale(None, now_monotonic=1000.0, max_age_sec=900.0) is True


def test_proxy_settlement_flags_boundary_uncertain() -> None:
    prices = WindowPrices(k_price=100.0)
    result = choose_settlement(prices, latest_proxy_price=102.0, boundary_usd=5.0)

    assert result["winning_side"] == "up"
    assert result["settlement_source"] == "binance_proxy"
    assert result["settlement_uncertain"] is True


def test_config_log_row_contains_non_secret_runtime_shape() -> None:
    args = build_arg_parser().parse_args(["--once"])
    opts = build_runtime_options(args)

    row = _config_log_row(opts)

    assert row["event"] == "config"
    assert row["mode"] == "paper"
    assert row["analysis_logs"] is True
    assert row["strategy"]["core_required_edge"] == opts.config.edge.core_required_edge
    assert row["execution"]["amount_usd"] == opts.config.amount_usd
    assert "private_key" not in str(row).lower()


def test_entry_analysis_records_signal_and_fill_edges() -> None:
    decision = StrategyDecision(
        action="enter",
        reason="edge",
        side="up",
        model_prob=0.70,
        price=0.40,
        limit_price=0.58,
        depth_limit_price=0.55,
        best_ask=0.39,
        edge=0.30,
        phase="core",
        required_edge=0.08,
    )
    result = ExecutionResult(True, filled_size=10.0, avg_price=0.56, attempt=2, total_latency_ms=620, timing={"paper_actual_sleep_ms": 400})

    analysis = _entry_analysis(decision, result)

    assert analysis["order_intent"] == "entry"
    assert analysis["entry_edge_signal"] == 0.30
    assert analysis["entry_edge_at_fill"] == 0.14
    assert analysis["entry_depth_limit_price"] == 0.55
    assert analysis["order_attempt"] == 2
    assert analysis["order_timing"]["paper_actual_sleep_ms"] == 400


def test_exit_analysis_records_exit_floor_and_profit() -> None:
    decision = StrategyDecision(
        action="exit",
        reason="logic_decay_exit",
        side="down",
        model_prob=0.30,
        price=0.42,
        limit_price=0.40,
        profit_now=-0.03,
        prob_stagnant=True,
        prob_delta_3s=-0.01,
    )
    result = ExecutionResult(True, filled_size=7.0, avg_price=0.41, attempt=1, total_latency_ms=410, timing={"book_read_ms": 1})

    analysis = _exit_analysis(decision, result)

    assert analysis["order_intent"] == "exit"
    assert analysis["exit_reason"] == "logic_decay_exit"
    assert analysis["exit_min_price"] == 0.40
    assert analysis["exit_profit_per_share"] == -0.03
    assert analysis["exit_price"] == 0.41
    assert analysis["order_timing"]["book_read_ms"] == 1


def test_outside_entry_time_skip_logs_once_per_window() -> None:
    seen: set[tuple[str, str]] = set()
    row = {
        "event": "tick",
        "market_slug": "m1",
        "decision": {"action": "skip", "reason": "outside_entry_time"},
    }

    assert _should_write_row(row, seen) is True
    assert _should_write_row(row, seen) is False
    assert _should_write_row({**row, "market_slug": "m2"}, seen) is True
    assert _should_write_row({**row, "decision": {"action": "skip", "reason": "edge_too_small"}}, seen) is True


def test_max_entries_skip_logs_once_per_window() -> None:
    seen: set[tuple[str, str]] = set()
    row = {
        "event": "tick",
        "market_slug": "m1",
        "decision": {"action": "skip", "reason": "max_entries"},
    }

    assert _should_write_row(row, seen) is True
    assert _should_write_row(row, seen) is False
    assert _should_write_row({**row, "market_slug": "m2"}, seen) is True


def test_edge_too_small_skip_logs_once_per_window_phase() -> None:
    seen: set[tuple[str, str]] = set()
    row = {
        "event": "tick",
        "market_slug": "m1",
        "decision": {"action": "skip", "reason": "edge_too_small", "phase": "early"},
    }

    assert _should_write_row(row, seen) is True
    assert _should_write_row(row, seen) is False
    assert _should_write_row({**row, "decision": {"action": "skip", "reason": "edge_too_small", "phase": "core"}}, seen) is True


def test_final_no_entry_skip_logs_once_per_window() -> None:
    seen: set[tuple[str, str]] = set()
    row = {
        "event": "tick",
        "market_slug": "m1",
        "decision": {"action": "skip", "reason": "final_no_entry", "phase": "final_no_entry"},
    }

    assert _should_write_row(row, seen) is True
    assert _should_write_row(row, seen) is False
    assert _should_write_row({**row, "market_slug": "m2"}, seen) is True
