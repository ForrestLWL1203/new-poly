from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.run_prob_edge_bot import (
    WindowPrices,
    build_arg_parser,
    build_runtime_options,
    choose_settlement,
    is_dvol_stale,
)


def test_default_mode_is_paper() -> None:
    args = build_arg_parser().parse_args(["--once"])
    opts = build_runtime_options(args)

    assert opts.mode == "paper"
    assert opts.windows is None
    assert opts.config.amount_usd > 0


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
    assert opts.config.execution.retry_interval_sec == 0.2


def test_config_uses_phase_edges_and_defensive_exit_thresholds() -> None:
    args = build_arg_parser().parse_args(["--once"])
    opts = build_runtime_options(args)

    assert not hasattr(opts.config.edge, "required_edge")
    assert opts.config.edge.early_required_edge == 0.12
    assert opts.config.edge.core_required_edge == 0.08
    assert opts.config.edge.entry_start_age_sec == 90.0
    assert opts.config.edge.late_entry_enabled is False
    assert opts.config.edge.defensive_profit_min == 0.03
    assert opts.config.edge.protection_profit_min == 0.01
    assert opts.config.edge.final_hold_min_prob == 0.98


def test_dvol_stale_after_configured_age() -> None:
    assert is_dvol_stale(None, now_monotonic=1000.0, max_age_sec=900.0) is True


def test_proxy_settlement_flags_boundary_uncertain() -> None:
    prices = WindowPrices(k_price=100.0)
    result = choose_settlement(prices, latest_proxy_price=102.0, boundary_usd=5.0)

    assert result["winning_side"] == "up"
    assert result["settlement_source"] == "binance_proxy"
    assert result["settlement_uncertain"] is True
