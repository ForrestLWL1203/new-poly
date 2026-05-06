from __future__ import annotations

import sys
import asyncio
import datetime as dt
import math
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.run_prob_edge_bot import (
    WindowPrices,
    _config_log_row,
    _entry_analysis,
    _exit_analysis,
    _polymarket_reference_unhealthy_row,
    _price_analysis,
    _should_attach_reference_meta,
    _refresh_exit_retry_params,
    _runtime_log_meta,
    _snapshot,
    _should_write_row,
    _warmup_warning_row,
    build_arg_parser,
    build_runtime_options,
    choose_settlement,
    is_dvol_stale,
    prune_jsonl_by_retention,
)
from new_poly.strategy.prob_edge import MarketSnapshot, PositionSnapshot, StrategyDecision
from new_poly.strategy.state import StrategyState
from new_poly.trading.execution import ExecutionResult


def test_default_mode_is_paper() -> None:
    args = build_arg_parser().parse_args(["--once"])
    opts = build_runtime_options(args)

    assert opts.mode == "paper"
    assert opts.analysis_logs is True
    assert opts.windows is None
    assert opts.config.amount_usd > 0
    assert opts.config.polymarket_price_enabled is True
    assert opts.config.polymarket_stale_reconnect_sec == 5.0
    assert opts.config.polymarket_unhealthy_log_after_sec == 10.0
    assert opts.config.coinbase_enabled is False


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


def test_coinbase_cli_overrides_config() -> None:
    enabled_args = build_arg_parser().parse_args(["--once", "--coinbase"])
    disabled_args = build_arg_parser().parse_args(["--once", "--coinbase", "--no-coinbase"])

    assert build_runtime_options(enabled_args).config.coinbase_enabled is True
    assert build_runtime_options(disabled_args).config.coinbase_enabled is False


def test_polymarket_price_cli_overrides_config() -> None:
    enabled_args = build_arg_parser().parse_args(["--once", "--no-polymarket-price", "--polymarket-price"])
    disabled_args = build_arg_parser().parse_args(["--once", "--no-polymarket-price"])

    assert build_runtime_options(enabled_args).config.polymarket_price_enabled is True
    assert build_runtime_options(disabled_args).config.polymarket_price_enabled is False


def test_polymarket_unhealthy_log_delay_cli_overrides_config() -> None:
    args = build_arg_parser().parse_args(["--once", "--polymarket-unhealthy-log-after-sec", "60"])

    assert build_runtime_options(args).config.polymarket_unhealthy_log_after_sec == 60.0


def test_polymarket_divergence_exit_cli_overrides_config() -> None:
    args = build_arg_parser().parse_args(["--once", "--polymarket-divergence-exit-bps", "2.5"])

    assert build_runtime_options(args).config.edge.polymarket_divergence_exit_bps == 2.5


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
    assert opts.config.execution.sell_price_buffer_ticks == 5.0
    assert opts.config.execution.sell_retry_price_buffer_ticks == 6.0
    assert opts.config.execution.batch_exit_enabled is True
    assert opts.config.execution.batch_exit_min_shares == 20.0
    assert opts.config.execution.batch_exit_min_notional_usd == 5.0
    assert opts.config.execution.batch_exit_slices == (0.4, 0.3, 1.0)
    assert opts.config.execution.batch_exit_extra_buffer_ticks == (0.0, 3.0, 6.0)
    assert opts.config.execution.paper_latency_sec == 0.0
    assert opts.config.execution.retry_interval_sec == 0.0
    assert opts.config.interval_sec == 0.5
    assert opts.config.edge.min_fair_cap_margin_ticks == 1.0
    assert opts.config.edge.prob_drop_exit_window_sec == 0.0
    assert opts.config.edge.prob_drop_exit_threshold == 0.0
    assert opts.config.edge.model_decay_buffer == 0.03
    assert opts.config.edge.early_required_edge == 0.16
    assert opts.config.edge.core_required_edge == 0.14
    assert opts.config.edge.min_entry_model_prob == 0.35
    assert opts.config.edge.low_price_extra_edge_threshold == 0.30
    assert opts.config.edge.low_price_extra_edge == 0.02
    assert opts.config.edge.cross_source_max_bps == 5.0
    assert opts.config.edge.market_disagrees_exit_threshold == 0.30
    assert opts.config.edge.market_disagrees_exit_max_remaining_sec == 60.0
    assert opts.config.edge.market_disagrees_exit_min_loss == 0.03
    assert opts.config.edge.final_force_exit_remaining_sec == 30.0
    assert opts.config.polymarket_stale_reconnect_sec == 5.0
    assert opts.config.polymarket_unhealthy_log_after_sec == 10.0
    assert opts.config.edge.polymarket_divergence_exit_bps == 3.0
    assert opts.config.edge.polymarket_divergence_exit_min_age_sec == 3.0
    assert opts.config.coinbase_enabled is False


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


def test_exit_retry_refresh_commits_to_sell_even_if_signal_no_longer_exits(monkeypatch) -> None:
    args = build_arg_parser().parse_args(["--once"])
    opts = build_runtime_options(args)
    position = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=120.0,
        entry_avg_price=0.40,
        filled_shares=2.5,
        entry_model_prob=0.55,
        entry_edge=0.15,
    )
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=150.0,
        remaining_sec=150.0,
        s_price=100.0,
        k_price=100.0,
        sigma_eff=0.6,
        up_bid_avg=0.41,
        up_bid_limit=0.41,
        up_bid_depth_ok=True,
        down_bid_avg=0.58,
        down_bid_limit=0.58,
        down_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    def fake_snapshot(*args, **kwargs):
        return snap, {}

    monkeypatch.setattr("scripts.run_prob_edge_bot._snapshot", fake_snapshot)

    retry = asyncio.run(
        _refresh_exit_retry_params(
            window=object(),
            prices=WindowPrices(),
            feed=object(),
            coinbase_feed=None,
            polymarket_feed=None,
            stream=object(),
            cfg=opts.config,
            sigma_eff=0.6,
            state=StrategyState(current_market_slug="m1"),
            position=position,
            exit_reason="prob_drop_exit",
        )
    )

    assert retry is not None
    assert retry.min_price == 0.41
    assert retry.exit_reason == "prob_drop_exit"


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
    assert result["settlement_source"] == "multi_source_proxy"
    assert result["settlement_uncertain"] is True


def test_runtime_log_meta_keeps_price_diagnostics_for_analysis_logs() -> None:
    meta = {
        "price_source": "proxy_multi_source_basis_adjusted",
        "s_price": 100070.0,
        "k_price": 100000.0,
        "basis_bps": 4.0,
        "binance_price": 100120.0,
        "coinbase_price": 100100.0,
        "proxy_price": 100110.0,
        "binance_open_price": 100050.0,
        "coinbase_open_price": 100030.0,
        "proxy_open_price": 100040.0,
        "source_spread_usd": 20.0,
        "source_spread_bps": 1.998,
    }

    runtime = _runtime_log_meta(meta)
    analysis = _price_analysis(meta)

    assert runtime == {
        "price_source": "proxy_multi_source_basis_adjusted",
        "s_price": 100070.0,
        "k_price": 100000.0,
        "basis_bps": 4.0,
    }
    assert analysis["binance_price"] == 100120.0
    assert analysis["coinbase_price"] == 100100.0
    assert analysis["source_spread_usd"] == 20.0


def test_price_analysis_uses_proxy_branch_for_reference_diagnostics() -> None:
    meta = {
        "price_source": "proxy_binance_basis_adjusted",
        "s_price": 100080.0,
        "k_price": 100000.0,
        "basis_bps": 0.0,
        "polymarket_price": 100080.0,
        "polymarket_price_age_sec": 0.8,
        "polymarket_open_price": 100000.0,
        "polymarket_open_source": "ws_first_after",
        "binance_price": None,
        "coinbase_price": None,
        "proxy_price": None,
        "source_spread_usd": None,
        "lead_binance_vs_polymarket_usd": 40.0,
        "lead_binance_vs_polymarket_bps": 3.997,
        "polymarket_divergence_bps": 3.997,
        "lead_coinbase_vs_polymarket_usd": 20.0,
        "lead_proxy_vs_polymarket_usd": 30.0,
        "lead_binance_return_3s_bps": 1.2,
        "lead_polymarket_return_3s_bps": 0.4,
        "lead_binance_side": "up",
        "lead_polymarket_side": "down",
        "lead_binance_side_disagrees_with_polymarket": True,
    }

    analysis = _price_analysis(meta)

    assert analysis == {
        "price_source": "proxy_binance_basis_adjusted",
        "s_price": 100080.0,
        "k_price": 100000.0,
        "basis_bps": 0.0,
        "polymarket_price": 100080.0,
        "polymarket_price_age_sec": 0.8,
        "lead_binance_vs_polymarket_usd": 40.0,
        "lead_binance_vs_polymarket_bps": 3.997,
        "polymarket_divergence_bps": 3.997,
        "lead_binance_return_3s_bps": 1.2,
        "lead_polymarket_return_3s_bps": 0.4,
        "lead_binance_side": "up",
        "lead_polymarket_side": "down",
        "lead_binance_side_disagrees_with_polymarket": True,
    }


def test_binance_proxy_is_model_source_while_polymarket_is_reference() -> None:
    class FakeFeed:
        latest_price = 100_120.0

        def price_at_or_before(self, *_args, **_kwargs):
            return 100_100.0

    class FakePolymarketFeed:
        latest_price = 100_080.0

        def latest_age_sec(self):
            return 5.0

        def price_at_or_before(self, *_args, **_kwargs):
            return 100_070.0

    class FakeStream:
        def get_latest_ask_levels_with_size(self, _token_id):
            return [(0.5, 10.0)]

        def get_latest_bid_levels_with_size(self, _token_id):
            return [(0.49, 10.0)]

        def get_latest_best_bid(self, _token_id):
            return 0.49

        def get_latest_best_ask(self, _token_id):
            return 0.5

        def get_latest_best_ask_age(self, _token_id):
            return 0.01

    window = type("Window", (), {
        "slug": "m1",
        "start_time": dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=120),
        "end_time": dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=180),
        "up_token": "up",
        "down_token": "down",
    })()
    args = build_arg_parser().parse_args(["--config", "configs/prob_edge_aggressive.yaml", "--once"])
    cfg = build_runtime_options(args).config

    snap, meta = _snapshot(
        window,
        WindowPrices(k_price=100_000.0),
        FakeFeed(),
        None,
        FakePolymarketFeed(),
        FakeStream(),
        cfg,
        0.4,
    )

    assert snap.s_price == 100_120.0
    assert meta["price_source"] == "proxy_binance"
    assert meta["polymarket_price"] == 100_080.0
    assert meta["lead_binance_vs_polymarket_usd"] == 40.0
    assert math.isclose(snap.polymarket_divergence_bps or 0.0, meta["polymarket_divergence_bps"], abs_tol=0.001)


def test_price_analysis_logs_only_backup_proxy_fields_when_fallback_active() -> None:
    meta = {
        "price_source": "proxy_multi_source_basis_adjusted",
        "s_price": 100070.0,
        "k_price": 100000.0,
        "basis_bps": 4.0,
        "polymarket_price": 100080.0,
        "polymarket_price_age_sec": 4.5,
        "binance_price": 100120.0,
        "coinbase_price": 100100.0,
        "proxy_price": 100110.0,
        "binance_open_price": 100050.0,
        "coinbase_open_price": 100030.0,
        "proxy_open_price": 100040.0,
        "source_spread_usd": 20.0,
        "source_spread_bps": 1.998,
    }

    analysis = _price_analysis(meta)

    assert analysis["price_source"] == "proxy_multi_source_basis_adjusted"
    assert analysis["proxy_price"] == 100110.0
    assert analysis["binance_price"] == 100120.0
    assert analysis["coinbase_price"] == 100100.0
    assert analysis["polymarket_price"] == 100080.0
    assert "polymarket_open_price" not in analysis


def test_reference_meta_only_attaches_for_analysis_or_active_risk_context() -> None:
    reference = {"polymarket_divergence_bps": 3.2}
    exit_decision = StrategyDecision(action="exit", reason="polymarket_divergence_exit")
    hold_decision = StrategyDecision(action="hold", reason="edge_intact")

    assert _should_attach_reference_meta(reference, analysis_logs=True, has_position=False, decision=None) is True
    assert _should_attach_reference_meta(reference, analysis_logs=False, has_position=True, decision=hold_decision) is True
    assert _should_attach_reference_meta(reference, analysis_logs=False, has_position=False, decision=exit_decision) is True
    assert _should_attach_reference_meta(reference, analysis_logs=False, has_position=False, decision=None) is False
    assert _should_attach_reference_meta({}, analysis_logs=True, has_position=True, decision=hold_decision) is False


def test_warmup_warning_row_reports_missing_binance_tick() -> None:
    now = dt.datetime(2026, 5, 6, 0, 0, tzinfo=dt.timezone.utc)

    row = _warmup_warning_row(now=now, mode="paper", market_slug="m1", unhealthy_log_after_sec=180.0)

    assert row == {
        "ts": now.astimezone().isoformat(),
        "event": "warning",
        "mode": "paper",
        "market_slug": "m1",
        "warning": "binance_ws_warmup_no_tick",
        "message": "Binance WS warmup expired without first tick",
        "polymarket_reference_check_after_sec": 180.0,
    }


def test_polymarket_reference_unhealthy_row_is_auditable() -> None:
    now = dt.datetime(2026, 5, 6, 0, 0, tzinfo=dt.timezone.utc)

    row = _polymarket_reference_unhealthy_row(
        now=now,
        mode="paper",
        market_slug="m1",
        unhealthy_for_sec=181.2,
        coinbase_started=True,
    )

    assert row["event"] == "polymarket_reference_unhealthy"
    assert row["trigger"] == "polymarket_unhealthy_for_seconds"
    assert row["unhealthy_for_sec"] == 181.2
    assert row["coinbase_started"] is True


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
