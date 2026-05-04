from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.strategy.dynamic_params import (
    CandidateResult,
    DynamicConfig,
    DynamicState,
    HealthCheck,
    SignalProfile,
    decide_dynamic_update,
    load_dynamic_config,
    recent_completed_window_rows,
    summarize_actual_results,
)


def _profiles() -> list[SignalProfile]:
    return [
        SignalProfile("aggressive", 100, 240, 0.14, 0.12, 4, min_candidate_trades=12),
        SignalProfile("balanced", 105, 240, 0.18, 0.16, 3, min_candidate_trades=8),
        SignalProfile("conservative", 135, 240, 0.22, 0.20, 2, min_candidate_trades=4),
    ]


def _config() -> DynamicConfig:
    return DynamicConfig(profiles=_profiles(), lookback_windows=30, check_every_windows=5)


def test_recent_completed_window_rows_keeps_last_complete_windows() -> None:
    rows = []
    for idx in range(4):
        slug = f"m{idx}"
        rows.append({"market_slug": slug, "event": "tick", "age_sec": 100, "remaining_sec": 200})
        rows.append({"market_slug": slug, "event": "tick", "age_sec": 300, "remaining_sec": 0})
    rows.append({"market_slug": "open", "event": "tick", "age_sec": 50, "remaining_sec": 250})

    selected = recent_completed_window_rows(rows, limit=2)

    assert [row["market_slug"] for row in selected if row["age_sec"] == 300] == ["m2", "m3"]
    assert {row["market_slug"] for row in selected} == {"m2", "m3"}


def test_summarize_actual_results_uses_strategy_exit_and_settlement_pnl() -> None:
    rows = [
        {"market_slug": "m1", "event": "tick", "age_sec": 300, "remaining_sec": 0},
        {"market_slug": "m1", "event": "exit", "exit_pnl": 1.0},
        {"market_slug": "m2", "event": "tick", "age_sec": 300, "remaining_sec": 0},
        {"market_slug": "m2", "event": "settlement", "settlement_pnl": -0.5},
    ]

    health = summarize_actual_results(rows, min_trades=2, min_win_rate=0.55, min_pnl=0.0)

    assert health.windows == 2
    assert health.closed_trades == 2
    assert health.win_rate == 0.5
    assert health.total_pnl == 0.5
    assert health.healthy is False
    assert "win_rate_low" in health.reasons


def test_first_unhealthy_check_does_not_switch_profile() -> None:
    state = DynamicState(active_profile="aggressive", failed_health_checks=0)
    health = HealthCheck(windows=30, closed_trades=20, win_rate=0.40, total_pnl=-1.0, max_drawdown=-2.0, healthy=False, reasons=["pnl_negative"])

    decision, new_state = decide_dynamic_update(health, [], _config(), state, mode="paper", current_window_id="m30")

    assert decision.action == "wait_for_confirmation"
    assert decision.selected_profile is None
    assert new_state.failed_health_checks == 1
    assert new_state.pending_profile is None


def test_second_unhealthy_check_selects_safe_more_conservative_profile() -> None:
    state = DynamicState(active_profile="aggressive", failed_health_checks=1)
    health = HealthCheck(windows=30, closed_trades=20, win_rate=0.40, total_pnl=-1.0, max_drawdown=-2.0, healthy=False, reasons=["pnl_negative"])
    candidates = [
        CandidateResult("aggressive", closed_trades=30, win_rate=0.70, total_pnl=-0.1, avg_pnl_per_trade=-0.003, max_drawdown=-3.0),
        CandidateResult("balanced", closed_trades=9, win_rate=0.60, total_pnl=1.2, avg_pnl_per_trade=0.133, max_drawdown=-1.0),
        CandidateResult("conservative", closed_trades=4, win_rate=0.75, total_pnl=0.8, avg_pnl_per_trade=0.2, max_drawdown=-0.5),
    ]

    decision, new_state = decide_dynamic_update(health, candidates, _config(), state, mode="paper", current_window_id="m35")

    assert decision.action == "switch_pending"
    assert decision.selected_profile == "conservative"
    assert new_state.pending_profile == "conservative"
    assert new_state.failed_health_checks == 2


def test_healthy_check_clears_failed_streak_and_keeps_current_profile() -> None:
    state = DynamicState(active_profile="balanced", failed_health_checks=1, pending_profile="conservative")
    health = HealthCheck(windows=30, closed_trades=25, win_rate=0.64, total_pnl=2.0, max_drawdown=-1.0, healthy=True, reasons=[])

    decision, new_state = decide_dynamic_update(health, [], _config(), state, mode="paper", current_window_id="m40")

    assert decision.action == "no_change"
    assert new_state.failed_health_checks == 0
    assert new_state.pending_profile is None


def test_live_cooldown_blocks_switch() -> None:
    state = DynamicState(active_profile="aggressive", failed_health_checks=1, switched_at_ts="2026-05-04T08:00:00+00:00")
    health = HealthCheck(windows=30, closed_trades=20, win_rate=0.40, total_pnl=-1.0, max_drawdown=-2.0, healthy=False, reasons=["pnl_negative"])
    candidates = [CandidateResult("balanced", closed_trades=9, win_rate=0.60, total_pnl=1.2, avg_pnl_per_trade=0.133, max_drawdown=-1.0)]

    decision, new_state = decide_dynamic_update(
        health,
        candidates,
        _config(),
        state,
        mode="live",
        current_window_id="m45",
        now_ts="2026-05-04T08:30:00+00:00",
    )

    assert decision.action == "cooldown_active"
    assert new_state.pending_profile is None


def test_drawdown_pause_uses_actual_drawdown_not_cumulative_pnl() -> None:
    state = DynamicState(active_profile="aggressive", failed_health_checks=1)
    health = HealthCheck(windows=30, closed_trades=20, win_rate=0.40, total_pnl=-1.0, max_drawdown=-2.0, healthy=False, reasons=["pnl_negative"])
    candidates = [CandidateResult("balanced", closed_trades=9, win_rate=0.60, total_pnl=1.2, avg_pnl_per_trade=0.133, max_drawdown=-1.0)]

    decision, new_state = decide_dynamic_update(
        health,
        candidates,
        _config(),
        state,
        mode="paper",
        current_window_id="m45",
        realized_drawdown=-60.0,
    )

    assert decision.action == "dynamic_paused"
    assert new_state.pending_profile is None


def test_no_safe_candidate_keeps_current_profile() -> None:
    state = DynamicState(active_profile="aggressive", failed_health_checks=1)
    health = HealthCheck(windows=30, closed_trades=20, win_rate=0.40, total_pnl=-1.0, max_drawdown=-2.0, healthy=False, reasons=["pnl_negative"])
    candidates = [
        CandidateResult("balanced", closed_trades=3, win_rate=1.0, total_pnl=1.0, avg_pnl_per_trade=0.333, max_drawdown=0.0),
        CandidateResult("conservative", closed_trades=4, win_rate=0.25, total_pnl=1.0, avg_pnl_per_trade=0.25, max_drawdown=0.0),
    ]

    decision, new_state = decide_dynamic_update(health, candidates, _config(), state, mode="paper", current_window_id="m50")

    assert decision.action == "no_safe_candidate"
    assert new_state.pending_profile is None
    assert {candidate.rejection_reason for candidate in decision.candidate_results} == {"insufficient_candidate_trades", "candidate_win_rate_low"}


def test_current_profile_best_does_not_switch() -> None:
    state = DynamicState(active_profile="aggressive", failed_health_checks=1)
    health = HealthCheck(windows=30, closed_trades=20, win_rate=0.40, total_pnl=-1.0, max_drawdown=-2.0, healthy=False, reasons=["pnl_negative"])
    candidates = [
        CandidateResult("aggressive", closed_trades=30, win_rate=0.80, total_pnl=5.0, avg_pnl_per_trade=0.166, max_drawdown=-1.0),
        CandidateResult("balanced", closed_trades=9, win_rate=0.60, total_pnl=1.2, avg_pnl_per_trade=0.133, max_drawdown=-1.0),
    ]

    decision, new_state = decide_dynamic_update(health, candidates, _config(), state, mode="paper", current_window_id="m55")

    assert decision.action == "keep_current"
    assert decision.selected_profile is None
    assert new_state.pending_profile is None


def test_more_aggressive_candidates_are_logged_but_rejected() -> None:
    state = DynamicState(active_profile="balanced", failed_health_checks=1)
    health = HealthCheck(windows=30, closed_trades=20, win_rate=0.40, total_pnl=-1.0, max_drawdown=-2.0, healthy=False, reasons=["pnl_negative"])
    candidates = [
        CandidateResult("aggressive", closed_trades=30, win_rate=0.80, total_pnl=5.0, avg_pnl_per_trade=0.166, max_drawdown=-1.0),
        CandidateResult("conservative", closed_trades=4, win_rate=0.75, total_pnl=0.8, avg_pnl_per_trade=0.2, max_drawdown=-0.5),
    ]

    decision, _new_state = decide_dynamic_update(health, candidates, _config(), state, mode="paper", current_window_id="m60")

    rejected = {candidate.profile: candidate.rejection_reason for candidate in decision.candidate_results}
    assert rejected["aggressive"] == "more_aggressive_than_active"
    assert decision.selected_profile == "conservative"


def test_default_dynamic_config_file_loads_profiles() -> None:
    cfg = load_dynamic_config(Path(__file__).resolve().parents[1] / "configs/prob_edge_dynamic.yaml")

    assert cfg.active_profile == "aggressive"
    assert cfg.lookback_windows == 50
    assert cfg.check_every_windows == 5
    assert cfg.slippage_ticks == 3
    assert cfg.profile("strict").min_candidate_trades == 3
