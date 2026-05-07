from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.strategy.prob_edge import (
    EdgeConfig,
    MarketSnapshot,
    PositionSnapshot,
    required_edge_for_entry,
    evaluate_entry,
    evaluate_exit,
)
from new_poly.strategy.probability import MIN_SECONDS_LEFT_FOR_D2, binary_probabilities, binary_probability
from new_poly.strategy.state import StrategyState


def test_binary_probability_direction_and_expiry() -> None:
    at_strike = binary_probability(100.0, 100.0, 0.6, 300.0)
    above = binary_probability(101.0, 100.0, 0.6, 300.0)

    assert 0.48 < at_strike < 0.52
    assert above > 0.5
    assert binary_probability(101.0, 100.0, 0.6, 0.0) == 1.0
    assert binary_probability(99.0, 100.0, 0.6, 0.0) == 0.0


def test_binary_probabilities_sum_to_one() -> None:
    probs = binary_probabilities(101.0, 100.0, 0.6, 120.0)

    assert probs.up > probs.down
    assert math.isclose(probs.up + probs.down, 1.0, abs_tol=1e-12)


def test_binary_probability_uses_minimum_pre_expiry_time_guard() -> None:
    tiny = binary_probabilities(100.1, 100.0, 0.6, MIN_SECONDS_LEFT_FOR_D2 / 2)
    guarded = binary_probabilities(100.1, 100.0, 0.6, MIN_SECONDS_LEFT_FOR_D2)

    assert tiny.up == guarded.up
    assert tiny.d2 == guarded.d2
    assert binary_probability(100.1, 100.0, 0.6, 0.0) == 1.0


def test_strategy_state_tracks_peak_pnl_and_drawdown() -> None:
    state = StrategyState()
    state.record_entry(PositionSnapshot("m1", "up", "up-token", 1.0, 0.20, 10.0, 0.5, 0.3))
    state.record_exit(0.70, "take_profit")
    state.record_entry(PositionSnapshot("m1", "up", "up-token", 2.0, 0.50, 10.0, 0.5, 0.0))
    state.record_exit(0.20, "loss")

    assert math.isclose(state.realized_pnl, 2.0)
    assert math.isclose(state.peak_pnl, 5.0)
    assert math.isclose(state.drawdown, -3.0)


def test_strategy_state_partial_exit_keeps_remaining_position() -> None:
    state = StrategyState()
    state.record_entry(PositionSnapshot("m1", "up", "up-token", 1.0, 0.20, 100.0, 0.5, 0.3))

    pnl, closed = state.record_partial_exit(0.50, 40.0, "partial_exit")

    assert math.isclose(pnl, 12.0)
    assert closed is False
    assert state.has_position is True
    assert state.open_position is not None
    assert math.isclose(state.open_position.filled_shares, 60.0)
    assert math.isclose(state.realized_pnl, 12.0)

    pnl, closed = state.record_partial_exit(0.40, 100.0, "final_exit")

    assert math.isclose(pnl, 12.0)
    assert closed is True
    assert state.open_position is None
    assert math.isclose(state.realized_pnl, 24.0)


def test_entry_rejects_warmup_and_existing_position() -> None:
    cfg = EdgeConfig()
    state = StrategyState(current_market_slug="m1")
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=39.0,
        remaining_sec=261.0,
        s_price=101.0,
        k_price=100.0,
        sigma_eff=0.6,
        up_ask_avg=0.45,
        down_ask_avg=0.45,
        up_bid_avg=0.44,
        down_bid_avg=0.44,
        up_ask_depth_ok=True,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    warmup = evaluate_entry(snap, state, cfg)
    assert warmup.action == "skip"
    assert warmup.reason == "outside_entry_time"

    state.open_position = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.45,
        filled_shares=10.0,
        entry_model_prob=0.6,
        entry_edge=0.15,
    )
    in_position = evaluate_entry(snap, state, cfg)
    assert in_position.reason == "already_holding"


def test_entry_selects_largest_edge_when_both_sides_pass() -> None:
    cfg = EdgeConfig(core_required_edge=0.05)
    state = StrategyState(current_market_slug="m1")
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=120.0,
        remaining_sec=180.0,
        s_price=100.0,
        k_price=100.0,
        sigma_eff=2.0,
        up_ask_avg=0.41,
        down_ask_avg=0.39,
        up_ask_limit=0.42,
        down_ask_limit=0.41,
        up_best_ask=0.40,
        down_best_ask=0.38,
        up_bid_avg=0.40,
        down_bid_avg=0.38,
        up_ask_depth_ok=True,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    decision = evaluate_entry(snap, state, cfg)

    assert decision.action == "enter"
    assert decision.side == "down"
    assert decision.edge > 0.05
    assert decision.best_ask == 0.38
    assert decision.depth_limit_price == 0.38
    assert decision.limit_price is not None
    assert math.isclose(decision.limit_price, (decision.model_prob or 0.0) - 0.05, abs_tol=1e-12)
    assert decision.phase == "core"
    assert decision.required_edge == 0.05


def test_entry_ignores_depth_limit_when_best_ask_is_inside_formula_cap() -> None:
    cfg = EdgeConfig(core_required_edge=0.05)
    state = StrategyState(current_market_slug="m1")
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=180.0,
        remaining_sec=120.0,
        s_price=100.0,
        k_price=100.0,
        sigma_eff=2.0,
        up_ask_avg=0.40,
        up_ask_limit=0.47,
        up_best_ask=0.38,
        up_ask_depth_ok=True,
        down_ask_avg=0.90,
        down_ask_limit=0.90,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    decision = evaluate_entry(snap, state, cfg)

    assert decision.action == "enter"
    assert decision.side == "up"
    assert decision.price == 0.38
    assert decision.depth_limit_price == 0.38


def test_entry_rejects_when_formula_cap_has_less_than_one_tick_margin() -> None:
    cfg = EdgeConfig(core_required_edge=0.05, min_fair_cap_margin_ticks=1.0, entry_tick_size=0.01)
    state = StrategyState(current_market_slug="m1")
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=180.0,
        remaining_sec=120.0,
        s_price=100.0,
        k_price=100.0,
        sigma_eff=2.0,
        up_ask_avg=0.40,
        up_ask_limit=0.451,
        up_best_ask=0.44,
        up_ask_depth_ok=True,
        down_ask_avg=0.90,
        down_ask_limit=0.90,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    decision = evaluate_entry(snap, state, cfg)

    assert decision.action == "skip"
    assert decision.reason == "edge_too_small"


def test_entry_uses_fresh_best_ask_without_depth_accumulation() -> None:
    cfg = EdgeConfig(core_required_edge=0.05, min_fair_cap_margin_ticks=1.0, entry_tick_size=0.01)
    state = StrategyState(current_market_slug="m1")
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=180.0,
        remaining_sec=120.0,
        s_price=100.1,
        k_price=100.0,
        sigma_eff=0.55,
        up_best_ask=0.20,
        down_best_ask=0.90,
        up_ask_avg=None,
        down_ask_avg=None,
        up_ask_limit=None,
        down_ask_limit=None,
        up_ask_depth_ok=False,
        down_ask_depth_ok=False,
        up_book_age_ms=5000.0,
        down_book_age_ms=5000.0,
    )

    decision = evaluate_entry(snap, state, cfg)

    assert decision.action == "enter"
    assert decision.side == "up"
    assert decision.price == 0.20
    assert decision.best_ask == 0.20
    assert decision.depth_limit_price == 0.20


def test_entry_ignores_safety_depth_when_best_ask_is_inside_formula_cap() -> None:
    cfg = EdgeConfig(core_required_edge=0.05)
    state = StrategyState(current_market_slug="m1")
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=180.0,
        remaining_sec=120.0,
        s_price=100.0,
        k_price=100.0,
        sigma_eff=2.0,
        up_ask_avg=0.40,
        up_ask_limit=0.42,
        up_ask_safety_limit=0.47,
        up_best_ask=0.40,
        up_ask_depth_ok=True,
        down_ask_avg=0.90,
        down_ask_limit=0.90,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    decision = evaluate_entry(snap, state, cfg)

    assert decision.action == "enter"
    assert decision.side == "up"
    assert decision.price == 0.40
    assert decision.depth_limit_price == 0.40


def test_entry_rejects_low_model_probability_even_when_edge_is_large() -> None:
    cfg = EdgeConfig(core_required_edge=0.14, min_entry_model_prob=0.35)
    state = StrategyState(current_market_slug="m1")
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=180.0,
        remaining_sec=120.0,
        s_price=99.95,
        k_price=100.0,
        sigma_eff=0.55,
        up_ask_avg=0.05,
        up_ask_limit=0.05,
        up_best_ask=0.05,
        up_ask_depth_ok=True,
        down_ask_avg=0.90,
        down_ask_limit=0.90,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    decision = evaluate_entry(snap, state, cfg)

    assert decision.action == "skip"
    assert decision.reason == "model_prob_too_low"
    assert decision.up_prob is not None and decision.up_prob < 0.35


def test_low_price_entry_requires_extra_edge_when_configured() -> None:
    state = StrategyState(current_market_slug="m1")
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=180.0,
        remaining_sec=120.0,
        s_price=100.03,
        k_price=100.0,
        sigma_eff=0.55,
        up_ask_avg=0.90,
        up_ask_limit=0.90,
        up_ask_depth_ok=True,
        down_ask_avg=0.20,
        down_ask_limit=0.20,
        down_ask_safety_limit=0.20,
        down_best_ask=0.20,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )
    loose = EdgeConfig(core_required_edge=0.14, low_price_extra_edge_threshold=0.25, low_price_extra_edge=0.04)
    strict = EdgeConfig(core_required_edge=0.14, low_price_extra_edge_threshold=0.25, low_price_extra_edge=0.08)

    loose_decision = evaluate_entry(snap, state, loose)
    strict_decision = evaluate_entry(snap, state, strict)

    assert loose_decision.action == "enter"
    assert loose_decision.side == "down"
    assert math.isclose(loose_decision.required_edge or 0.0, 0.18)
    assert strict_decision.action == "skip"
    assert strict_decision.reason == "edge_too_small"
    assert math.isclose(strict_decision.required_edge or 0.0, 0.22)


def test_logic_decay_exit_blocks_same_side_reentry_during_cooldown() -> None:
    cfg = EdgeConfig(core_required_edge=0.05, logic_decay_reentry_cooldown_sec=30.0)
    state = StrategyState(current_market_slug="m1")
    state.last_exit_reason = "logic_decay_exit"
    state.last_exit_side = "up"
    state.last_exit_age_sec = 150.0
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=170.0,
        remaining_sec=130.0,
        s_price=100.2,
        k_price=100.0,
        sigma_eff=0.55,
        up_ask_avg=0.40,
        up_ask_limit=0.40,
        up_best_ask=0.40,
        up_ask_depth_ok=True,
        down_ask_avg=0.90,
        down_ask_limit=0.90,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    decision = evaluate_entry(snap, state, cfg)

    assert decision.action == "skip"
    assert decision.reason == "logic_decay_cooldown"


def test_risk_exit_blocks_same_side_reentry_during_cooldown() -> None:
    cfg = EdgeConfig(core_required_edge=0.05, logic_decay_reentry_cooldown_sec=30.0)
    state = StrategyState(current_market_slug="m1")
    state.last_exit_reason = "polymarket_divergence_exit"
    state.last_exit_side = "up"
    state.last_exit_age_sec = 150.0
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=170.0,
        remaining_sec=130.0,
        s_price=100.2,
        k_price=100.0,
        sigma_eff=0.55,
        up_ask_avg=0.40,
        up_ask_limit=0.40,
        up_best_ask=0.40,
        up_ask_depth_ok=True,
        down_ask_avg=0.90,
        down_ask_limit=0.90,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    decision = evaluate_entry(snap, state, cfg)

    assert decision.action == "skip"
    assert decision.reason == "risk_exit_cooldown"


def test_profit_exit_does_not_block_same_side_reentry() -> None:
    cfg = EdgeConfig(core_required_edge=0.05, logic_decay_reentry_cooldown_sec=30.0)
    state = StrategyState(current_market_slug="m1")
    state.last_exit_reason = "market_overprice_exit"
    state.last_exit_side = "up"
    state.last_exit_age_sec = 150.0
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=170.0,
        remaining_sec=130.0,
        s_price=100.2,
        k_price=100.0,
        sigma_eff=0.55,
        up_ask_avg=0.40,
        up_ask_limit=0.40,
        up_best_ask=0.40,
        up_ask_depth_ok=True,
        down_ask_avg=0.90,
        down_ask_limit=0.90,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    decision = evaluate_entry(snap, state, cfg)

    assert decision.action == "enter"
    assert decision.side == "up"


def test_logic_decay_exit_cooldown_is_side_specific() -> None:
    cfg = EdgeConfig(core_required_edge=0.05, logic_decay_reentry_cooldown_sec=30.0)
    state = StrategyState(current_market_slug="m1")
    state.last_exit_reason = "logic_decay_exit"
    state.last_exit_side = "up"
    state.last_exit_age_sec = 150.0
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=170.0,
        remaining_sec=130.0,
        s_price=99.8,
        k_price=100.0,
        sigma_eff=0.55,
        up_ask_avg=0.90,
        up_ask_limit=0.90,
        up_ask_depth_ok=True,
        down_ask_avg=0.40,
        down_ask_limit=0.40,
        down_best_ask=0.40,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    decision = evaluate_entry(snap, state, cfg)

    assert decision.action == "enter"
    assert decision.side == "down"


def test_entry_rejects_cross_source_divergence() -> None:
    cfg = EdgeConfig(core_required_edge=0.05, cross_source_max_bps=5.0)
    state = StrategyState(current_market_slug="m1")
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=180.0,
        remaining_sec=120.0,
        s_price=100.20,
        k_price=100.0,
        sigma_eff=0.6,
        source_spread_bps=8.0,
        up_ask_avg=0.30,
        up_ask_limit=0.30,
        up_best_ask=0.30,
        up_ask_depth_ok=True,
        down_ask_avg=0.90,
        down_ask_limit=0.90,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    decision = evaluate_entry(snap, state, cfg)

    assert decision.action == "skip"
    assert decision.reason == "source_divergence"


def test_entry_uses_phase_edges_and_disables_late() -> None:
    cfg = EdgeConfig(early_required_edge=0.10, core_required_edge=0.06, entry_start_age_sec=40.0)
    state = StrategyState(current_market_slug="m1")
    base = dict(
        market_slug="m1",
        remaining_sec=180.0,
        s_price=100.0,
        k_price=100.0,
        sigma_eff=2.0,
        up_ask_avg=0.43,
        up_ask_limit=0.43,
        up_best_ask=0.43,
        down_ask_avg=0.9,
        down_ask_limit=0.9,
        up_ask_depth_ok=True,
        down_ask_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    early = evaluate_entry(MarketSnapshot(age_sec=80.0, **base), state, cfg)
    assert early.action == "skip"
    assert early.reason == "edge_too_small"
    assert early.phase == "early"
    assert early.required_edge == 0.10

    core_boundary_phase = required_edge_for_entry(MarketSnapshot(age_sec=120.0, **base), cfg)
    assert core_boundary_phase.phase == "core"
    assert core_boundary_phase.required_edge == 0.06

    core = evaluate_entry(MarketSnapshot(age_sec=180.0, **base), state, cfg)
    assert core.action == "enter"
    assert core.phase == "core"
    assert core.required_edge == 0.06

    late = evaluate_entry(MarketSnapshot(age_sec=250.0, **base), state, cfg)
    assert late.action == "skip"
    assert late.reason == "late_entry_disabled"
    assert late.phase == "late_disabled"

    final_no_entry = evaluate_entry(MarketSnapshot(age_sec=230.0, remaining_sec=30.0, **{k: v for k, v in base.items() if k != "remaining_sec"}), state, cfg)
    assert final_no_entry.reason == "final_no_entry"
    assert final_no_entry.phase == "final_no_entry"


def test_entry_phase_boundaries_are_configurable() -> None:
    cfg = EdgeConfig(
        entry_start_age_sec=60.0,
        entry_end_age_sec=260.0,
        early_to_core_age_sec=90.0,
        core_to_late_age_sec=210.0,
        early_required_edge=0.11,
        core_required_edge=0.07,
        late_entry_enabled=False,
    )
    base = dict(
        market_slug="m1",
        remaining_sec=180.0,
        s_price=100.0,
        k_price=100.0,
        sigma_eff=2.0,
    )

    assert required_edge_for_entry(MarketSnapshot(age_sec=70.0, **base), cfg).phase == "early"
    core = required_edge_for_entry(MarketSnapshot(age_sec=90.0, **base), cfg)
    late = required_edge_for_entry(MarketSnapshot(age_sec=220.0, **base), cfg)

    assert core.phase == "core"
    assert core.required_edge == 0.07
    assert late.phase == "late_disabled"


def test_dynamic_entry_allows_early_fast_and_strong_moves_only() -> None:
    cfg = EdgeConfig(
        entry_start_age_sec=100.0,
        early_required_edge=0.16,
        core_required_edge=0.14,
        dynamic_entry_enabled=True,
        fast_move_entry_start_age_sec=70.0,
        fast_move_min_abs_sk_usd=80.0,
        fast_move_required_edge=0.22,
        strong_move_entry_start_age_sec=60.0,
        strong_move_min_abs_sk_usd=120.0,
        strong_move_required_edge=0.24,
    )
    base = dict(
        market_slug="m1",
        remaining_sec=220.0,
        k_price=100.0,
        sigma_eff=0.6,
    )

    ordinary_early = required_edge_for_entry(MarketSnapshot(age_sec=65.0, s_price=101.0, **base), cfg)
    strong = required_edge_for_entry(MarketSnapshot(age_sec=65.0, s_price=221.0, **base), cfg)
    fast = required_edge_for_entry(MarketSnapshot(age_sec=75.0, s_price=181.0, **base), cfg)
    regular = required_edge_for_entry(MarketSnapshot(age_sec=100.0, s_price=101.0, **base), cfg)

    assert ordinary_early.allowed is False
    assert ordinary_early.phase == "outside_window"
    assert strong.allowed is True
    assert strong.phase == "strong_move"
    assert strong.required_edge == 0.24
    assert fast.allowed is True
    assert fast.phase == "fast_move"
    assert fast.required_edge == 0.22
    assert regular.allowed is True
    assert regular.phase == "early"
    assert regular.required_edge == 0.16


def test_exit_logic_decay_and_market_overprice() -> None:
    cfg = EdgeConfig(model_decay_buffer=0.02, overprice_buffer=0.02)
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.70,
        filled_shares=10.0,
        entry_model_prob=0.78,
        entry_edge=0.08,
    )
    decay_snap = MarketSnapshot(
        market_slug="m1",
        age_sec=130.0,
        remaining_sec=170.0,
        s_price=99.9,
        k_price=100.0,
        sigma_eff=0.2,
        up_ask_avg=0.30,
        down_ask_avg=0.70,
        up_bid_avg=0.61,
        down_bid_avg=0.69,
        up_bid_limit=0.60,
        down_bid_limit=0.68,
        up_bid_depth_ok=True,
        down_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    decay = evaluate_exit(decay_snap, pos, cfg)
    assert decay.action == "exit"
    assert decay.reason == "logic_decay_exit"

    rich_bid_snap = MarketSnapshot(
        market_slug="m1",
        age_sec=130.0,
        remaining_sec=170.0,
        s_price=100.0,
        k_price=100.0,
        sigma_eff=2.0,
        up_ask_avg=0.53,
        down_ask_avg=0.53,
        up_bid_avg=0.60,
        down_bid_avg=0.47,
        up_bid_limit=0.59,
        down_bid_limit=0.46,
        up_bid_depth_ok=True,
        down_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )

    profitable_pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.45,
        filled_shares=10.0,
        entry_model_prob=0.55,
        entry_edge=0.10,
    )
    overprice = evaluate_exit(rich_bid_snap, profitable_pos, cfg)
    assert overprice.action == "exit"
    assert overprice.reason == "market_overprice_exit"


def test_exit_waits_on_stale_book_before_final_window() -> None:
    cfg = EdgeConfig(final_force_exit_remaining_sec=30.0, max_book_age_ms=1000.0)
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=120.0,
        entry_avg_price=0.40,
        filled_shares=10.0,
        entry_model_prob=0.60,
        entry_edge=0.20,
    )
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=150.0,
        remaining_sec=90.0,
        s_price=100.1,
        k_price=100.0,
        sigma_eff=0.55,
        up_bid_avg=0.41,
        up_bid_limit=0.40,
        up_bid_depth_ok=True,
        up_book_age_ms=8_000.0,
        down_book_age_ms=8_000.0,
    )

    decision = evaluate_exit(snap, pos, cfg)

    assert decision.action == "hold"
    assert decision.reason == "stale_book_wait"
    assert decision.model_prob is not None


def test_exit_final_force_uses_stale_book_near_window_end() -> None:
    cfg = EdgeConfig(final_force_exit_remaining_sec=30.0, max_book_age_ms=1000.0)
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="down",
        token_id="down-token",
        entry_time=250.0,
        entry_avg_price=0.40,
        filled_shares=10.0,
        entry_model_prob=0.60,
        entry_edge=0.20,
    )
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=275.0,
        remaining_sec=25.0,
        s_price=99.9,
        k_price=100.0,
        sigma_eff=0.55,
        down_bid_avg=0.42,
        down_bid_limit=0.41,
        down_bid_depth_ok=True,
        up_book_age_ms=8_000.0,
        down_book_age_ms=8_000.0,
    )

    decision = evaluate_exit(snap, pos, cfg)

    assert decision.action == "exit"
    assert decision.reason == "final_force_exit"
    assert decision.limit_price == 0.41


def _divergence_position(side: str) -> PositionSnapshot:
    return PositionSnapshot(
        market_slug="m1",
        token_side=side,
        token_id=f"{side}-token",
        entry_time=100.0,
        entry_avg_price=0.40,
        filled_shares=10.0,
        entry_model_prob=0.60,
        entry_edge=0.20,
    )


def _divergence_snapshot(side: str, divergence_bps: float, age_sec: float = 130.0) -> MarketSnapshot:
    bids = {
        "up_bid_avg": 0.42,
        "up_bid_limit": 0.41,
        "up_bid_depth_ok": True,
        "down_bid_avg": 0.42,
        "down_bid_limit": 0.41,
        "down_bid_depth_ok": True,
    }
    return MarketSnapshot(
        market_slug="m1",
        age_sec=age_sec,
        remaining_sec=170.0,
        s_price=100.0,
        k_price=100.0,
        sigma_eff=2.0,
        polymarket_divergence_bps=divergence_bps,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
        **bids,
    )


def test_polymarket_divergence_exit_triggers_only_when_adverse() -> None:
    cfg = EdgeConfig(polymarket_divergence_exit_bps=3.0, polymarket_divergence_exit_min_age_sec=3.0)

    up_adverse = evaluate_exit(_divergence_snapshot("up", 3.2), _divergence_position("up"), cfg)
    down_adverse = evaluate_exit(_divergence_snapshot("down", -3.2), _divergence_position("down"), cfg)
    up_favorable = evaluate_exit(_divergence_snapshot("up", -8.0), _divergence_position("up"), cfg)
    down_favorable = evaluate_exit(_divergence_snapshot("down", 8.0), _divergence_position("down"), cfg)

    assert up_adverse.action == "exit"
    assert up_adverse.reason == "polymarket_divergence_exit"
    assert up_adverse.polymarket_divergence_bps == 3.2
    assert down_adverse.action == "exit"
    assert down_adverse.reason == "polymarket_divergence_exit"
    assert down_adverse.polymarket_divergence_bps == -3.2
    assert up_favorable.reason != "polymarket_divergence_exit"
    assert down_favorable.reason != "polymarket_divergence_exit"


def test_polymarket_divergence_exit_respects_age_and_disable_switch() -> None:
    young_cfg = EdgeConfig(polymarket_divergence_exit_bps=3.0, polymarket_divergence_exit_min_age_sec=3.0)
    disabled_cfg = EdgeConfig(polymarket_divergence_exit_bps=0.0, polymarket_divergence_exit_min_age_sec=3.0)
    young_pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=128.0,
        entry_avg_price=0.40,
        filled_shares=10.0,
        entry_model_prob=0.60,
        entry_edge=0.20,
    )

    too_young = evaluate_exit(_divergence_snapshot("up", 10.0, age_sec=130.0), young_pos, young_cfg)
    disabled = evaluate_exit(_divergence_snapshot("up", 10.0), _divergence_position("up"), disabled_cfg)

    assert too_young.reason != "polymarket_divergence_exit"
    assert disabled.reason != "polymarket_divergence_exit"


def test_defensive_take_profit_requires_three_second_stagnation_history() -> None:
    cfg = EdgeConfig(defensive_profit_min=0.03, prob_stagnation_epsilon=0.002)
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.50,
        filled_shares=10.0,
        entry_model_prob=0.60,
        entry_edge=0.10,
    )
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=245.0,
        remaining_sec=55.0,
        s_price=100.1,
        k_price=100.0,
        sigma_eff=0.55,
        up_bid_avg=0.54,
        up_bid_limit=0.53,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )
    state = StrategyState(current_market_slug="m1")

    no_history = evaluate_exit(snap, pos, cfg, state)
    assert no_history.reason != "defensive_take_profit"
    assert no_history.prob_stagnant is False
    assert no_history.prob_delta_3s is None

    state.prob_history = [(241.5, no_history.model_prob + 0.001)]
    stagnant = evaluate_exit(snap, pos, cfg, state)
    assert stagnant.action == "exit"
    assert stagnant.reason == "defensive_take_profit"
    assert stagnant.prob_stagnant is True
    assert stagnant.prob_delta_3s is not None

    state.prob_history = [(241.5, no_history.model_prob - 0.02)]
    rising = evaluate_exit(snap, pos, cfg, state)
    assert rising.reason != "defensive_take_profit"
    assert rising.prob_stagnant is False


def test_late_profit_protection_and_final_force_exit() -> None:
    cfg = EdgeConfig()
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.50,
        filled_shares=10.0,
        entry_model_prob=0.60,
        entry_edge=0.10,
    )

    force_at_30s = evaluate_exit(MarketSnapshot(
        market_slug="m1",
        age_sec=272.0,
        remaining_sec=28.0,
        s_price=100.1,
        k_price=100.0,
        sigma_eff=0.55,
        up_bid_avg=0.515,
        up_bid_limit=0.51,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    ), pos, cfg, StrategyState(current_market_slug="m1"))
    assert force_at_30s.reason == "final_force_exit"

    force = evaluate_exit(MarketSnapshot(
        market_slug="m1",
        age_sec=288.0,
        remaining_sec=12.0,
        s_price=100.1,
        k_price=100.0,
        sigma_eff=0.55,
        up_bid_avg=0.70,
        up_bid_limit=0.69,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    ), pos, cfg, StrategyState(current_market_slug="m1"))
    assert force.reason == "final_force_exit"

    hold = evaluate_exit(MarketSnapshot(
        market_slug="m1",
        age_sec=288.0,
        remaining_sec=12.0,
        s_price=102.0,
        k_price=100.0,
        sigma_eff=0.2,
        up_bid_avg=0.98,
        up_bid_limit=0.96,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    ), pos, cfg, StrategyState(current_market_slug="m1"))
    assert hold.reason != "final_force_exit"


def test_profit_exit_bands_are_configurable() -> None:
    cfg = EdgeConfig(
        final_force_exit_remaining_sec=20.0,
        profit_protection_start_remaining_sec=20.0,
        profit_protection_end_remaining_sec=45.0,
        defensive_take_profit_start_remaining_sec=45.0,
        defensive_take_profit_end_remaining_sec=90.0,
        protection_profit_min=0.01,
        defensive_profit_min=0.03,
    )
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.50,
        filled_shares=10.0,
        entry_model_prob=0.60,
        entry_edge=0.10,
    )
    base = dict(
        market_slug="m1",
        age_sec=220.0,
        s_price=100.1,
        k_price=100.0,
        sigma_eff=0.55,
        up_bid_avg=0.54,
        up_bid_limit=0.53,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )
    state = StrategyState(current_market_slug="m1")
    state.prob_history = [(216.0, 0.90)]

    defensive = evaluate_exit(MarketSnapshot(remaining_sec=70.0, **base), pos, cfg, state)
    protected = evaluate_exit(MarketSnapshot(remaining_sec=40.0, **base), pos, cfg, state)

    assert defensive.reason == "defensive_take_profit"
    assert protected.reason == "profit_protection_exit"


def test_logic_decay_still_triggers_inside_last_sixty_seconds() -> None:
    cfg = EdgeConfig(model_decay_buffer=0.02, defensive_profit_min=0.03)
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.70,
        filled_shares=10.0,
        entry_model_prob=0.75,
        entry_edge=0.05,
    )
    decision = evaluate_exit(MarketSnapshot(
        market_slug="m1",
        age_sec=245.0,
        remaining_sec=55.0,
        s_price=99.95,
        k_price=100.0,
        sigma_eff=0.2,
        up_bid_avg=0.71,
        up_bid_limit=0.70,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    ), pos, cfg, StrategyState(current_market_slug="m1"))

    assert decision.reason == "logic_decay_exit"


def test_prob_drop_exit_triggers_on_fast_probability_drop() -> None:
    cfg = EdgeConfig(prob_drop_exit_window_sec=5.0, prob_drop_exit_threshold=0.06)
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.40,
        filled_shares=10.0,
        entry_model_prob=0.70,
        entry_edge=0.30,
    )
    state = StrategyState(current_market_slug="m1")
    state.prob_history = [(135.0, 0.75)]

    decision = evaluate_exit(MarketSnapshot(
        market_slug="m1",
        age_sec=140.0,
        remaining_sec=160.0,
        s_price=100.05,
        k_price=100.0,
        sigma_eff=0.5,
        up_bid_avg=0.41,
        up_bid_limit=0.40,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    ), pos, cfg, state)

    assert decision.action == "exit"
    assert decision.reason == "prob_drop_exit"
    assert decision.prob_drop_delta is not None
    assert decision.prob_drop_delta <= -0.06


def test_prob_drop_exit_does_not_fire_when_disabled_or_history_short() -> None:
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.40,
        filled_shares=10.0,
        entry_model_prob=0.70,
        entry_edge=0.30,
    )
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=140.0,
        remaining_sec=160.0,
        s_price=100.05,
        k_price=100.0,
        sigma_eff=0.5,
        up_bid_avg=0.41,
        up_bid_limit=0.40,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    )
    short_history = StrategyState(current_market_slug="m1")
    short_history.prob_history = [(136.0, 0.62)]

    enabled = evaluate_exit(snap, pos, EdgeConfig(prob_drop_exit_window_sec=5.0, prob_drop_exit_threshold=0.06), short_history)
    disabled = evaluate_exit(snap, pos, EdgeConfig(prob_drop_exit_window_sec=5.0, prob_drop_exit_threshold=0.0), short_history)

    assert enabled.reason != "prob_drop_exit"
    assert disabled.reason != "prob_drop_exit"


def test_prob_drop_exit_does_not_fire_while_above_entry_model_probability() -> None:
    cfg = EdgeConfig(prob_drop_exit_window_sec=5.0, prob_drop_exit_threshold=0.06)
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.40,
        filled_shares=10.0,
        entry_model_prob=0.60,
        entry_edge=0.20,
    )
    state = StrategyState(current_market_slug="m1")
    state.prob_history = [(135.0, 0.75)]

    decision = evaluate_exit(MarketSnapshot(
        market_slug="m1",
        age_sec=140.0,
        remaining_sec=160.0,
        s_price=100.05,
        k_price=100.0,
        sigma_eff=0.5,
        up_bid_avg=0.62,
        up_bid_limit=0.61,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    ), pos, cfg, state)

    assert decision.prob_drop_delta is not None
    assert decision.prob_drop_delta <= -0.06
    assert decision.reason != "prob_drop_exit"


def test_prob_drop_exit_has_priority_over_logic_decay() -> None:
    cfg = EdgeConfig(model_decay_buffer=0.02, prob_drop_exit_window_sec=5.0, prob_drop_exit_threshold=0.06)
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.70,
        filled_shares=10.0,
        entry_model_prob=0.80,
        entry_edge=0.10,
    )
    state = StrategyState(current_market_slug="m1")
    state.prob_history = [(135.0, 0.80)]

    decision = evaluate_exit(MarketSnapshot(
        market_slug="m1",
        age_sec=140.0,
        remaining_sec=160.0,
        s_price=99.95,
        k_price=100.0,
        sigma_eff=0.2,
        up_bid_avg=0.61,
        up_bid_limit=0.60,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    ), pos, cfg, state)

    assert decision.reason == "prob_drop_exit"


def test_market_disagrees_exit_triggers_when_bid_ratio_breaks_late() -> None:
    cfg = EdgeConfig(
        market_disagrees_exit_threshold=0.20,
        market_disagrees_exit_max_remaining_sec=60.0,
        market_disagrees_exit_min_loss=0.03,
        market_disagrees_exit_min_age_sec=3.0,
        market_disagrees_exit_max_profit=0.01,
    )
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=100.0,
        entry_avg_price=0.35,
        filled_shares=10.0,
        entry_model_prob=0.60,
        entry_edge=0.25,
    )

    decision = evaluate_exit(MarketSnapshot(
        market_slug="m1",
        age_sec=145.0,
        remaining_sec=45.0,
        s_price=100.0,
        k_price=100.0,
        sigma_eff=0.6,
        up_bid_avg=0.10,
        up_bid_limit=0.10,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    ), pos, cfg)

    assert decision.action == "exit"
    assert decision.reason == "market_disagrees_exit"
    assert decision.market_disagreement is not None
    assert decision.market_disagreement >= 0.20


def test_market_disagrees_exit_does_not_fire_while_profitable() -> None:
    cfg = EdgeConfig(
        market_disagrees_exit_threshold=0.20,
        market_disagrees_exit_max_remaining_sec=60.0,
        market_disagrees_exit_min_loss=0.03,
        market_disagrees_exit_min_age_sec=3.0,
        market_disagrees_exit_max_profit=0.01,
    )
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=100.0,
        entry_avg_price=0.35,
        filled_shares=10.0,
        entry_model_prob=0.60,
        entry_edge=0.25,
    )

    decision = evaluate_exit(MarketSnapshot(
        market_slug="m1",
        age_sec=145.0,
        remaining_sec=45.0,
        s_price=100.05,
        k_price=100.0,
        sigma_eff=0.6,
        up_bid_avg=0.37,
        up_bid_limit=0.36,
        up_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    ), pos, cfg)

    assert decision.reason != "market_disagrees_exit"


def test_market_disagrees_exit_does_not_fire_when_model_prob_improves() -> None:
    cfg = EdgeConfig(
        market_disagrees_exit_threshold=0.20,
        market_disagrees_exit_max_remaining_sec=60.0,
        market_disagrees_exit_min_loss=0.03,
        market_disagrees_exit_min_age_sec=3.0,
        market_disagrees_exit_max_profit=0.01,
    )
    pos = PositionSnapshot(
        market_slug="m1",
        token_side="down",
        token_id="down-token",
        entry_time=100.0,
        entry_avg_price=0.38,
        filled_shares=10.0,
        entry_model_prob=0.536,
        entry_edge=0.156,
    )

    decision = evaluate_exit(MarketSnapshot(
        market_slug="m1",
        age_sec=108.0,
        remaining_sec=54.0,
        s_price=99.995,
        k_price=100.0,
        sigma_eff=0.389,
        down_bid_avg=0.24,
        down_bid_limit=0.24,
        down_bid_depth_ok=True,
        up_book_age_ms=20.0,
        down_book_age_ms=20.0,
    ), pos, cfg)

    assert decision.reason != "market_disagrees_exit"


def test_state_records_window_settlement_pnl() -> None:
    state = StrategyState(current_market_slug="m1")
    state.record_entry(PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=1.0,
        entry_avg_price=0.40,
        filled_shares=10.0,
        entry_model_prob=0.70,
        entry_edge=0.30,
    ))

    pnl = state.record_settlement(winning_side="up")

    assert pnl == 6.0
    assert state.realized_pnl == 6.0
    assert state.open_position is None
