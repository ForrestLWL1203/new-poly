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
from new_poly.strategy.probability import binary_probabilities, binary_probability
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
    assert decision.depth_limit_price == 0.41
    assert decision.limit_price is not None
    assert math.isclose(decision.limit_price, (decision.model_prob or 0.0) - 0.05, abs_tol=1e-12)
    assert decision.phase == "core"
    assert decision.required_edge == 0.05


def test_entry_rejects_depth_limit_above_formula_cap() -> None:
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

    assert decision.action == "skip"
    assert decision.reason == "edge_too_small"


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

    protection = evaluate_exit(MarketSnapshot(
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
    assert protection.reason == "profit_protection_exit"
    assert protection.profit_now is not None and protection.profit_now >= 0.01

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
