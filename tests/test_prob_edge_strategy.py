from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.strategy.prob_edge import (
    EdgeConfig,
    MarketSnapshot,
    PositionSnapshot,
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
    cfg = EdgeConfig(required_edge=0.05)
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
    assert decision.limit_price == 0.41


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
