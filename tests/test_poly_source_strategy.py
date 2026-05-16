from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.strategy.poly_source import PolySourceConfig, entry_amount_usd, evaluate_poly_entry, evaluate_poly_exit
from new_poly.strategy.types import MarketSnapshot
from new_poly.strategy.state import PositionSnapshot, StrategyState


def _snapshot(
    *,
    poly_price: float,
    k_price: float = 100.0,
    return_bps: float = 0.4,
    lookback: float = 3.0,
    up_ask: float = 0.60,
    down_ask: float = 0.60,
    up_bid: float = 0.59,
    down_bid: float = 0.39,
    up_bid_limit: float | None = None,
    down_bid_limit: float | None = None,
    up_depth_ok: bool = True,
    down_depth_ok: bool = True,
    age: float = 130.0,
    remaining: float = 170.0,
    binance_price: float = 99.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        market_slug="m1",
        age_sec=age,
        remaining_sec=remaining,
        s_price=binance_price,
        k_price=k_price,
        sigma_eff=None,
        up_best_ask=up_ask,
        up_ask_avg=up_ask,
        up_ask_limit=up_ask,
        up_bid_avg=up_bid,
        up_bid_limit=up_bid if up_bid_limit is None else up_bid_limit,
        up_bid_depth_ok=up_depth_ok,
        up_book_age_ms=20.0,
        up_bid_age_ms=20.0,
        down_best_ask=down_ask,
        down_ask_avg=down_ask,
        down_ask_limit=down_ask,
        down_bid_avg=down_bid,
        down_bid_limit=down_bid if down_bid_limit is None else down_bid_limit,
        down_bid_depth_ok=down_depth_ok,
        down_book_age_ms=20.0,
        down_bid_age_ms=20.0,
        polymarket_price=poly_price,
        polymarket_price_age_sec=1.0,
        polymarket_return_1s_bps=return_bps if lookback == 1.0 else 0.0,
        polymarket_return_3s_bps=return_bps if lookback == 3.0 else 0.0,
        polymarket_return_5s_bps=return_bps if lookback == 5.0 else 0.0,
        polymarket_return_10s_bps=return_bps if lookback == 10.0 else 0.0,
        polymarket_return_15s_bps=return_bps if lookback == 15.0 else 0.0,
        poly_return_since_entry_start_bps=1.2,
    )


def _observe_direction(state: StrategyState, cfg: PolySourceConfig, prices: list[tuple[float, float]]) -> None:
    for age, price in prices:
        state.record_direction_observation(_snapshot(poly_price=price, age=age), cfg)


def test_poly_source_enters_up_when_reference_and_rolling_return_are_up() -> None:
    cfg = PolySourceConfig(
        poly_reference_distance_bps=0.5,
        poly_trend_lookback_sec=3.0,
        poly_return_bps=0.3,
        max_entry_ask=0.65,
    )
    state = StrategyState(current_market_slug="m1")

    decision = evaluate_poly_entry(_snapshot(poly_price=100.04, return_bps=0.4, binance_price=99.0), state, cfg)

    assert decision.action == "enter"
    assert decision.side == "up"
    assert decision.reason == "poly_edge"
    assert decision.price == 0.60
    assert decision.limit_price == pytest.approx(1.0)
    assert decision.poly_reference_distance_bps == pytest.approx(4.0)
    assert decision.poly_return_bps == pytest.approx(0.4)
    assert decision.poly_entry_score is not None


def test_poly_source_direction_observation_ignores_first_30s_crosses() -> None:
    cfg = PolySourceConfig(direction_min_observed_sec=20.0)
    state = StrategyState(current_market_slug="m1")

    _observe_direction(state, cfg, [(10.0, 99.98), (20.0, 100.02), (30.0, 100.02), (60.0, 100.03)])

    direction = state.direction_state
    assert direction is not None
    assert direction.dominant_side == "up"
    assert direction.cross_count_total == 0
    assert direction.quality == "stable"


def test_poly_source_skips_choppy_direction_even_when_entry_score_passes() -> None:
    cfg = PolySourceConfig(poly_reference_distance_bps=0.5, poly_return_bps=0.3, max_entry_ask=0.65)
    state = StrategyState(current_market_slug="m1")
    _observe_direction(state, cfg, [(30.0, 100.02), (60.0, 99.98), (90.0, 100.02), (120.0, 99.98)])

    decision = evaluate_poly_entry(_snapshot(poly_price=100.04, return_bps=0.4, age=130.0), state, cfg)

    assert decision.action == "skip"
    assert decision.reason == "direction_choppy"
    assert decision.side == "up"


def test_poly_source_marks_direction_choppy_after_too_many_total_crosses() -> None:
    cfg = PolySourceConfig(
        direction_choppy_recent_crosses=99,
        direction_choppy_total_crosses=4,
        direction_choppy_cross_rate_per_min=99.0,
    )
    state = StrategyState(current_market_slug="m1")

    _observe_direction(
        state,
        cfg,
        [(30.0, 100.02), (60.0, 99.98), (90.0, 100.02), (120.0, 99.98), (150.0, 100.02)],
    )

    assert state.direction_state is not None
    assert state.direction_state.cross_count_total == 4
    assert state.direction_state.quality == "choppy"


def test_poly_source_skips_fresh_cross_direction() -> None:
    cfg = PolySourceConfig(poly_reference_distance_bps=0.5, poly_return_bps=0.3, max_entry_ask=0.65)
    state = StrategyState(current_market_slug="m1")
    _observe_direction(state, cfg, [(30.0, 99.98), (70.0, 99.97), (120.0, 100.02)])

    decision = evaluate_poly_entry(_snapshot(poly_price=100.04, return_bps=0.4, age=130.0), state, cfg)

    assert decision.action == "skip"
    assert decision.reason == "direction_fresh_cross"
    assert decision.side == "up"


def test_poly_source_uses_direction_state_side_not_single_tick_side() -> None:
    cfg = PolySourceConfig(poly_reference_distance_bps=0.5, poly_return_bps=0.3, max_entry_ask=0.75)
    state = StrategyState(current_market_slug="m1")
    _observe_direction(state, cfg, [(30.0, 100.04), (70.0, 100.04), (110.0, 100.04)])

    decision = evaluate_poly_entry(
        _snapshot(poly_price=99.98, return_bps=-0.4, up_ask=0.60, down_ask=0.60, age=130.0),
        state,
        cfg,
    )

    assert decision.action == "skip"
    assert decision.reason == "direction_fresh_cross"
    assert decision.side == "down"


def test_poly_source_entry_amount_requires_stable_direction_for_full_size() -> None:
    cfg = PolySourceConfig(entry_size_score_mid=5.6, entry_size_score_full=6.3)

    assert entry_amount_usd(
        1.0,
        score=7.0,
        entry_price=0.55,
        reference_distance_bps=4.0,
        direction_quality="acceptable",
        cfg=cfg,
        age_sec=160.0,
    ) == pytest.approx(2.0)
    assert entry_amount_usd(
        1.0,
        score=7.0,
        entry_price=0.55,
        reference_distance_bps=4.0,
        direction_quality="stable",
        direction_cross_count_recent=0,
        cfg=cfg,
        age_sec=160.0,
    ) == pytest.approx(3.0)


def test_poly_source_entry_amount_scales_linearly_by_direction_confidence() -> None:
    cfg = PolySourceConfig(
        direction_confidence_enabled=True,
        min_direction_confidence=0.85,
        entry_size_full_confidence=0.95,
        entry_size_full_multiplier=3.0,
    )

    assert entry_amount_usd(1.0, score=0.0, entry_price=0.60, direction_confidence=0.84, cfg=cfg) == pytest.approx(1.0)
    assert entry_amount_usd(1.0, score=0.0, entry_price=0.60, direction_confidence=0.85, cfg=cfg) == pytest.approx(1.0)
    assert entry_amount_usd(1.0, score=0.0, entry_price=0.60, direction_confidence=0.90, cfg=cfg) == pytest.approx(2.0)
    assert entry_amount_usd(1.0, score=0.0, entry_price=0.60, direction_confidence=0.95, cfg=cfg) == pytest.approx(3.0)
    assert entry_amount_usd(1.0, score=0.0, entry_price=0.60, direction_confidence=0.99, cfg=cfg) == pytest.approx(3.0)


def test_poly_source_records_prior_same_side_window_streak() -> None:
    state = StrategyState(current_market_slug="btc-updown-5m-1000")

    state.record_window_settlement("btc-updown-5m-1000", "down")
    state.record_window_settlement("btc-updown-5m-1300", "down")
    state.record_window_settlement("btc-updown-5m-1600", "down")

    assert state.prior_same_side_streak_len == 3
    assert state.prior_same_side_streak_side == "down"

    state.record_window_settlement("btc-updown-5m-1900", "up")

    assert state.prior_same_side_streak_len == 1
    assert state.prior_same_side_streak_side == "up"


def test_poly_source_high_confidence_direction_can_enter_with_low_entry_score() -> None:
    cfg = PolySourceConfig(
        poly_reference_distance_bps=0.5,
        poly_return_bps=0.0,
        max_entry_ask=0.75,
        min_poly_entry_score=9.0,
        direction_confidence_enabled=True,
        min_direction_confidence=0.85,
        direction_confidence_score_override=True,
    )
    state = StrategyState(current_market_slug="btc-updown-5m-1900")
    for slug in ("btc-updown-5m-1000", "btc-updown-5m-1300", "btc-updown-5m-1600"):
        state.record_window_settlement(slug, "up")
    _observe_direction(state, cfg, [(30.0, 100.02), (70.0, 100.03), (120.0, 100.035)])

    decision = evaluate_poly_entry(_snapshot(poly_price=100.035, return_bps=0.0, up_ask=0.72, age=130.0), state, cfg)

    assert decision.action == "enter"
    assert decision.side == "up"
    assert decision.direction_confidence is not None
    assert decision.direction_confidence >= 0.85
    assert decision.prior_streak_len == 3
    assert decision.prior_streak_side == "up"


def test_poly_source_low_confidence_direction_is_blocked_before_entry_score() -> None:
    cfg = PolySourceConfig(
        poly_reference_distance_bps=0.5,
        poly_return_bps=0.0,
        max_entry_ask=0.75,
        min_poly_entry_score=0.0,
        direction_confidence_enabled=True,
        min_direction_confidence=0.85,
    )
    state = StrategyState(current_market_slug="m1")
    _observe_direction(state, cfg, [(30.0, 100.01), (70.0, 100.015), (120.0, 100.016)])

    decision = evaluate_poly_entry(_snapshot(poly_price=100.016, return_bps=0.0, up_ask=0.72, age=130.0), state, cfg)

    assert decision.action == "skip"
    assert decision.reason == "direction_confidence_too_low"
    assert decision.direction_confidence is not None
    assert decision.direction_confidence < 0.85


def test_poly_source_config_exposes_only_active_single_source_exit_knobs() -> None:
    cfg = PolySourceConfig()
    fields = set(cfg.__dataclass_fields__)

    assert len(fields) < 60
    assert all(not name.startswith("legacy_") for name in fields)
    assert "extreme_loss_ratio" in fields
    assert "late_ev_exit_enabled" in fields
    assert "direction_confidence_enabled" in fields



def test_poly_source_reference_distance_cap_defaults_off() -> None:
    cfg = PolySourceConfig(poly_reference_distance_bps=0.5, poly_return_bps=0.3, max_entry_ask=0.75)
    state = StrategyState(current_market_slug="m1")

    decision = evaluate_poly_entry(
        _snapshot(poly_price=100.06, return_bps=1.0, up_ask=0.60),
        state,
        cfg,
    )

    assert decision.action == "enter"
    assert decision.poly_reference_distance_bps == pytest.approx(6.0)


def test_poly_source_skips_entry_when_reference_distance_exceeds_cap() -> None:
    cfg = PolySourceConfig(
        poly_reference_distance_bps=0.5,
        max_poly_reference_distance_bps=4.0,
        poly_return_bps=0.3,
        max_entry_ask=0.75,
    )
    state = StrategyState(current_market_slug="m1")

    decision = evaluate_poly_entry(
        _snapshot(poly_price=100.05, return_bps=1.0, up_ask=0.60),
        state,
        cfg,
    )

    assert decision.action == "skip"
    assert decision.reason == "poly_reference_distance_too_high"
    assert decision.side == "up"
    assert decision.poly_reference_distance_bps == pytest.approx(5.0)


def test_poly_source_enters_down_when_reference_and_rolling_return_are_down() -> None:
    cfg = PolySourceConfig(poly_reference_distance_bps=0.5, poly_return_bps=0.3, max_entry_ask=0.65)
    state = StrategyState(current_market_slug="m1")

    decision = evaluate_poly_entry(
        _snapshot(poly_price=99.96, return_bps=-0.4, up_ask=0.72, down_ask=0.61, up_bid=0.28, down_bid=0.60),
        state,
        cfg,
    )

    assert decision.action == "enter"
    assert decision.side == "down"
    assert decision.poly_reference_distance_bps == pytest.approx(4.0)
    assert decision.poly_return_bps == pytest.approx(0.4)


@pytest.mark.parametrize("lookback", [1.0, 3.0, 5.0, 10.0, 15.0])
def test_poly_source_uses_configured_rolling_return_lookback(lookback: float) -> None:
    cfg = PolySourceConfig(poly_trend_lookback_sec=lookback, poly_return_bps=0.3, max_entry_ask=0.65)
    state = StrategyState(current_market_slug="m1")

    decision = evaluate_poly_entry(_snapshot(poly_price=100.02, return_bps=0.4, lookback=lookback), state, cfg)

    assert decision.action == "enter"
    assert decision.poly_trend_lookback_sec == lookback
    assert decision.poly_return_bps == pytest.approx(0.4)


@pytest.mark.parametrize(
    ("snap", "reason"),
    [
        (_snapshot(poly_price=100.004, return_bps=0.4), "poly_reference_not_confirmed"),
        (_snapshot(poly_price=100.04, return_bps=-0.4), "poly_trend_not_confirmed"),
        (_snapshot(poly_price=100.04, return_bps=0.4, up_ask=0.66), "poly_ask_too_high"),
    ],
)
def test_poly_source_skips_weak_entry_conditions(snap: MarketSnapshot, reason: str) -> None:
    cfg = PolySourceConfig(poly_reference_distance_bps=0.5, poly_return_bps=0.3, max_entry_ask=0.65)

    decision = evaluate_poly_entry(snap, StrategyState(current_market_slug="m1"), cfg)

    assert decision.action == "skip"
    assert decision.reason == reason


def test_poly_source_score_threshold_can_block_entry() -> None:
    cfg = PolySourceConfig(
        poly_reference_distance_bps=0.5,
        poly_return_bps=0.3,
        max_entry_ask=0.65,
        min_poly_entry_score=9.0,
    )

    decision = evaluate_poly_entry(_snapshot(poly_price=100.04, return_bps=0.4), StrategyState(current_market_slug="m1"), cfg)

    assert decision.action == "skip"
    assert decision.reason == "poly_score_too_low"


def test_poly_source_entry_amount_scales_by_score() -> None:
    cfg = PolySourceConfig(
        entry_size_score_mid=6.0,
        entry_size_score_full=6.5,
        entry_size_full_min_age_sec=150.0,
    )

    assert entry_amount_usd(1.0, score=5.99, entry_price=0.55, reference_distance_bps=4.0, cfg=cfg) == pytest.approx(1.0)
    assert entry_amount_usd(1.0, score=6.0, entry_price=0.55, reference_distance_bps=4.0, cfg=cfg) == pytest.approx(2.0)
    assert entry_amount_usd(1.0, score=6.5, entry_price=0.55, reference_distance_bps=4.0, cfg=cfg) == pytest.approx(3.0)
    assert entry_amount_usd(1.0, score=7.0, entry_price=0.55, reference_distance_bps=4.0, cfg=cfg, age_sec=149.0) == pytest.approx(2.0)
    assert entry_amount_usd(1.0, score=7.0, entry_price=0.55, reference_distance_bps=4.0, cfg=cfg, age_sec=150.0) == pytest.approx(3.0)
    assert entry_amount_usd(1.0, score=7.5, entry_price=0.70, reference_distance_bps=4.0, cfg=cfg) == pytest.approx(3.0)


def test_poly_source_entry_amount_requires_reference_depth_for_size_increase() -> None:
    cfg = PolySourceConfig(
        entry_size_score_mid=5.6,
        entry_size_score_full=6.3,
        entry_size_full_min_age_sec=150.0,
    )

    assert entry_amount_usd(1.0, score=5.8, entry_price=0.55, reference_distance_bps=2.9, cfg=cfg) == pytest.approx(1.0)
    assert entry_amount_usd(1.0, score=5.8, entry_price=0.55, reference_distance_bps=3.0, cfg=cfg) == pytest.approx(2.0)
    assert entry_amount_usd(1.0, score=6.5, entry_price=0.55, reference_distance_bps=3.4, cfg=cfg, age_sec=160.0) == pytest.approx(2.0)
    assert entry_amount_usd(1.0, score=6.5, entry_price=0.55, reference_distance_bps=3.5, cfg=cfg, age_sec=160.0) == pytest.approx(3.0)


def test_poly_source_entry_score_caps_overextended_reference_distance() -> None:
    cfg = PolySourceConfig(poly_reference_distance_bps=0.5, poly_return_bps=0.3, max_entry_ask=0.75)
    state = StrategyState(current_market_slug="m1")

    ideal = evaluate_poly_entry(_snapshot(poly_price=100.08, return_bps=0.8, up_ask=0.60), state, cfg)
    overextended = evaluate_poly_entry(_snapshot(poly_price=100.15, return_bps=0.8, up_ask=0.60), state, cfg)

    assert ideal.action == "enter"
    assert overextended.action == "enter"
    assert overextended.poly_entry_score is not None
    assert ideal.poly_entry_score is not None
    assert overextended.poly_entry_score <= ideal.poly_entry_score
    assert overextended.poly_entry_overextended is True


def test_poly_source_entry_score_rewards_lower_ask_price_quality() -> None:
    cfg = PolySourceConfig(poly_reference_distance_bps=0.5, poly_return_bps=0.3, max_entry_ask=0.75)
    low_ask = evaluate_poly_entry(
        _snapshot(poly_price=100.06, return_bps=0.8, up_ask=0.52, up_bid=0.51),
        StrategyState(current_market_slug="m1"),
        cfg,
    )
    high_ask = evaluate_poly_entry(
        _snapshot(poly_price=100.06, return_bps=0.8, up_ask=0.72, up_bid=0.71),
        StrategyState(current_market_slug="m1"),
        cfg,
    )

    assert low_ask.action == "enter"
    assert high_ask.action == "enter"
    assert low_ask.poly_entry_score is not None
    assert high_ask.poly_entry_score is not None
    assert low_ask.poly_entry_price_quality_score > high_ask.poly_entry_price_quality_score
    assert low_ask.poly_entry_score > high_ask.poly_entry_score


def test_poly_source_entry_score_discounts_very_low_ask_price_quality() -> None:
    cfg = PolySourceConfig(poly_reference_distance_bps=0.5, poly_return_bps=0.3, max_entry_ask=0.75)

    very_low = evaluate_poly_entry(
        _snapshot(poly_price=100.06, return_bps=0.8, up_ask=0.21, up_bid=0.20),
        StrategyState(current_market_slug="m1"),
        cfg,
    )

    assert very_low.action == "enter"
    assert very_low.poly_entry_price_quality_score == pytest.approx(0.1)


def test_poly_source_entry_score_is_symmetric_for_down_side() -> None:
    cfg = PolySourceConfig(poly_reference_distance_bps=0.5, poly_return_bps=0.3, max_entry_ask=0.75)
    up = evaluate_poly_entry(
        _snapshot(poly_price=100.06, return_bps=0.8, up_ask=0.52, up_bid=0.51),
        StrategyState(current_market_slug="m1"),
        cfg,
    )
    down = evaluate_poly_entry(
        _snapshot(poly_price=99.94, return_bps=-0.8, down_ask=0.52, down_bid=0.51),
        StrategyState(current_market_slug="m1"),
        cfg,
    )

    assert up.action == "enter"
    assert down.action == "enter"
    assert up.poly_entry_score == pytest.approx(down.poly_entry_score)
    assert up.poly_entry_distance_score == pytest.approx(down.poly_entry_distance_score)
    assert up.poly_entry_trend_score == pytest.approx(down.poly_entry_trend_score)


def test_poly_source_entry_score_discounts_fresh_cross_trend() -> None:
    cfg = PolySourceConfig(poly_reference_distance_bps=0.5, poly_trend_lookback_sec=10.0, poly_return_bps=0.3, max_entry_ask=0.75)
    state = StrategyState(current_market_slug="m1")

    fresh_cross = evaluate_poly_entry(
        _snapshot(poly_price=100.029, return_bps=6.7, lookback=10.0, up_ask=0.58, up_bid=0.57),
        state,
        cfg,
    )
    continuation = evaluate_poly_entry(
        _snapshot(poly_price=100.029, return_bps=1.0, lookback=10.0, up_ask=0.58, up_bid=0.57),
        state,
        cfg,
    )

    assert fresh_cross.action == "enter"
    assert continuation.action == "enter"
    assert fresh_cross.poly_entry_trend_score is not None
    assert continuation.poly_entry_trend_score is not None
    assert fresh_cross.poly_entry_trend_score < continuation.poly_entry_trend_score


def test_poly_source_caps_entry_limit_price_at_max_fill_price() -> None:
    cfg = PolySourceConfig(
        poly_reference_distance_bps=0.5,
        poly_return_bps=0.3,
        max_entry_ask=0.75,
        max_entry_fill_price=0.75,
        buy_price_buffer_ticks=8,
    )

    decision = evaluate_poly_entry(_snapshot(poly_price=100.04, return_bps=0.4, up_ask=0.70), StrategyState(current_market_slug="m1"), cfg)

    assert decision.action == "enter"
    assert decision.price == 0.70
    assert decision.limit_price == pytest.approx(0.75)


def test_poly_source_uses_fill_cap_as_buy_limit_so_execution_dynamic_buffer_can_apply() -> None:
    cfg = PolySourceConfig(
        poly_reference_distance_bps=0.5,
        poly_return_bps=0.3,
        max_entry_ask=0.75,
        max_entry_fill_price=0.75,
        buy_price_buffer_ticks=2,
    )

    decision = evaluate_poly_entry(
        _snapshot(poly_price=100.04, return_bps=0.4, up_ask=0.60),
        StrategyState(current_market_slug="m1"),
        cfg,
    )

    assert decision.action == "enter"
    assert decision.price == 0.60
    assert decision.limit_price == pytest.approx(0.75)


def test_poly_source_skips_when_best_ask_exceeds_max_fill_price() -> None:
    cfg = PolySourceConfig(
        poly_reference_distance_bps=0.5,
        poly_return_bps=0.3,
        max_entry_ask=0.80,
        max_entry_fill_price=0.75,
        buy_price_buffer_ticks=8,
    )

    decision = evaluate_poly_entry(_snapshot(poly_price=100.04, return_bps=0.4, up_ask=0.76), StrategyState(current_market_slug="m1"), cfg)

    assert decision.action == "skip"
    assert decision.reason == "poly_fill_cap_exceeded"


def test_poly_source_late_ev_exit_sells_when_bid_exceeds_confidence_value() -> None:
    cfg = PolySourceConfig(
        late_ev_exit_enabled=True,
        late_ev_exit_min_hold_sec=60.0,
        late_ev_exit_remaining_sec=(120.0, 80.0, 45.0),
        late_ev_exit_margin=(0.18, 0.12, 0.06),
        direction_choppy_recent_crosses=99,
        direction_choppy_cross_rate_per_min=99.0,
    )
    state = StrategyState(current_market_slug="m1")
    _observe_direction(state, cfg, [(30.0, 100.03), (90.0, 100.03), (130.0, 100.03)])
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        130.0,
        0.65,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=3.0,
    )

    decision = evaluate_poly_exit(
        _snapshot(poly_price=99.99, return_bps=-0.2, up_bid=0.42, age=210.0, remaining=90.0),
        position,
        cfg,
        state,
    )

    assert decision.action == "exit"
    assert decision.reason == "late_ev_exit"
    assert decision.direction_confidence == pytest.approx(0.0)
    assert decision.reference_exit_reason == "reference_crossed_k"


def test_poly_source_late_ev_exit_ignores_shallow_short_reference_cross() -> None:
    cfg = PolySourceConfig(
        late_ev_exit_enabled=True,
        late_ev_exit_min_hold_sec=60.0,
        late_ev_exit_remaining_sec=(120.0, 80.0, 45.0),
        late_ev_exit_margin=(0.18, 0.12, 0.06),
        late_ev_exit_min_cross_bps=0.5,
        late_ev_exit_min_cross_sec=5.0,
        direction_choppy_recent_crosses=99,
        direction_choppy_cross_rate_per_min=99.0,
    )
    state = StrategyState(current_market_slug="m1")
    _observe_direction(state, cfg, [(30.0, 100.03), (90.0, 100.03), (130.0, 100.03)])
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        130.0,
        0.65,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=3.0,
    )

    first_cross = evaluate_poly_exit(
        _snapshot(poly_price=99.999, return_bps=-0.2, up_bid=0.42, age=210.0, remaining=90.0),
        position,
        cfg,
        state,
    )
    second_cross = evaluate_poly_exit(
        _snapshot(poly_price=99.999, return_bps=-0.2, up_bid=0.42, age=212.0, remaining=88.0),
        position,
        cfg,
        state,
    )

    assert first_cross.action == "hold"
    assert first_cross.reason == "poly_edge_intact"
    assert second_cross.action == "hold"
    assert second_cross.reason == "poly_edge_intact"
    assert second_cross.reference_exit_reason == "reference_crossed_k"


def test_poly_source_late_ev_exit_ignores_reference_floor_broken_without_cross() -> None:
    cfg = PolySourceConfig(
        late_ev_exit_enabled=True,
        late_ev_exit_min_hold_sec=60.0,
        late_ev_exit_remaining_sec=(120.0, 80.0, 45.0),
        late_ev_exit_margin=(0.18, 0.12, 0.06),
    )
    state = StrategyState(current_market_slug="m1")
    _observe_direction(state, cfg, [(30.0, 100.03), (90.0, 100.03), (130.0, 100.03)])
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        130.0,
        0.65,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=3.0,
    )

    decision = evaluate_poly_exit(
        _snapshot(poly_price=100.005, return_bps=-0.2, up_bid=0.92, age=278.0, remaining=22.0),
        position,
        cfg,
        state,
    )

    assert decision.action == "hold"
    assert decision.reason == "poly_edge_intact"
    assert decision.reference_exit_reason is None


def test_poly_source_late_ev_exit_holds_when_market_bid_is_not_worth_selling() -> None:
    cfg = PolySourceConfig(
        late_ev_exit_enabled=True,
        late_ev_exit_min_hold_sec=60.0,
        late_ev_exit_remaining_sec=(120.0, 80.0, 45.0),
        late_ev_exit_margin=(0.18, 0.12, 0.06),
        direction_choppy_recent_crosses=99,
        direction_choppy_cross_rate_per_min=99.0,
    )
    state = StrategyState(current_market_slug="m1")
    _observe_direction(state, cfg, [(30.0, 100.03), (90.0, 100.03), (130.0, 100.03)])
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        130.0,
        0.65,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=3.0,
    )

    decision = evaluate_poly_exit(
        _snapshot(poly_price=99.99, return_bps=-0.2, up_bid=0.08, age=210.0, remaining=90.0),
        position,
        cfg,
        state,
    )

    assert decision.action == "hold"
    assert decision.reason == "poly_edge_intact"
    assert decision.direction_confidence == pytest.approx(0.0)
    assert decision.reference_exit_reason == "reference_crossed_k"


def test_poly_source_late_ev_exit_waits_until_late_window_and_min_hold() -> None:
    cfg = PolySourceConfig(
        late_ev_exit_enabled=True,
        late_ev_exit_min_hold_sec=60.0,
        late_ev_exit_remaining_sec=(120.0, 80.0, 45.0),
        late_ev_exit_margin=(0.18, 0.12, 0.06),
        direction_choppy_recent_crosses=99,
        direction_choppy_cross_rate_per_min=99.0,
    )
    state = StrategyState(current_market_slug="m1")
    _observe_direction(state, cfg, [(30.0, 100.03), (90.0, 100.03), (130.0, 100.03)])
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        130.0,
        0.65,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=3.0,
    )

    too_early = evaluate_poly_exit(
        _snapshot(poly_price=99.99, return_bps=-0.2, up_bid=0.42, age=175.0, remaining=125.0),
        position,
        cfg,
        state,
    )
    too_young = evaluate_poly_exit(
        _snapshot(poly_price=99.99, return_bps=-0.2, up_bid=0.42, age=185.0, remaining=115.0),
        position,
        cfg,
        state,
    )

    assert too_early.action == "hold"
    assert too_early.reason == "poly_edge_intact"
    assert too_young.action == "hold"
    assert too_young.reason == "poly_edge_intact"


def test_poly_source_late_ev_exit_stops_near_settlement() -> None:
    cfg = PolySourceConfig(
        late_ev_exit_enabled=True,
        late_ev_exit_min_hold_sec=60.0,
        late_ev_exit_min_remaining_sec=45.0,
        late_ev_exit_remaining_sec=(140.0, 80.0, 45.0),
        late_ev_exit_margin=(0.18, 0.12, 0.06),
        late_ev_exit_min_cross_bps=0.5,
        late_ev_exit_min_cross_sec=5.0,
        direction_choppy_recent_crosses=99,
        direction_choppy_cross_rate_per_min=99.0,
    )
    state = StrategyState(current_market_slug="m1")
    _observe_direction(state, cfg, [(30.0, 100.03), (90.0, 100.03), (130.0, 100.03)])
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        130.0,
        0.65,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=3.0,
    )

    first_cross = evaluate_poly_exit(
        _snapshot(poly_price=99.999, return_bps=-0.2, up_bid=0.42, age=250.0, remaining=50.0),
        position,
        cfg,
        state,
    )
    confirmed_but_too_late = evaluate_poly_exit(
        _snapshot(poly_price=99.999, return_bps=-0.2, up_bid=0.42, age=256.0, remaining=44.0),
        position,
        cfg,
        state,
    )

    assert first_cross.action == "hold"
    assert first_cross.reason == "poly_edge_intact"
    assert confirmed_but_too_late.action == "hold"
    assert confirmed_but_too_late.reason == "poly_edge_intact"


def test_poly_source_extreme_loss_exit_needs_reference_break_unless_near_zero() -> None:
    cfg = PolySourceConfig()
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        100.0,
        0.60,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=2.0,
    )

    decision = evaluate_poly_exit(
        _snapshot(poly_price=100.025, return_bps=0.2, up_bid=0.14, age=140.0, remaining=160.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )

    near_zero = evaluate_poly_exit(
        _snapshot(poly_price=100.025, return_bps=0.2, up_bid=0.05, age=140.0, remaining=160.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )

    assert decision.action == "hold"
    assert decision.reason == "poly_edge_intact"
    assert near_zero.action == "exit"
    assert near_zero.reason == "extreme_loss_exit"


def test_poly_source_extreme_loss_exit_triggers_with_reference_break() -> None:
    cfg = PolySourceConfig()
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        100.0,
        0.60,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=2.0,
    )

    decision = evaluate_poly_exit(
        _snapshot(poly_price=99.99, return_bps=-0.2, up_bid=0.14, age=140.0, remaining=160.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )

    assert decision.action == "hold"
    assert decision.reason == "poly_edge_intact"


def test_poly_source_extreme_loss_exit_respects_strong_same_side_reference() -> None:
    cfg = PolySourceConfig()
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        100.0,
        0.60,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=2.0,
    )

    decision = evaluate_poly_exit(
        _snapshot(poly_price=100.025, return_bps=0.2, up_bid=0.24, age=140.0, remaining=160.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )

    assert decision.action == "hold"
    assert decision.reason == "poly_edge_intact"


def test_poly_source_hold_to_settlement_allows_high_price_winner() -> None:
    cfg = PolySourceConfig(
        hold_to_settlement_enabled=True,
        hold_to_settlement_min_profit_ratio=0.50,
        hold_to_settlement_min_bid_avg=0.80,
        hold_to_settlement_min_bid_limit=0.75,
        hold_to_settlement_min_reference_distance_bps=1.0,
        hold_to_settlement_min_poly_return_bps=0.0,
    )
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.60, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=100.03, return_bps=0.2, up_bid=0.91, up_bid_limit=0.86, remaining=20.0, age=280.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "hold"
    assert decision.reason == "hold_to_settlement"


def test_poly_source_hold_to_settlement_uses_reference_and_market_quality() -> None:
    cfg = PolySourceConfig(
        hold_to_settlement_enabled=True,
        hold_to_settlement_min_profit_ratio=0.50,
        hold_to_settlement_min_bid_avg=0.80,
        hold_to_settlement_min_bid_limit=0.75,
        hold_to_settlement_min_reference_distance_bps=1.0,
        hold_to_settlement_min_poly_return_bps=-0.1,
    )
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.60, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=100.03, return_bps=-0.05, up_bid=0.91, up_bid_limit=0.86, remaining=70.0, age=230.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "hold"
    assert decision.reason == "hold_to_settlement"



def test_poly_source_missing_depth_flag_holds_when_bid_exists() -> None:
    cfg = PolySourceConfig()
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.62, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=100.02, return_bps=0.1, up_bid=0.30, up_bid_limit=0.25, up_depth_ok=False, remaining=80.0, age=220.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "hold"
    assert decision.reason == "missing_exit_depth"
    assert decision.price == 0.30
    assert decision.limit_price == 0.25


def test_poly_source_depth_unavailable_does_not_emit_zero_price_exit() -> None:
    cfg = PolySourceConfig()
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.62, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=100.02, return_bps=0.1, up_bid=None, up_bid_limit=None, remaining=80.0, age=220.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "hold"
    assert decision.reason == "missing_exit_depth"
    assert decision.price is None
    assert decision.limit_price is None
