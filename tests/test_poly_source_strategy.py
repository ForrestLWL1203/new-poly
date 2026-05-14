from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.strategy.poly_source import PolySourceConfig, evaluate_poly_entry, evaluate_poly_exit
from new_poly.strategy.prob_edge import MarketSnapshot
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
    assert decision.model_prob is None


def test_poly_source_config_exposes_only_active_single_source_exit_knobs() -> None:
    cfg = PolySourceConfig()
    fields = set(cfg.__dataclass_fields__)

    assert hasattr(cfg, "reference_distance_exit_remaining_sec")
    assert hasattr(cfg, "reference_distance_exit_min_bps")
    assert hasattr(cfg, "min_poly_hold_score")
    assert fields == {
        "entry_start_age_sec",
        "entry_end_age_sec",
        "final_no_entry_remaining_sec",
        "pre_entry_observation_start_age_sec",
        "early_to_core_age_sec",
        "core_to_late_age_sec",
        "early_value_entry_enabled",
        "early_value_start_age_sec",
        "early_value_end_age_sec",
        "early_value_min_reference_distance_bps",
        "early_value_min_poly_return_bps",
        "early_value_min_entry_score",
        "early_value_max_entry_ask",
        "early_value_max_spread",
        "early_value_hold_protection_enabled",
        "max_entries_per_market",
        "max_book_age_ms",
        "poly_reference_distance_bps",
        "max_poly_reference_distance_bps",
        "poly_trend_lookback_sec",
        "poly_return_bps",
        "max_entry_ask",
        "max_entry_fill_price",
        "min_poly_entry_score",
        "min_poly_hold_score",
        "poly_score_component_logs",
        "entry_tick_size",
        "buy_price_buffer_ticks",
        "reference_distance_exit_remaining_sec",
        "reference_distance_exit_min_bps",
        "exit_min_hold_sec",
        "hold_to_settlement_enabled",
        "hold_to_settlement_min_profit_ratio",
        "hold_to_settlement_min_bid_avg",
        "hold_to_settlement_min_bid_limit",
        "hold_to_settlement_min_reference_distance_bps",
        "hold_to_settlement_min_poly_return_bps",
    }


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


def test_poly_source_early_value_entry_allows_strong_cheap_signal_before_normal_window() -> None:
    cfg = PolySourceConfig(
        entry_start_age_sec=120.0,
        entry_end_age_sec=220.0,
        early_value_entry_enabled=True,
        early_value_start_age_sec=60.0,
        early_value_end_age_sec=120.0,
        early_value_min_reference_distance_bps=2.5,
        early_value_min_poly_return_bps=0.5,
        early_value_min_entry_score=5.5,
        early_value_max_entry_ask=0.60,
        early_value_max_spread=0.06,
        poly_reference_distance_bps=1.5,
        poly_return_bps=0.3,
        max_entry_ask=0.75,
    )

    decision = evaluate_poly_entry(
        _snapshot(poly_price=100.03, return_bps=0.8, up_ask=0.56, up_bid=0.53, age=80.0, remaining=220.0),
        StrategyState(current_market_slug="m1"),
        cfg,
    )

    assert decision.action == "enter"
    assert decision.reason == "poly_early_value"
    assert decision.phase == "early_value"
    assert decision.poly_reference_distance_bps == pytest.approx(3.0)
    assert decision.poly_entry_score is not None
    assert decision.poly_entry_score >= 5.5


def test_poly_source_early_value_entry_rejects_expensive_or_wide_signal() -> None:
    cfg = PolySourceConfig(
        early_value_entry_enabled=True,
        early_value_min_reference_distance_bps=2.5,
        early_value_min_poly_return_bps=0.5,
        early_value_min_entry_score=5.5,
        early_value_max_entry_ask=0.60,
        early_value_max_spread=0.06,
        poly_reference_distance_bps=1.5,
        poly_return_bps=0.3,
        max_entry_ask=0.75,
    )

    expensive = evaluate_poly_entry(
        _snapshot(poly_price=100.03, return_bps=0.8, up_ask=0.64, up_bid=0.61, age=80.0, remaining=220.0),
        StrategyState(current_market_slug="m1"),
        cfg,
    )
    wide = evaluate_poly_entry(
        _snapshot(poly_price=100.03, return_bps=0.8, up_ask=0.56, up_bid=0.48, age=80.0, remaining=220.0),
        StrategyState(current_market_slug="m1"),
        cfg,
    )

    assert expensive.action == "skip"
    assert expensive.reason == "early_value_ask_too_high"
    assert wide.action == "skip"
    assert wide.reason == "early_value_spread_too_wide"


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


def test_poly_source_poly_hold_score_exit_replaces_fixed_adverse_exit() -> None:
    cfg = PolySourceConfig()
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.60, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=99.97, return_bps=-0.2, up_bid=0.45, age=140.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "exit"
    assert decision.reason == "poly_hold_score_exit"
    assert decision.model_prob is None
    assert decision.price == 0.45


def test_poly_source_poly_hold_score_exit_uses_remaining_time_curve() -> None:
    cfg = PolySourceConfig(
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 0.25, 0.75, 1.0),
    )
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.60, 10.0, 0.0, 0.0)

    early = evaluate_poly_exit(
        _snapshot(poly_price=99.99, return_bps=-0.2, up_bid=0.45, age=180.0, remaining=120.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )
    late = evaluate_poly_exit(
        _snapshot(poly_price=99.989, return_bps=-0.2, up_bid=0.45, age=210.0, remaining=90.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )

    assert early.action == "hold"
    assert late.action == "exit"
    assert late.reason == "poly_hold_score_exit"


def test_poly_source_hold_score_allows_90s_light_adverse_margin() -> None:
    cfg = PolySourceConfig(
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 0.25, 0.75, 1.0),
        min_poly_hold_score=0.0,
    )
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        120.0,
        0.60,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=2.0,
    )
    decision = evaluate_poly_exit(
        _snapshot(poly_price=99.995, return_bps=-0.1, up_bid=0.55, age=210.0, remaining=90.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )

    assert decision.action == "hold"
    assert decision.poly_hold_score is not None
    assert decision.poly_hold_score >= 0.0
    assert decision.poly_hold_floor_bps == pytest.approx(-1.0)


def test_poly_source_hold_score_exits_70s_light_adverse_margin() -> None:
    cfg = PolySourceConfig(
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 0.25, 0.75, 1.0),
        min_poly_hold_score=0.0,
    )
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        120.0,
        0.60,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=2.0,
    )
    decision = evaluate_poly_exit(
        _snapshot(poly_price=99.999, return_bps=-0.1, up_bid=0.55, age=230.0, remaining=70.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )

    assert decision.action == "exit"
    assert decision.reason == "poly_hold_score_exit"
    assert decision.poly_hold_floor_bps == pytest.approx(0.25)
    assert decision.poly_hold_reference_margin_bps < 0.0
    assert decision.poly_hold_score is not None
    assert decision.poly_hold_score < 0.0


def test_poly_source_hold_score_requires_stronger_late_reference_edge() -> None:
    cfg = PolySourceConfig(
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 0.25, 0.75, 1.0),
        min_poly_hold_score=0.0,
    )
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        120.0,
        0.60,
        10.0,
        0.0,
        0.0,
        entry_reference_distance_bps=2.0,
    )
    at_45s = evaluate_poly_exit(
        _snapshot(poly_price=100.005, return_bps=0.1, up_bid=0.70, age=255.0, remaining=45.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )
    at_30s = evaluate_poly_exit(
        _snapshot(poly_price=100.008, return_bps=0.1, up_bid=0.70, age=270.0, remaining=30.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )

    assert at_45s.action == "exit"
    assert at_45s.reason == "poly_hold_score_exit"
    assert at_45s.poly_hold_floor_bps == pytest.approx(0.75)
    assert at_30s.action == "exit"
    assert at_30s.poly_hold_floor_bps == pytest.approx(1.0)


def test_poly_source_midlate_hold_floor_relaxes_when_reference_and_book_still_support() -> None:
    cfg = PolySourceConfig(
        poly_trend_lookback_sec=10.0,
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 1.0, 1.5, 1.75),
        min_poly_hold_score=0.0,
    )
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        150.0,
        0.60,
        10.0,
        0.0,
        5.5,
        entry_reference_distance_bps=2.0,
    )

    decision = evaluate_poly_exit(
        _snapshot(
            poly_price=100.016,
            return_bps=-2.0,
            lookback=10.0,
            up_bid=0.50,
            down_bid=0.50,
            age=243.0,
            remaining=57.0,
        ),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )

    assert decision.action == "hold"
    assert decision.poly_hold_floor_bps == pytest.approx(0.25)
    assert decision.poly_hold_score is not None
    assert decision.poly_hold_score >= 0.0


def test_poly_source_midlate_hold_floor_stays_strict_when_reference_is_weak() -> None:
    cfg = PolySourceConfig(
        poly_trend_lookback_sec=10.0,
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 1.0, 1.5, 1.75),
        min_poly_hold_score=0.0,
    )
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        150.0,
        0.60,
        10.0,
        0.0,
        5.5,
        entry_reference_distance_bps=2.0,
    )

    decision = evaluate_poly_exit(
        _snapshot(
            poly_price=100.010,
            return_bps=-2.0,
            lookback=10.0,
            up_bid=0.50,
            down_bid=0.50,
            age=243.0,
            remaining=57.0,
        ),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )

    assert decision.action == "exit"
    assert decision.reason == "poly_hold_score_exit"
    assert decision.poly_hold_floor_bps == pytest.approx(1.26)


def test_poly_source_poly_hold_score_exit_is_symmetric_for_down_positions() -> None:
    cfg = PolySourceConfig(
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 0.0, 0.75),
    )
    position = PositionSnapshot("m1", "down", "down-token", 120.0, 0.60, 10.0, 0.0, 0.0)
    snap = _snapshot(
        poly_price=100.01,
        return_bps=0.2,
        up_bid=0.39,
        down_bid=0.45,
        age=210.0,
        remaining=90.0,
    )

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "exit"
    assert decision.reason == "poly_hold_score_exit"


def test_poly_source_late_reference_distance_requires_positive_edge_even_above_k() -> None:
    cfg = PolySourceConfig(
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 0.0, 0.75),
    )
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.60, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=100.005, return_bps=0.1, up_bid=0.65, age=255.0, remaining=45.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "exit"
    assert decision.reason == "poly_hold_score_exit"


def test_poly_source_hold_to_settlement_does_not_mask_poly_hold_score_exit() -> None:
    cfg = PolySourceConfig(
        hold_to_settlement_enabled=True,
        hold_to_settlement_min_profit_ratio=0.0,
        hold_to_settlement_min_bid_avg=0.78,
        hold_to_settlement_min_bid_limit=0.72,
        hold_to_settlement_min_reference_distance_bps=0.0,
        hold_to_settlement_min_poly_return_bps=-0.3,
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 0.0, 0.75),
    )
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.60, 10.0, 0.0, 0.0)
    snap = _snapshot(
        poly_price=100.005,
        return_bps=0.1,
        up_bid=0.91,
        up_bid_limit=0.86,
        age=255.0,
        remaining=45.0,
    )

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "exit"
    assert decision.reason == "poly_hold_score_exit"



def test_poly_source_hold_score_exit_waits_for_min_hold() -> None:
    cfg = PolySourceConfig(exit_min_hold_sec=3.0)
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.60, 10.0, 0.0, 0.0)

    immature = evaluate_poly_exit(
        _snapshot(poly_price=99.98, return_bps=-0.1, up_bid=0.55, age=122.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )
    mature = evaluate_poly_exit(
        _snapshot(poly_price=99.98, return_bps=-0.1, up_bid=0.55, age=123.0),
        position,
        cfg,
        StrategyState(current_market_slug="m1"),
    )

    assert immature.action == "hold"
    assert mature.action == "exit"
    assert mature.reason == "poly_hold_score_exit"

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


def test_poly_source_orderbook_pressure_is_ignored_before_120s() -> None:
    cfg = PolySourceConfig(
        poly_trend_lookback_sec=10.0,
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 1.0, 1.5, 1.75),
        min_poly_hold_score=0.0,
    )
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        178.0,
        0.54,
        1.85,
        0.0,
        5.2,
        entry_reference_distance_bps=2.35,
    )
    snap = _snapshot(
        poly_price=100.0404,
        return_bps=0.266,
        lookback=10.0,
        up_bid=0.36,
        up_ask=0.40,
        down_bid=0.60,
        down_ask=0.64,
        age=170.0,
        remaining=130.0,
    )

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "hold"
    assert decision.poly_hold_orderbook_score == pytest.approx(0.0)


def test_poly_source_early_value_hold_protection_starts_orderbook_pressure_before_120s() -> None:
    cfg = PolySourceConfig(
        early_value_hold_protection_enabled=True,
        poly_trend_lookback_sec=10.0,
        reference_distance_exit_remaining_sec=(210.0, 180.0, 150.0, 120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-3.0, -2.0, -1.0, -0.3, -1.0, 1.0, 1.5, 1.75),
        min_poly_hold_score=0.0,
    )
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        82.0,
        0.54,
        1.85,
        0.0,
        5.2,
        entry_reference_distance_bps=2.35,
    )
    snap = _snapshot(
        poly_price=100.0404,
        return_bps=0.266,
        lookback=10.0,
        up_bid=0.36,
        up_ask=0.40,
        down_bid=0.60,
        down_ask=0.64,
        age=120.0,
        remaining=180.0,
    )

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.poly_hold_orderbook_score is not None
    assert decision.poly_hold_orderbook_score < 0.0


def test_poly_source_orderbook_pressure_is_light_near_105s() -> None:
    cfg = PolySourceConfig(
        poly_trend_lookback_sec=10.0,
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 1.0, 1.5, 1.75),
        min_poly_hold_score=0.0,
    )
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        178.0,
        0.54,
        1.85,
        0.0,
        5.2,
        entry_reference_distance_bps=2.35,
    )
    snap = _snapshot(
        poly_price=100.0373,
        return_bps=0.17,
        lookback=10.0,
        up_bid=0.51,
        up_ask=0.52,
        down_bid=0.53,
        down_ask=0.49,
        age=195.0,
        remaining=105.0,
    )

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "hold"
    assert decision.poly_hold_orderbook_score is not None
    assert -0.25 < decision.poly_hold_orderbook_score < 0.0


def test_poly_source_orderbook_pressure_is_meaningful_near_85s() -> None:
    cfg = PolySourceConfig(
        poly_trend_lookback_sec=10.0,
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 1.0, 1.5, 1.75),
        min_poly_hold_score=0.0,
    )
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        178.0,
        0.54,
        1.85,
        0.0,
        5.2,
        entry_reference_distance_bps=2.35,
    )
    snap = _snapshot(
        poly_price=100.0404,
        return_bps=0.266,
        lookback=10.0,
        up_bid=0.36,
        up_ask=0.40,
        down_bid=0.60,
        down_ask=0.64,
        age=215.0,
        remaining=85.0,
    )

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.poly_hold_orderbook_score is not None
    assert decision.poly_hold_orderbook_score < -0.40
    assert decision.poly_hold_score is not None
    assert decision.poly_hold_score > decision.poly_hold_orderbook_score


def test_poly_source_orderbook_pressure_can_override_slow_reference_near_66s() -> None:
    cfg = PolySourceConfig(
        poly_trend_lookback_sec=10.0,
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 1.0, 1.5, 1.75),
        min_poly_hold_score=0.0,
    )
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        178.0,
        0.54,
        1.85,
        0.0,
        5.2,
        entry_reference_distance_bps=2.35,
    )
    snap = _snapshot(
        poly_price=100.0136,
        return_bps=0.266,
        lookback=10.0,
        up_bid=0.23,
        up_ask=0.24,
        down_bid=0.76,
        down_ask=0.77,
        age=234.0,
        remaining=66.0,
    )

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "exit"
    assert decision.reason == "poly_hold_score_exit"
    assert decision.poly_hold_score is not None
    assert decision.poly_hold_score < 0.0
    assert decision.poly_hold_orderbook_score is not None
    assert decision.poly_hold_orderbook_score < -1.0


def test_poly_source_orderbook_pressure_remains_strong_near_30s() -> None:
    cfg = PolySourceConfig(
        poly_trend_lookback_sec=10.0,
        reference_distance_exit_remaining_sec=(120.0, 90.0, 70.0, 45.0, 30.0),
        reference_distance_exit_min_bps=(-2.0, -1.0, 1.0, 1.5, 1.75),
        min_poly_hold_score=0.0,
    )
    position = PositionSnapshot(
        "m1",
        "up",
        "up-token",
        178.0,
        0.54,
        1.85,
        0.0,
        5.2,
        entry_reference_distance_bps=2.35,
    )
    snap = _snapshot(
        poly_price=100.0404,
        return_bps=0.266,
        lookback=10.0,
        up_bid=0.36,
        up_ask=0.40,
        down_bid=0.60,
        down_ask=0.64,
        age=270.0,
        remaining=30.0,
    )

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "exit"
    assert decision.poly_hold_orderbook_score is not None
    assert decision.poly_hold_orderbook_score < -1.0



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
