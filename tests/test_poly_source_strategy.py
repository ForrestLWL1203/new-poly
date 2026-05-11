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
    assert decision.limit_price == pytest.approx(0.62)
    assert decision.poly_reference_distance_bps == pytest.approx(4.0)
    assert decision.poly_return_bps == pytest.approx(0.4)
    assert decision.poly_entry_score is not None
    assert decision.model_prob is None


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


def test_poly_source_reference_adverse_exit_is_market_only() -> None:
    cfg = PolySourceConfig(exit_reference_adverse_bps=1.0)
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.60, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=99.98, return_bps=-0.2, up_bid=0.45, age=140.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "exit"
    assert decision.reason == "reference_adverse_exit"
    assert decision.model_prob is None
    assert decision.price == 0.45


def test_poly_source_trend_reversal_exit_when_losing() -> None:
    cfg = PolySourceConfig(poly_trend_reversal_bps=0.3, exit_reference_adverse_bps=5.0)
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.60, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=100.02, return_bps=-0.4, up_bid=0.55, age=140.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "exit"
    assert decision.reason == "poly_trend_reversal_exit"


def test_poly_source_market_disagrees_exit_uses_bid_ratio_without_model_prob() -> None:
    cfg = PolySourceConfig(market_disagrees_exit_threshold=0.55, market_disagrees_exit_min_loss=0.03)
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.60, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=100.02, return_bps=0.1, up_bid=0.32, age=140.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "exit"
    assert decision.reason == "market_disagrees_exit"
    assert decision.market_disagreement == pytest.approx(0.32 / 0.60)


def test_poly_source_hold_to_settlement_allows_high_price_winner() -> None:
    cfg = PolySourceConfig(
        hold_to_settlement_enabled=True,
        hold_to_settlement_min_profit_ratio=0.50,
        hold_to_settlement_min_bid_avg=0.80,
        hold_to_settlement_min_bid_limit=0.75,
        hold_to_settlement_min_reference_distance_bps=1.0,
        hold_to_settlement_min_poly_return_bps=0.0,
        final_force_exit_remaining_sec=30.0,
    )
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.60, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=100.03, return_bps=0.2, up_bid=0.91, up_bid_limit=0.86, remaining=20.0, age=280.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "hold"
    assert decision.reason == "hold_to_settlement"


def test_poly_source_profit_protection_exits_profitable_weak_trend_before_final() -> None:
    cfg = PolySourceConfig(
        profit_protection_start_remaining_sec=90.0,
        profit_protection_end_remaining_sec=45.0,
        profit_protection_min_profit=0.08,
        profit_protection_trend_weak_bps=0.0,
        hold_to_settlement_min_profit_ratio=0.25,
    )
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.62, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=100.02, return_bps=-0.05, up_bid=0.72, remaining=70.0, age=230.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "exit"
    assert decision.reason == "profit_protection_exit"
    assert decision.price == 0.72


def test_poly_source_hold_to_settlement_overrides_profit_protection_window() -> None:
    cfg = PolySourceConfig(
        profit_protection_start_remaining_sec=90.0,
        profit_protection_end_remaining_sec=45.0,
        profit_protection_min_profit=0.08,
        profit_protection_trend_weak_bps=0.0,
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


def test_poly_source_late_depth_risk_exits_when_depth_flag_is_bad_but_bid_exists() -> None:
    cfg = PolySourceConfig(late_depth_guard_remaining_sec=90.0, late_depth_min_bid_avg=0.20, late_depth_min_bid_limit=0.15)
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.62, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=100.02, return_bps=0.1, up_bid=0.30, up_bid_limit=0.25, up_depth_ok=False, remaining=80.0, age=220.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "exit"
    assert decision.reason == "late_depth_risk_exit"
    assert decision.price == 0.30
    assert decision.limit_price == 0.25


def test_poly_source_late_depth_unavailable_does_not_emit_zero_price_exit() -> None:
    cfg = PolySourceConfig(late_depth_guard_remaining_sec=90.0)
    position = PositionSnapshot("m1", "up", "up-token", 120.0, 0.62, 10.0, 0.0, 0.0)
    snap = _snapshot(poly_price=100.02, return_bps=0.1, up_bid=None, up_bid_limit=None, remaining=80.0, age=220.0)

    decision = evaluate_poly_exit(snap, position, cfg, StrategyState(current_market_slug="m1"))

    assert decision.action == "hold"
    assert decision.reason == "late_exit_depth_unavailable"
    assert decision.price is None
    assert decision.limit_price is None
