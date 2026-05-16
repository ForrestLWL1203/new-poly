"""Shared JSON log schema helpers for strategy decisions and positions."""

from __future__ import annotations

from typing import Any

from new_poly.strategy.types import StrategyDecision
from new_poly.strategy.state import PositionSnapshot
from new_poly.trading.execution import ExecutionResult


def _compact(value: float | None, digits: int = 6) -> float | None:
    return round(float(value), digits) if value is not None else None


def _entry_analysis(decision: StrategyDecision, result: ExecutionResult | None = None) -> dict[str, Any]:
    fill_price = result.avg_price if result is not None and result.success else None
    row = {
        "order_intent": "entry",
        "entry_side": decision.side,
        "entry_phase": decision.phase,
        "entry_signal_price": _compact(decision.price),
        "entry_best_ask": _compact(decision.best_ask),
        "entry_fair_cap": _compact(decision.limit_price),
        "entry_depth_limit_price": _compact(decision.depth_limit_price),
        "entry_score_signal": _compact(decision.edge),
        "entry_price": _compact(fill_price),
        "entry_shares": _compact(result.filled_size if result is not None and result.success else None),
        "entry_poly_reference_distance_bps": _compact(decision.poly_reference_distance_bps, 3),
        "entry_poly_return_bps": _compact(decision.poly_return_bps, 3),
        "entry_poly_trend_lookback_sec": _compact(decision.poly_trend_lookback_sec, 3),
        "entry_poly_return_since_entry_start_bps": _compact(decision.poly_return_since_entry_start_bps, 3),
        "entry_poly_score": _compact(decision.poly_entry_score, 3),
        "entry_poly_distance_score": _compact(decision.poly_entry_distance_score, 3),
        "entry_poly_trend_score": _compact(decision.poly_entry_trend_score, 3),
        "entry_poly_price_quality_score": _compact(decision.poly_entry_price_quality_score, 3),
        "entry_poly_market_quality_score": _compact(decision.poly_entry_market_quality_score, 3),
        "entry_poly_overextended": decision.poly_entry_overextended,
        "entry_direction_quality": decision.direction_quality,
        "entry_direction_current_side": decision.direction_current_side,
        "entry_direction_dominant_side": decision.direction_dominant_side,
        "entry_direction_same_side_duration_sec": _compact(decision.direction_same_side_duration_sec, 3),
        "entry_direction_cross_count_total": decision.direction_cross_count_total,
        "entry_direction_cross_count_recent": decision.direction_cross_count_recent,
        "entry_direction_cross_rate_per_min": _compact(decision.direction_cross_rate_per_min, 3),
        "entry_direction_support_margin": _compact(decision.direction_support_margin, 3),
        "entry_direction_observed_sec": _compact(decision.direction_observed_sec, 3),
        "entry_direction_confidence": _compact(decision.direction_confidence, 3),
        "entry_prior_streak_len": decision.prior_streak_len,
        "entry_prior_streak_side": decision.prior_streak_side,
        "order_attempt": result.attempt if result is not None else None,
        "order_total_latency_ms": result.total_latency_ms if result is not None else None,
    }
    if result is not None and result.timing:
        row["order_timing"] = result.timing
    return {key: value for key, value in row.items() if value is not None}


def _exit_analysis(decision: StrategyDecision, result: ExecutionResult | None = None) -> dict[str, Any]:
    fill_price = result.avg_price if result is not None and result.success else None
    row = {
        "exit_intent": "exit",
        "exit_side": decision.side,
        "exit_reason": decision.reason,
        "exit_signal_bid_avg": _compact(decision.price),
        "exit_min_price": _compact(decision.limit_price),
        "exit_profit_per_share": _compact(decision.profit_now),
        "exit_loss_ratio": _compact(decision.loss_ratio, 3),
        "exit_reference_reason": decision.reference_exit_reason,
        "exit_reference_cross_depth_bps": _compact(decision.reference_cross_depth_bps, 3),
        "exit_reference_cross_age_sec": _compact(decision.reference_cross_age_sec, 3),
        "exit_late_ev_margin": _compact(decision.late_ev_margin, 3),
        "exit_direction_confidence": _compact(decision.direction_confidence, 3),
        "exit_poly_reference_distance_bps": _compact(decision.poly_reference_distance_bps, 3),
        "exit_poly_return_bps": _compact(decision.poly_return_bps, 3),
        "exit_poly_trend_lookback_sec": _compact(decision.poly_trend_lookback_sec, 3),
        "exit_poly_return_since_entry_start_bps": _compact(decision.poly_return_since_entry_start_bps, 3),
        "exit_price": _compact(fill_price),
        "exit_shares": _compact(result.filled_size if result is not None and result.success else None),
        "order_attempt": result.attempt if result is not None else None,
        "order_total_latency_ms": result.total_latency_ms if result is not None else None,
    }
    if result is not None and result.timing:
        row["order_timing"] = result.timing
    return {key: value for key, value in row.items() if value is not None}


def _decision_log(decision: StrategyDecision, *, component_logs: str = "compact") -> dict[str, Any]:
    full_components = component_logs == "full" or decision.action == "exit" or decision.reason == "poly_score_too_low"
    compact_tick_omits = set() if full_components else {
        "poly_entry_distance_score",
        "poly_entry_trend_score",
        "poly_entry_price_quality_score",
        "poly_entry_market_quality_score",
        "poly_entry_overextended",
    }
    return {
        key: value
        for key, value in decision.__dict__.items()
        if value is not None and key not in compact_tick_omits
    }


def _position_log(position: PositionSnapshot | None, *, compact: bool) -> dict[str, Any] | None:
    if position is None:
        return None
    if not compact:
        return position.__dict__
    return {
        "market_slug": position.market_slug,
        "token_side": position.token_side,
        "entry_time": _compact(position.entry_time),
        "entry_avg_price": _compact(position.entry_avg_price),
        "filled_shares": _compact(position.filled_shares),
        "entry_amount_usd": _compact(position.entry_amount_usd),
        "entry_reference_distance_bps": _compact(position.entry_reference_distance_bps, 3),
        "exit_status": position.exit_status,
    }
