"""Shared strategy data structures.

The old dual-source poly-source strategy logic was removed. Runtime
decisions now live in ``new_poly.strategy.poly_source``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketSnapshot:
    market_slug: str
    age_sec: float
    remaining_sec: float
    s_price: float | None
    k_price: float | None
    sigma_eff: float | None
    up_ask_avg: float | None = None
    down_ask_avg: float | None = None
    up_ask_limit: float | None = None
    down_ask_limit: float | None = None
    up_best_ask: float | None = None
    down_best_ask: float | None = None
    up_bid_avg: float | None = None
    down_bid_avg: float | None = None
    up_bid_limit: float | None = None
    down_bid_limit: float | None = None
    up_bid_depth_ok: bool = False
    down_bid_depth_ok: bool = False
    up_book_age_ms: float | None = None
    down_book_age_ms: float | None = None
    up_bid_age_ms: float | None = None
    down_bid_age_ms: float | None = None
    source_spread_bps: float | None = None
    polymarket_price: float | None = None
    polymarket_price_age_sec: float | None = None
    polymarket_return_1s_bps: float | None = None
    polymarket_return_3s_bps: float | None = None
    polymarket_return_5s_bps: float | None = None
    polymarket_return_10s_bps: float | None = None
    polymarket_return_15s_bps: float | None = None
    poly_return_since_entry_start_bps: float | None = None


@dataclass(frozen=True, init=False)
class StrategyDecision:
    action: str
    reason: str
    side: str | None = None
    token_id: str | None = None
    price: float | None = None
    limit_price: float | None = None
    depth_limit_price: float | None = None
    best_ask: float | None = None
    edge: float | None = None
    phase: str | None = None
    profit_now: float | None = None
    poly_reference_distance_bps: float | None = None
    poly_return_bps: float | None = None
    poly_trend_lookback_sec: float | None = None
    poly_return_since_entry_start_bps: float | None = None
    poly_entry_score: float | None = None
    poly_entry_distance_score: float | None = None
    poly_entry_trend_score: float | None = None
    poly_entry_price_quality_score: float | None = None
    poly_entry_market_quality_score: float | None = None
    poly_entry_overextended: bool | None = None
    direction_quality: str | None = None
    direction_current_side: str | None = None
    direction_dominant_side: str | None = None
    direction_same_side_duration_sec: float | None = None
    direction_cross_count_total: int | None = None
    direction_cross_count_recent: int | None = None
    direction_cross_rate_per_min: float | None = None
    direction_support_margin: float | None = None
    direction_observed_sec: float | None = None
    direction_confidence: float | None = None
    prior_streak_len: int | None = None
    prior_streak_side: str | None = None
    loss_ratio: float | None = None
    reference_exit_reason: str | None = None
    reference_cross_depth_bps: float | None = None
    reference_cross_age_sec: float | None = None
    late_ev_margin: float | None = None

    def __init__(
        self,
        action: str,
        reason: str,
        side: str | None = None,
        token_id: str | None = None,
        price: float | None = None,
        limit_price: float | None = None,
        depth_limit_price: float | None = None,
        best_ask: float | None = None,
        edge: float | None = None,
        phase: str | None = None,
        profit_now: float | None = None,
        poly_reference_distance_bps: float | None = None,
        poly_return_bps: float | None = None,
        poly_trend_lookback_sec: float | None = None,
        poly_return_since_entry_start_bps: float | None = None,
        poly_entry_score: float | None = None,
        poly_entry_distance_score: float | None = None,
        poly_entry_trend_score: float | None = None,
        poly_entry_price_quality_score: float | None = None,
        poly_entry_market_quality_score: float | None = None,
        poly_entry_overextended: bool | None = None,
        direction_quality: str | None = None,
        direction_current_side: str | None = None,
        direction_dominant_side: str | None = None,
        direction_same_side_duration_sec: float | None = None,
        direction_cross_count_total: int | None = None,
        direction_cross_count_recent: int | None = None,
        direction_cross_rate_per_min: float | None = None,
        direction_support_margin: float | None = None,
        direction_observed_sec: float | None = None,
        direction_confidence: float | None = None,
        prior_streak_len: int | None = None,
        prior_streak_side: str | None = None,
        loss_ratio: float | None = None,
        reference_exit_reason: str | None = None,
        reference_cross_depth_bps: float | None = None,
        reference_cross_age_sec: float | None = None,
        late_ev_margin: float | None = None,
        **_removed_legacy_fields,
    ) -> None:
        for key, value in {
            "action": action,
            "reason": reason,
            "side": side,
            "token_id": token_id,
            "price": price,
            "limit_price": limit_price,
            "depth_limit_price": depth_limit_price,
            "best_ask": best_ask,
            "edge": edge,
            "phase": phase,
            "profit_now": profit_now,
            "poly_reference_distance_bps": poly_reference_distance_bps,
            "poly_return_bps": poly_return_bps,
            "poly_trend_lookback_sec": poly_trend_lookback_sec,
            "poly_return_since_entry_start_bps": poly_return_since_entry_start_bps,
            "poly_entry_score": poly_entry_score,
            "poly_entry_distance_score": poly_entry_distance_score,
            "poly_entry_trend_score": poly_entry_trend_score,
            "poly_entry_price_quality_score": poly_entry_price_quality_score,
            "poly_entry_market_quality_score": poly_entry_market_quality_score,
            "poly_entry_overextended": poly_entry_overextended,
            "direction_quality": direction_quality,
            "direction_current_side": direction_current_side,
            "direction_dominant_side": direction_dominant_side,
            "direction_same_side_duration_sec": direction_same_side_duration_sec,
            "direction_cross_count_total": direction_cross_count_total,
            "direction_cross_count_recent": direction_cross_count_recent,
            "direction_cross_rate_per_min": direction_cross_rate_per_min,
            "direction_support_margin": direction_support_margin,
            "direction_observed_sec": direction_observed_sec,
            "direction_confidence": direction_confidence,
            "prior_streak_len": prior_streak_len,
            "prior_streak_side": prior_streak_side,
            "loss_ratio": loss_ratio,
            "reference_exit_reason": reference_exit_reason,
            "reference_cross_depth_bps": reference_cross_depth_bps,
            "reference_cross_age_sec": reference_cross_age_sec,
            "late_ev_margin": late_ev_margin,
        }.items():
            object.__setattr__(self, key, value)
