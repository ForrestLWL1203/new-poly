"""Shared strategy data structures.

The old dual-source probability-edge strategy logic was removed. Runtime
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
    polymarket_divergence_bps: float | None = None
    polymarket_price: float | None = None
    polymarket_price_age_sec: float | None = None
    polymarket_return_1s_bps: float | None = None
    polymarket_return_3s_bps: float | None = None
    polymarket_return_5s_bps: float | None = None
    polymarket_return_10s_bps: float | None = None
    polymarket_return_15s_bps: float | None = None
    poly_return_since_entry_start_bps: float | None = None


@dataclass(frozen=True)
class StrategyDecision:
    action: str
    reason: str
    side: str | None = None
    token_id: str | None = None
    model_prob: float | None = None
    price: float | None = None
    limit_price: float | None = None
    depth_limit_price: float | None = None
    best_ask: float | None = None
    edge: float | None = None
    up_prob: float | None = None
    down_prob: float | None = None
    phase: str | None = None
    required_edge: float | None = None
    profit_now: float | None = None
    prob_stagnant: bool | None = None
    prob_delta_3s: float | None = None
    prob_drop_delta: float | None = None
    market_disagreement: float | None = None
    polymarket_divergence_bps: float | None = None
    favorable_gap_bps: float | None = None
    entry_reference_distance_bps: float | None = None
    gap_compression_from_entry_bps: float | None = None
    gap_risk_flag: str | None = None
    adjusted_up_prob_shadow: float | None = None
    adjusted_down_prob_shadow: float | None = None
    adjusted_model_prob_shadow: float | None = None
    prob_shadow_adjustment: float | None = None
    lead_follow_state: str | None = None
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
    poly_hold_score: float | None = None
    poly_hold_floor_bps: float | None = None
    poly_hold_reference_margin_bps: float | None = None
    poly_hold_reference_margin_score: float | None = None
    poly_hold_trend_score: float | None = None
    poly_hold_entry_baseline_score: float | None = None
    poly_hold_pnl_context_score: float | None = None
    poly_hold_settlement_bonus: float | None = None
