"""Polymarket-only entry and exit decisions."""

from __future__ import annotations

from dataclasses import dataclass

from .prob_edge import MarketSnapshot, StrategyDecision
from .state import DirectionState, PositionSnapshot, StrategyState


@dataclass(frozen=True)
class PolySourceConfig:
    entry_start_age_sec: float = 100.0
    entry_end_age_sec: float = 240.0
    final_no_entry_remaining_sec: float = 30.0
    pre_entry_observation_start_age_sec: float = 0.0
    early_to_core_age_sec: float = 120.0
    core_to_late_age_sec: float = 240.0
    early_value_entry_enabled: bool = False
    early_value_start_age_sec: float = 60.0
    early_value_end_age_sec: float = 120.0
    early_value_min_reference_distance_bps: float = 2.5
    early_value_min_poly_return_bps: float = 0.5
    early_value_min_entry_score: float = 5.5
    early_value_max_entry_ask: float = 0.60
    early_value_max_spread: float = 0.06
    early_value_hold_protection_enabled: bool = False
    max_entries_per_market: int = 1
    max_book_age_ms: float = 1000.0
    poly_reference_distance_bps: float = 0.5
    max_poly_reference_distance_bps: float = 0.0
    poly_trend_lookback_sec: float = 3.0
    poly_return_bps: float = 0.3
    max_entry_ask: float = 0.65
    max_entry_fill_price: float = 0.0
    min_poly_entry_score: float = 0.0
    direction_observe_start_age_sec: float = 30.0
    direction_min_observed_sec: float = 0.0
    direction_recent_window_sec: float = 30.0
    direction_fresh_cross_sec: float = 20.0
    direction_choppy_recent_crosses: int = 2
    direction_choppy_total_crosses: int = 0
    direction_choppy_cross_rate_per_min: float = 1.5
    direction_stable_min_same_side_sec: float = 30.0
    direction_stable_max_recent_crosses: int = 1
    direction_confidence_enabled: bool = False
    min_direction_confidence: float = 0.0
    direction_confidence_score_override: bool = False
    direction_confidence_high_reference_bps: float = 3.0
    direction_confidence_prior_streak_min: int = 3
    exit_direction_confidence_enabled: bool = False
    exit_min_direction_confidence: float = 0.78
    exit_direction_confidence_min_hold_sec: float = 20.0
    exit_direction_confidence_pressure_count: int = 2
    late_ev_exit_enabled: bool = False
    late_ev_exit_min_hold_sec: float = 60.0
    late_ev_exit_min_remaining_sec: float = 45.0
    late_ev_exit_remaining_sec: tuple[float, ...] = (120.0, 80.0, 45.0)
    late_ev_exit_margin: tuple[float, ...] = (0.18, 0.12, 0.06)
    late_ev_exit_min_cross_bps: float = 0.5
    late_ev_exit_min_cross_sec: float = 5.0
    progressive_stop_warmup_sec: float = 30.0
    progressive_stop_full_sec: float = 120.0
    progressive_stop_initial_loss_ratio: float = 0.60
    progressive_stop_final_loss_ratio: float = 0.30
    progressive_stop_late_remaining_sec: float = 80.0
    progressive_stop_reference_deterioration_bps: float = 2.0
    progressive_stop_extreme_loss_ratio: float = 0.75
    reentry_cooldown_sec: float = 20.0
    reentry_min_score_bonus: float = 1.0
    reentry_max_entry_fill_price: float = 0.65
    entry_size_score_mid: float = 6.0
    entry_size_score_full: float = 6.5
    entry_size_full_confidence: float = 0.95
    entry_size_high_price_cap: float = 0.70
    entry_size_full_min_age_sec: float = 150.0
    entry_size_mid_multiplier: float = 2.0
    entry_size_full_multiplier: float = 3.0
    poly_score_component_logs: str = "compact"
    entry_tick_size: float = 0.01
    buy_price_buffer_ticks: float = 2.0
    reference_distance_exit_remaining_sec: tuple[float, ...] = (120.0, 90.0, 70.0, 45.0, 30.0)
    reference_distance_exit_min_bps: tuple[float, ...] = (-2.0, -1.0, 0.25, 0.75, 1.0)
    exit_min_hold_sec: float = 3.0
    hold_to_settlement_enabled: bool = True
    hold_to_settlement_min_profit_ratio: float = 0.50
    hold_to_settlement_min_bid_avg: float = 0.80
    hold_to_settlement_min_bid_limit: float = 0.75
    hold_to_settlement_min_reference_distance_bps: float = 1.0
    hold_to_settlement_min_poly_return_bps: float = 0.0


@dataclass(frozen=True)
class PolyEntryScore:
    total: float
    distance_score: float
    trend_score: float
    price_quality_score: float
    market_quality_score: float
    overextended: bool


@dataclass(frozen=True)
class PolyHoldScore:
    total: float
    floor_bps: float | None
    reference_margin_bps: float | None
    reference_margin_score: float
    trend_score: float
    entry_baseline_score: float
    pnl_context_score: float
    orderbook_score: float
    settlement_bonus: float


def _distance_bps(snapshot: MarketSnapshot, side: str) -> float | None:
    if snapshot.polymarket_price is None or snapshot.k_price is None or snapshot.k_price <= 0:
        return None
    raw = (snapshot.polymarket_price - snapshot.k_price) / snapshot.k_price * 10000.0
    return raw if side == "up" else -raw


def _trend_bps(snapshot: MarketSnapshot, lookback_sec: float, side: str) -> float | None:
    candidates = {
        1.0: snapshot.polymarket_return_1s_bps,
        3.0: snapshot.polymarket_return_3s_bps,
        5.0: snapshot.polymarket_return_5s_bps,
        10.0: snapshot.polymarket_return_10s_bps,
        15.0: snapshot.polymarket_return_15s_bps,
    }
    value = candidates.get(float(lookback_sec))
    if value is None:
        return None
    return value if side == "up" else -value


def _ask(snapshot: MarketSnapshot, side: str) -> float | None:
    return snapshot.up_best_ask if side == "up" else snapshot.down_best_ask


def _bid(snapshot: MarketSnapshot, side: str) -> float | None:
    return snapshot.up_bid_avg if side == "up" else snapshot.down_bid_avg


def _bid_limit(snapshot: MarketSnapshot, side: str) -> float | None:
    return snapshot.up_bid_limit if side == "up" else snapshot.down_bid_limit


def _depth_ok(snapshot: MarketSnapshot, side: str) -> bool:
    return snapshot.up_bid_depth_ok if side == "up" else snapshot.down_bid_depth_ok


def _book_age(snapshot: MarketSnapshot, side: str) -> float | None:
    return snapshot.up_book_age_ms if side == "up" else snapshot.down_book_age_ms


def _book_fresh(snapshot: MarketSnapshot, side: str, cfg: PolySourceConfig) -> bool:
    age = _book_age(snapshot, side)
    return age is not None and age <= cfg.max_book_age_ms


def _max_entry_fill_price(cfg: PolySourceConfig) -> float | None:
    return cfg.max_entry_fill_price if cfg.max_entry_fill_price > 0 else None


def _reentry_max_entry_fill_price(cfg: PolySourceConfig) -> float | None:
    return cfg.reentry_max_entry_fill_price if cfg.reentry_max_entry_fill_price > 0 else None


def _raw_poly_side(snapshot: MarketSnapshot) -> str | None:
    if snapshot.polymarket_price is None or snapshot.k_price is None:
        return None
    if snapshot.polymarket_price > snapshot.k_price:
        return "up"
    if snapshot.polymarket_price < snapshot.k_price:
        return "down"
    return None


def _clamp(value: float, low: float, high: float) -> float:
    return min(max(value, low), high)


def entry_amount_usd(
    base_amount_usd: float,
    *,
    score: float | None,
    entry_price: float | None,
    reference_distance_bps: float | None = None,
    direction_quality: str | None = None,
    direction_cross_count_recent: int | None = None,
    direction_confidence: float | None = None,
    cfg: PolySourceConfig,
    phase: str | None = None,
    age_sec: float | None = None,
) -> float:
    base = max(0.0, float(base_amount_usd))
    if base <= 0.0:
        return base
    if phase == "early_value":
        return base
    if entry_price is not None and cfg.entry_size_high_price_cap > 0 and entry_price >= cfg.entry_size_high_price_cap:
        return base
    if score is None:
        return base
    if cfg.direction_confidence_enabled:
        if direction_confidence is None or direction_confidence < cfg.min_direction_confidence:
            return base
        span = max(1e-9, cfg.entry_size_full_confidence - cfg.min_direction_confidence)
        progress = _clamp((direction_confidence - cfg.min_direction_confidence) / span, 0.0, 1.0)
        multiplier = 1.0 + progress * (max(1.0, cfg.entry_size_full_multiplier) - 1.0)
        return round(base * multiplier, 6)
    reference_distance = reference_distance_bps if reference_distance_bps is not None else 0.0
    if direction_quality is not None and direction_quality not in {"acceptable", "stable"}:
        return base
    full_size_allowed = age_sec is None or age_sec >= cfg.entry_size_full_min_age_sec
    stable_direction = direction_quality is None or (
        direction_quality == "stable" and (direction_cross_count_recent is None or direction_cross_count_recent <= cfg.direction_stable_max_recent_crosses)
    )
    if full_size_allowed and stable_direction and score >= cfg.entry_size_score_full and reference_distance >= 3.5 and (entry_price is None or entry_price <= 0.60):
        return round(base * max(1.0, cfg.entry_size_full_multiplier), 6)
    if score >= cfg.entry_size_score_mid and reference_distance >= 3.0:
        return round(base * max(1.0, cfg.entry_size_mid_multiplier), 6)
    return base


def _distance_score(distance_bps: float) -> tuple[float, bool]:
    distance = max(distance_bps, 0.0)
    if distance <= 5.0:
        return distance, False
    if distance <= 8.0:
        return 5.0 + (distance - 5.0) / 3.0, False
    if distance <= 12.0:
        return 6.0, False
    penalty = min((distance - 12.0) * 0.25, 2.0)
    return max(4.0, 6.0 - penalty), True


def _price_quality_score(ask: float) -> float:
    if ask < 0.35:
        return -0.5 + ask / 0.35
    if ask <= 0.55:
        return 1.0
    if ask <= 0.65:
        return 1.0 - (ask - 0.55) / 0.10
    if ask <= 0.75:
        return -1.5 * ((ask - 0.65) / 0.10)
    return -2.0


def _effective_entry_trend_bps(distance_bps: float, trend_bps: float) -> float:
    if trend_bps <= 0.0 or distance_bps <= 0.0:
        return 0.0
    previous_distance = distance_bps - trend_bps
    if previous_distance < 0.0:
        return min(distance_bps, trend_bps) * 0.25
    return min(distance_bps - previous_distance, trend_bps)


def _hold_orderbook_time_weight(remaining_sec: float, cfg: PolySourceConfig) -> float:
    if cfg.early_value_hold_protection_enabled:
        if remaining_sec > 210.0:
            return 0.0
        if remaining_sec > 180.0:
            return (210.0 - remaining_sec) / 30.0 * 0.15
        if remaining_sec > 150.0:
            return 0.15 + (180.0 - remaining_sec) / 30.0 * 0.10
        if remaining_sec > 110.0:
            return 0.25 + (150.0 - remaining_sec) / 40.0 * 0.20
    if remaining_sec > 110.0:
        return 0.0
    if remaining_sec > 90.0:
        return (110.0 - remaining_sec) / 20.0 * 0.45
    if remaining_sec > 60.0:
        return 0.45 + (90.0 - remaining_sec) / 30.0 * 0.30
    return 0.75 + (60.0 - max(0.0, remaining_sec)) / 60.0 * 0.25


def _entry_score(distance_bps: float, trend_bps: float, ask: float, bid: float | None) -> PolyEntryScore:
    distance_score, overextended = _distance_score(distance_bps)
    trend_score = min(_effective_entry_trend_bps(distance_bps, trend_bps), 2.0)
    price_score = _price_quality_score(ask)
    market_score = 0.0
    if bid is not None:
        spread = max(0.0, ask - bid)
        market_score = max(0.0, 1.0 - spread / 0.05)
    total = round(distance_score + trend_score + price_score + market_score, 6)
    return PolyEntryScore(
        total=total,
        distance_score=round(distance_score, 6),
        trend_score=round(trend_score, 6),
        price_quality_score=round(price_score, 6),
        market_quality_score=round(market_score, 6),
        overextended=overextended,
    )


def _direction_confidence(
    *,
    direction: DirectionState | None,
    distance_bps: float | None,
    state: StrategyState,
    cfg: PolySourceConfig,
) -> float | None:
    if direction is None or direction.current_side is None:
        return None
    confidence = 0.62
    observed = min(max(direction.observed_sec, 0.0), 120.0)
    confidence += observed / 120.0 * 0.05
    if distance_bps is not None:
        confidence += _clamp((distance_bps - 1.0) / 3.0, 0.0, 1.0) * 0.12
    if direction.same_side_duration_sec >= 90.0:
        confidence += 0.08
    elif direction.same_side_duration_sec >= 60.0:
        confidence += 0.05
    elif direction.same_side_duration_sec >= 30.0:
        confidence += 0.02
    confidence -= min(direction.cross_count_recent, 3) * 0.06
    confidence -= min(direction.cross_count_total, 5) * 0.025
    confidence -= max(0, direction.cross_count_total - 5) * 0.02
    if direction.quality == "stable":
        confidence += 0.03
    elif direction.quality == "choppy":
        confidence -= 0.12
    elif direction.quality == "fresh_cross":
        confidence -= 0.08
    streak_len = state.prior_same_side_streak_len
    if streak_len >= cfg.direction_confidence_prior_streak_min:
        confidence += 0.12
    elif streak_len >= 2:
        confidence += 0.06
    if (
        distance_bps is not None
        and distance_bps >= cfg.direction_confidence_high_reference_bps
        and direction.cross_count_recent == 0
        and streak_len >= cfg.direction_confidence_prior_streak_min
    ):
        confidence = max(confidence, 0.92)
    return round(_clamp(confidence, 0.0, 0.99), 6)


def _position_direction_confidence(
    *,
    side: str,
    direction: DirectionState | None,
    distance_bps: float | None,
    state: StrategyState,
    cfg: PolySourceConfig,
) -> float | None:
    if direction is None or direction.current_side is None:
        return None
    if direction.current_side != side:
        return 0.0
    return _direction_confidence(direction=direction, distance_bps=distance_bps, state=state, cfg=cfg)


def _contextual_reference_floor(
    *,
    reference_floor: float | None,
    position: PositionSnapshot,
    own_distance: float | None,
    bid: float,
    adverse_bid: float | None,
    remaining_sec: float,
) -> float | None:
    if reference_floor is None or own_distance is None:
        return reference_floor
    if not (45.0 < remaining_sec <= 70.0):
        return reference_floor
    if reference_floor <= 0.25:
        return reference_floor

    reference_still_supports = own_distance >= 1.5 or (
        own_distance >= 1.2 and position.entry_avg_price > 0 and bid >= position.entry_avg_price
    )
    if not reference_still_supports:
        return reference_floor
    if position.entry_avg_price > 0 and bid < position.entry_avg_price * 0.70:
        return reference_floor
    if adverse_bid is not None and adverse_bid - bid > 0.35:
        return reference_floor
    return 0.25


def _hold_score(
    *,
    position: PositionSnapshot,
    own_distance: float | None,
    reference_floor: float | None,
    trend_bps: float | None,
    bid: float,
    adverse_bid: float | None,
    remaining_sec: float,
    hold_to_settlement: bool,
    cfg: PolySourceConfig,
) -> PolyHoldScore:
    effective_floor = _contextual_reference_floor(
        reference_floor=reference_floor,
        position=position,
        own_distance=own_distance,
        bid=bid,
        adverse_bid=adverse_bid,
        remaining_sec=remaining_sec,
    )
    if own_distance is None or reference_floor is None:
        reference_margin = None
        reference_score = -2.0
    else:
        reference_margin = own_distance - effective_floor
        reference_score = min(reference_margin * 1.2, 3.0) if reference_margin >= 0 else reference_margin * 3.0

    trend_score = _clamp((trend_bps or 0.0) * 0.5, -1.0, 1.0)
    if own_distance is None or position.entry_reference_distance_bps is None:
        baseline_score = 0.0
    else:
        baseline_score = _clamp((own_distance - position.entry_reference_distance_bps) / 10.0, -0.5, 0.5)
    if position.entry_avg_price > 0:
        pnl_score = _clamp((bid - position.entry_avg_price) / position.entry_avg_price, -0.75, 0.75)
    else:
        pnl_score = 0.0
    orderbook_score = 0.0
    if position.entry_avg_price > 0 and bid < position.entry_avg_price:
        time_weight = _hold_orderbook_time_weight(remaining_sec, cfg)
        disagreement = max(0.0, (adverse_bid or 0.0) - bid)
        if disagreement > 0.0:
            loss_ratio = (position.entry_avg_price - bid) / position.entry_avg_price
            orderbook_score = -time_weight * _clamp(loss_ratio * 5.0 + disagreement * 12.0, 0.0, 4.5)
    settlement_bonus = 1.0 if hold_to_settlement and reference_margin is not None and reference_margin >= 0 else 0.0
    total = round(reference_score + trend_score + baseline_score + pnl_score + orderbook_score + settlement_bonus, 6)
    return PolyHoldScore(
        total=total,
        floor_bps=effective_floor,
        reference_margin_bps=reference_margin,
        reference_margin_score=round(reference_score, 6),
        trend_score=round(trend_score, 6),
        entry_baseline_score=round(baseline_score, 6),
        pnl_context_score=round(pnl_score, 6),
        orderbook_score=round(orderbook_score, 6),
        settlement_bonus=settlement_bonus,
    )


def _progressive_allowed_loss_ratio(held_sec: float, remaining_sec: float, cfg: PolySourceConfig) -> float:
    initial = max(0.0, cfg.progressive_stop_initial_loss_ratio)
    final = max(0.0, cfg.progressive_stop_final_loss_ratio)
    warmup = max(0.0, cfg.progressive_stop_warmup_sec)
    full = max(warmup, cfg.progressive_stop_full_sec)
    if held_sec <= warmup:
        allowed = initial
    elif held_sec >= full:
        allowed = final
    else:
        progress = (held_sec - warmup) / (full - warmup) if full > warmup else 1.0
        allowed = initial + progress * (final - initial)
    if remaining_sec <= cfg.progressive_stop_late_remaining_sec:
        allowed = min(allowed, final)
    return max(0.0, allowed)


def _progressive_reference_reason(
    *,
    own_distance: float | None,
    reference_floor: float | None,
    remaining_sec: float,
    position: PositionSnapshot,
    cfg: PolySourceConfig,
) -> str | None:
    if own_distance is None:
        return None
    if own_distance < 0.0:
        return "reference_crossed_k"
    if reference_floor is not None and own_distance < reference_floor - _same_side_floor_grace_bps(remaining_sec):
        return "reference_floor_broken"
    if (
        position.entry_reference_distance_bps is not None
        and own_distance <= position.entry_reference_distance_bps - cfg.progressive_stop_reference_deterioration_bps
    ):
        return "reference_deteriorated"
    return None


def _same_side_floor_grace_bps(remaining_sec: float) -> float:
    if remaining_sec > 80.0:
        return 0.75
    if remaining_sec > 45.0:
        return 0.50
    return 0.25


def _direction_thesis_exit_reason(
    *,
    reference_reason: str | None,
    direction: DirectionState | None,
    direction_confidence: float | None,
    side: str,
    cfg: PolySourceConfig,
) -> str | None:
    if reference_reason is None:
        return None
    if reference_reason == "reference_crossed_k":
        return reference_reason
    direction_broken = direction is not None and (
        direction.current_side != side or direction.quality == "choppy"
    )
    confidence_broken = (
        direction_confidence is not None
        and direction_confidence < cfg.exit_min_direction_confidence
    )
    if direction_broken or confidence_broken:
        return reference_reason
    return None


def _record_exit_pressure(state: StrategyState | None, reference_reason: str | None) -> int:
    if state is None:
        return 2 if reference_reason is not None else 0
    if reference_reason is None:
        state.exit_pressure_count = 0
        state.exit_pressure_reason = None
        return 0
    if state.exit_pressure_reason == reference_reason:
        state.exit_pressure_count += 1
    else:
        state.exit_pressure_reason = reference_reason
        state.exit_pressure_count = 1
    return state.exit_pressure_count


def _late_ev_exit_margin(remaining_sec: float, cfg: PolySourceConfig) -> float | None:
    remaining_points = tuple(float(value) for value in cfg.late_ev_exit_remaining_sec)
    margin_points = tuple(float(value) for value in cfg.late_ev_exit_margin)
    if len(remaining_points) != len(margin_points) or not remaining_points:
        return None
    points = sorted(zip(remaining_points, margin_points), key=lambda item: item[0], reverse=True)
    if remaining_sec > points[0][0]:
        return None
    if remaining_sec <= points[-1][0]:
        return points[-1][1]
    for (left_remaining, left_margin), (right_remaining, right_margin) in zip(points, points[1:]):
        if left_remaining >= remaining_sec >= right_remaining:
            span = left_remaining - right_remaining
            if span <= 0:
                return right_margin
            progress = (left_remaining - remaining_sec) / span
            return left_margin + progress * (right_margin - left_margin)
    return points[-1][1]


def _late_ev_exit_should_sell(
    *,
    bid: float,
    direction_confidence: float | None,
    reference_reason: str | None,
    own_distance: float | None,
    direction: DirectionState | None,
    side: str,
    age_sec: float,
    held_sec: float,
    remaining_sec: float,
    cfg: PolySourceConfig,
) -> bool:
    if not cfg.late_ev_exit_enabled:
        return False
    if held_sec < cfg.late_ev_exit_min_hold_sec:
        return False
    if remaining_sec < cfg.late_ev_exit_min_remaining_sec:
        return False
    margin = _late_ev_exit_margin(remaining_sec, cfg)
    if margin is None:
        return False
    if reference_reason is None or direction_confidence is None:
        return False
    if reference_reason != "reference_crossed_k":
        return False
    if reference_reason == "reference_crossed_k":
        cross_depth_bps = abs(own_distance) if own_distance is not None and own_distance < 0.0 else 0.0
        cross_age_sec = 0.0
        if direction is not None and direction.current_side != side and direction.last_cross_age_sec is not None:
            cross_age_sec = max(0.0, age_sec - direction.last_cross_age_sec)
        if cross_depth_bps < cfg.late_ev_exit_min_cross_bps and cross_age_sec < cfg.late_ev_exit_min_cross_sec:
            return False
    return bid > direction_confidence + margin


def _decision(
    action: str,
    reason: str,
    *,
    side: str | None = None,
    price: float | None = None,
    limit_price: float | None = None,
    distance_bps: float | None = None,
    trend_bps: float | None = None,
    cfg: PolySourceConfig,
    score: float | None = None,
    snapshot: MarketSnapshot | None = None,
    market_disagreement: float | None = None,
    profit_now: float | None = None,
    entry_score: PolyEntryScore | None = None,
    hold_score: PolyHoldScore | None = None,
    phase: str | None = None,
    progressive_loss_ratio: float | None = None,
    progressive_allowed_loss_ratio: float | None = None,
    progressive_reference_reason: str | None = None,
    direction_state: DirectionState | None = None,
    direction_confidence: float | None = None,
    state: StrategyState | None = None,
) -> StrategyDecision:
    return StrategyDecision(
        action=action,
        reason=reason,
        side=side,
        phase=phase,
        price=price,
        limit_price=limit_price,
        best_ask=price if action == "enter" else None,
        depth_limit_price=price if action == "enter" else None,
        edge=(entry_score.total if entry_score is not None else score),
        poly_reference_distance_bps=distance_bps,
        poly_return_bps=trend_bps,
        poly_trend_lookback_sec=cfg.poly_trend_lookback_sec,
        poly_return_since_entry_start_bps=(snapshot.poly_return_since_entry_start_bps if snapshot is not None else None),
        poly_entry_score=(entry_score.total if entry_score is not None else score),
        poly_entry_distance_score=(entry_score.distance_score if entry_score is not None else None),
        poly_entry_trend_score=(entry_score.trend_score if entry_score is not None else None),
        poly_entry_price_quality_score=(entry_score.price_quality_score if entry_score is not None else None),
        poly_entry_market_quality_score=(entry_score.market_quality_score if entry_score is not None else None),
        poly_entry_overextended=(entry_score.overextended if entry_score is not None else None),
        direction_quality=(direction_state.quality if direction_state is not None else None),
        direction_current_side=(direction_state.current_side if direction_state is not None else None),
        direction_dominant_side=(direction_state.dominant_side if direction_state is not None else None),
        direction_same_side_duration_sec=(round(direction_state.same_side_duration_sec, 6) if direction_state is not None else None),
        direction_cross_count_total=(direction_state.cross_count_total if direction_state is not None else None),
        direction_cross_count_recent=(direction_state.cross_count_recent if direction_state is not None else None),
        direction_cross_rate_per_min=(round(direction_state.cross_rate_per_min, 6) if direction_state is not None else None),
        direction_support_margin=(round(direction_state.dominant_support_margin, 6) if direction_state is not None else None),
        direction_observed_sec=(round(direction_state.observed_sec, 6) if direction_state is not None else None),
        direction_confidence=direction_confidence,
        prior_streak_len=(state.prior_same_side_streak_len if state is not None else None),
        prior_streak_side=(state.prior_same_side_streak_side if state is not None else None),
        poly_hold_score=(hold_score.total if hold_score is not None else None),
        poly_hold_floor_bps=(hold_score.floor_bps if hold_score is not None else None),
        poly_hold_reference_margin_bps=(hold_score.reference_margin_bps if hold_score is not None else None),
        poly_hold_reference_margin_score=(hold_score.reference_margin_score if hold_score is not None else None),
        poly_hold_trend_score=(hold_score.trend_score if hold_score is not None else None),
        poly_hold_entry_baseline_score=(hold_score.entry_baseline_score if hold_score is not None else None),
        poly_hold_pnl_context_score=(hold_score.pnl_context_score if hold_score is not None else None),
        poly_hold_orderbook_score=(hold_score.orderbook_score if hold_score is not None else None),
        poly_hold_settlement_bonus=(hold_score.settlement_bonus if hold_score is not None else None),
        progressive_stop_loss_ratio=progressive_loss_ratio,
        progressive_stop_allowed_loss_ratio=progressive_allowed_loss_ratio,
        progressive_stop_reference_reason=progressive_reference_reason,
        market_disagreement=market_disagreement,
        profit_now=profit_now,
    )


def evaluate_poly_entry(snapshot: MarketSnapshot, state: StrategyState, cfg: PolySourceConfig) -> StrategyDecision:
    state.record_direction_observation(snapshot, cfg)
    direction = state.direction_state
    if state.has_position:
        return _decision("skip", "already_holding", cfg=cfg, snapshot=snapshot, direction_state=direction, state=state)
    if state.loss_pause_remaining_windows > 0:
        return _decision("skip", "loss_pause", cfg=cfg, snapshot=snapshot, direction_state=direction, state=state)
    if state.entry_count >= cfg.max_entries_per_market:
        return _decision("skip", "max_entries", cfg=cfg, snapshot=snapshot, direction_state=direction, state=state)
    is_reentry = state.entry_count > 0
    if is_reentry:
        if state.last_exit_age_sec is None:
            return _decision("skip", "reentry_missing_exit_age", cfg=cfg, snapshot=snapshot, direction_state=direction, state=state)
        if snapshot.age_sec - state.last_exit_age_sec < cfg.reentry_cooldown_sec:
            return _decision("skip", "reentry_cooldown", cfg=cfg, snapshot=snapshot, direction_state=direction, state=state)

    phase = _entry_phase(snapshot, cfg)
    if not phase.allowed:
        reason = "outside_entry_time" if phase.phase == "outside_window" else phase.phase
        return _decision("skip", reason, cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, state=state)

    if direction is None or direction.current_side is None:
        return _decision("skip", "direction_insufficient_history", cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, state=state)
    side = direction.current_side
    if direction.quality in {"insufficient_history", "choppy", "fresh_cross"}:
        return _decision("skip", f"direction_{direction.quality}", side=side, cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, state=state)
    if side is None:
        return _decision("skip", "missing_poly_reference", cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, state=state)
    distance = _distance_bps(snapshot, side)
    direction_confidence = _direction_confidence(direction=direction, distance_bps=distance, state=state, cfg=cfg)
    if cfg.direction_confidence_enabled and (direction_confidence is None or direction_confidence < cfg.min_direction_confidence):
        return _decision("skip", "direction_confidence_too_low", side=side, distance_bps=distance, cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    if distance is None or distance < cfg.poly_reference_distance_bps:
        return _decision("skip", "poly_reference_not_confirmed", side=side, distance_bps=distance, cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    if cfg.max_poly_reference_distance_bps > 0 and distance > cfg.max_poly_reference_distance_bps:
        return _decision("skip", "poly_reference_distance_too_high", side=side, distance_bps=distance, cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    trend = _trend_bps(snapshot, cfg.poly_trend_lookback_sec, side)
    if trend is None or trend < cfg.poly_return_bps:
        return _decision("skip", "poly_trend_not_confirmed", side=side, distance_bps=distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    ask = _ask(snapshot, side)
    if ask is None:
        return _decision("skip", "missing_entry_price", side=side, distance_bps=distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    if not _book_fresh(snapshot, side, cfg):
        return _decision("skip", "stale_entry_book", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    if ask > cfg.max_entry_ask:
        return _decision("skip", "poly_ask_too_high", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    max_fill = _max_entry_fill_price(cfg)
    if max_fill is not None and ask > max_fill:
        return _decision("skip", "poly_fill_cap_exceeded", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    reentry_max_fill = _reentry_max_entry_fill_price(cfg)
    if is_reentry and reentry_max_fill is not None and ask > reentry_max_fill:
        return _decision("skip", "reentry_fill_cap_exceeded", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    score = _entry_score(distance, trend, ask, _bid(snapshot, side))
    score_override = cfg.direction_confidence_score_override and direction_confidence is not None and direction_confidence >= cfg.min_direction_confidence
    if score.total < cfg.min_poly_entry_score and not score_override:
        return _decision("skip", "poly_score_too_low", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, entry_score=score, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    if is_reentry and score.total < cfg.min_poly_entry_score + cfg.reentry_min_score_bonus:
        return _decision("skip", "reentry_score_too_low", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, entry_score=score, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    if phase.phase == "early_value":
        bid = _bid(snapshot, side)
        spread = (ask - bid) if bid is not None else None
        if distance < cfg.early_value_min_reference_distance_bps:
            return _decision("skip", "early_value_reference_too_weak", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, entry_score=score, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
        if trend < cfg.early_value_min_poly_return_bps:
            return _decision("skip", "early_value_trend_too_weak", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, entry_score=score, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
        if ask > cfg.early_value_max_entry_ask:
            return _decision("skip", "early_value_ask_too_high", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, entry_score=score, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
        if cfg.early_value_max_spread > 0 and (spread is None or spread > cfg.early_value_max_spread):
            return _decision("skip", "early_value_spread_too_wide", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, entry_score=score, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
        if score.total < cfg.early_value_min_entry_score:
            return _decision("skip", "early_value_score_too_low", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, entry_score=score, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)
    limit_candidates = [1.0]
    if max_fill is not None:
        limit_candidates.append(max_fill)
    if is_reentry and reentry_max_fill is not None:
        limit_candidates.append(reentry_max_fill)
    limit = min(limit_candidates)
    reason = "poly_early_value" if phase.phase == "early_value" else "poly_edge"
    return _decision("enter", reason, side=side, price=ask, limit_price=limit, distance_bps=distance, trend_bps=trend, cfg=cfg, entry_score=score, snapshot=snapshot, phase=phase.phase, direction_state=direction, direction_confidence=direction_confidence, state=state)


def evaluate_poly_exit(snapshot: MarketSnapshot, position: PositionSnapshot, cfg: PolySourceConfig, state: StrategyState | None = None) -> StrategyDecision:
    if state is not None:
        state.record_direction_observation(snapshot, cfg)
    direction = state.direction_state if state is not None else None
    side = position.token_side
    bid = _bid(snapshot, side)
    bid_limit = _bid_limit(snapshot, side)
    if bid is None or bid_limit is None:
        return _decision("hold", "missing_exit_depth", side=side, price=bid, limit_price=bid_limit, cfg=cfg, snapshot=snapshot, direction_state=direction, state=state)
    profit_now = bid - position.entry_avg_price
    adverse_side = "down" if side == "up" else "up"
    adverse_distance = _distance_bps(snapshot, adverse_side)
    own_distance = _distance_bps(snapshot, side)
    direction_confidence = (
        _position_direction_confidence(side=side, direction=direction, distance_bps=own_distance, state=state, cfg=cfg)
        if state is not None
        else None
    )
    trend = _trend_bps(snapshot, cfg.poly_trend_lookback_sec, side)
    hold_to_settlement = _hold_to_settlement(position, bid, bid_limit, profit_now, own_distance, trend, cfg)
    held_sec = snapshot.age_sec - position.entry_time
    if not _depth_ok(snapshot, side):
        return _decision("hold", "missing_exit_depth", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now, direction_state=direction, direction_confidence=direction_confidence, state=state)
    reference_floor = _reference_distance_exit_floor(snapshot.remaining_sec, cfg)
    hold_score = _hold_score(
        position=position,
        own_distance=own_distance,
        reference_floor=reference_floor,
        trend_bps=trend,
        bid=bid,
        adverse_bid=_bid(snapshot, adverse_side),
        remaining_sec=snapshot.remaining_sec,
        hold_to_settlement=hold_to_settlement,
        cfg=cfg,
    )
    loss_ratio = 0.0
    if position.entry_avg_price > 0:
        loss_ratio = max(0.0, (position.entry_avg_price - bid) / position.entry_avg_price)
    allowed_loss_ratio = _progressive_allowed_loss_ratio(held_sec, snapshot.remaining_sec, cfg)
    reference_reason = _progressive_reference_reason(
        own_distance=own_distance,
        reference_floor=reference_floor,
        remaining_sec=snapshot.remaining_sec,
        position=position,
        cfg=cfg,
    )
    near_zero_loss_ratio = max(cfg.progressive_stop_extreme_loss_ratio, 0.90)
    if (
        held_sec >= cfg.exit_min_hold_sec
        and loss_ratio >= near_zero_loss_ratio
    ):
        return _decision(
            "exit",
            "extreme_loss_exit",
            side=side,
            price=bid,
            limit_price=bid_limit,
            distance_bps=own_distance,
            trend_bps=trend,
            cfg=cfg,
            snapshot=snapshot,
            profit_now=profit_now,
            hold_score=hold_score,
            progressive_loss_ratio=loss_ratio,
            progressive_allowed_loss_ratio=allowed_loss_ratio,
            progressive_reference_reason=reference_reason,
            direction_state=direction,
            direction_confidence=direction_confidence,
            state=state,
        )
    if _late_ev_exit_should_sell(
        bid=bid,
        direction_confidence=direction_confidence,
        reference_reason=reference_reason,
        own_distance=own_distance,
        direction=direction,
        side=side,
        age_sec=snapshot.age_sec,
        held_sec=held_sec,
        remaining_sec=snapshot.remaining_sec,
        cfg=cfg,
    ):
        return _decision(
            "exit",
            "late_ev_exit",
            side=side,
            price=bid,
            limit_price=bid_limit,
            distance_bps=own_distance,
            trend_bps=trend,
            cfg=cfg,
            snapshot=snapshot,
            profit_now=profit_now,
            hold_score=hold_score,
            progressive_loss_ratio=loss_ratio,
            progressive_allowed_loss_ratio=allowed_loss_ratio,
            progressive_reference_reason=reference_reason,
            direction_state=direction,
            direction_confidence=direction_confidence,
            state=state,
        )
    thesis_exit_reason = _direction_thesis_exit_reason(
        reference_reason=reference_reason,
        direction=direction,
        direction_confidence=direction_confidence,
        side=side,
        cfg=cfg,
    )
    if (
        not hold_to_settlement
        and held_sec >= max(cfg.exit_min_hold_sec, cfg.exit_direction_confidence_min_hold_sec)
        and thesis_exit_reason is not None
    ):
        pressure_count = _record_exit_pressure(state, thesis_exit_reason)
        required_pressure_count = 1 if snapshot.remaining_sec <= 60.0 and thesis_exit_reason == "reference_crossed_k" else 2
        if pressure_count < required_pressure_count:
            return _decision(
                "hold",
                "exit_pressure_pending",
                side=side,
                price=bid,
                limit_price=bid_limit,
                distance_bps=own_distance,
                trend_bps=trend,
                cfg=cfg,
                snapshot=snapshot,
                profit_now=profit_now,
                hold_score=hold_score,
                progressive_loss_ratio=loss_ratio,
                progressive_allowed_loss_ratio=allowed_loss_ratio,
                progressive_reference_reason=reference_reason,
                direction_state=direction,
                direction_confidence=direction_confidence,
                state=state,
            )
        return _decision(
            "exit",
            "direction_thesis_exit",
            side=side,
            price=bid,
            limit_price=bid_limit,
            distance_bps=own_distance,
            trend_bps=trend,
            cfg=cfg,
            snapshot=snapshot,
            profit_now=profit_now,
            hold_score=hold_score,
            progressive_loss_ratio=loss_ratio,
            progressive_allowed_loss_ratio=allowed_loss_ratio,
            progressive_reference_reason=reference_reason,
            direction_state=direction,
            direction_confidence=direction_confidence,
            state=state,
        )
    else:
        _record_exit_pressure(state, None)
    if hold_to_settlement:
        return _decision("hold", "hold_to_settlement", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now, hold_score=hold_score, progressive_loss_ratio=loss_ratio, progressive_allowed_loss_ratio=allowed_loss_ratio, progressive_reference_reason=reference_reason, direction_state=direction, direction_confidence=direction_confidence, state=state)
    return _decision("hold", "poly_edge_intact", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now, hold_score=hold_score, progressive_loss_ratio=loss_ratio, progressive_allowed_loss_ratio=allowed_loss_ratio, progressive_reference_reason=reference_reason, direction_state=direction, direction_confidence=direction_confidence, state=state)


def _reference_distance_exit_floor(remaining_sec: float, cfg: PolySourceConfig) -> float | None:
    remaining_points = tuple(float(value) for value in cfg.reference_distance_exit_remaining_sec)
    floor_points = tuple(float(value) for value in cfg.reference_distance_exit_min_bps)
    if len(remaining_points) != len(floor_points) or not remaining_points:
        return None
    points = sorted(zip(remaining_points, floor_points), key=lambda item: item[0], reverse=True)
    if remaining_sec >= points[0][0]:
        return points[0][1]
    if remaining_sec <= points[-1][0]:
        return points[-1][1]
    for (left_remaining, left_floor), (right_remaining, right_floor) in zip(points, points[1:]):
        if left_remaining >= remaining_sec >= right_remaining:
            span = left_remaining - right_remaining
            if span <= 0:
                return right_floor
            progress = (left_remaining - remaining_sec) / span
            return left_floor + progress * (right_floor - left_floor)
    return points[-1][1]



def _hold_to_settlement(
    position: PositionSnapshot,
    bid: float,
    bid_limit: float,
    profit_now: float,
    distance_bps: float | None,
    trend_bps: float | None,
    cfg: PolySourceConfig,
) -> bool:
    if not cfg.hold_to_settlement_enabled or position.entry_avg_price <= 0:
        return False
    return (
        profit_now / position.entry_avg_price >= cfg.hold_to_settlement_min_profit_ratio
        and bid >= cfg.hold_to_settlement_min_bid_avg
        and bid_limit >= cfg.hold_to_settlement_min_bid_limit
        and distance_bps is not None
        and distance_bps >= cfg.hold_to_settlement_min_reference_distance_bps
        and trend_bps is not None
        and trend_bps >= cfg.hold_to_settlement_min_poly_return_bps
    )


@dataclass(frozen=True)
class _EntryPhase:
    phase: str
    allowed: bool


def _entry_phase(snapshot: MarketSnapshot, cfg: PolySourceConfig) -> _EntryPhase:
    if snapshot.remaining_sec <= cfg.final_no_entry_remaining_sec:
        return _EntryPhase("final_no_entry", False)
    if (
        cfg.early_value_entry_enabled
        and cfg.early_value_start_age_sec <= snapshot.age_sec < cfg.early_value_end_age_sec
    ):
        return _EntryPhase("early_value", True)
    if snapshot.age_sec < cfg.entry_start_age_sec or snapshot.age_sec > cfg.entry_end_age_sec:
        return _EntryPhase("outside_window", False)
    if snapshot.age_sec < cfg.early_to_core_age_sec:
        return _EntryPhase("early", True)
    if snapshot.age_sec < cfg.core_to_late_age_sec:
        return _EntryPhase("core", True)
    return _EntryPhase("late", True)
