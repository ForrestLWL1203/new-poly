"""Polymarket-only entry and exit decisions."""

from __future__ import annotations

from dataclasses import dataclass

from .prob_edge import MarketSnapshot, StrategyDecision
from .state import PositionSnapshot, StrategyState


@dataclass(frozen=True)
class PolySourceConfig:
    entry_start_age_sec: float = 100.0
    entry_end_age_sec: float = 240.0
    final_no_entry_remaining_sec: float = 30.0
    early_to_core_age_sec: float = 120.0
    core_to_late_age_sec: float = 240.0
    max_entries_per_market: int = 1
    max_book_age_ms: float = 1000.0
    poly_reference_distance_bps: float = 0.5
    max_poly_reference_distance_bps: float = 0.0
    poly_trend_lookback_sec: float = 3.0
    poly_return_bps: float = 0.3
    max_entry_ask: float = 0.65
    max_entry_fill_price: float = 0.0
    min_poly_entry_score: float = 0.0
    min_poly_hold_score: float = 0.0
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


def _hold_orderbook_time_weight(remaining_sec: float) -> float:
    if remaining_sec > 110.0:
        return 0.0
    if remaining_sec > 90.0:
        return (110.0 - remaining_sec) / 20.0 * 0.45
    if remaining_sec > 60.0:
        return 0.45 + (90.0 - remaining_sec) / 30.0 * 0.30
    return 0.75 + (60.0 - max(0.0, remaining_sec)) / 60.0 * 0.25


def _entry_score(distance_bps: float, trend_bps: float, ask: float, bid: float | None) -> PolyEntryScore:
    distance_score, overextended = _distance_score(distance_bps)
    trend_score = min(max(trend_bps, 0.0), 2.5) * 1.5
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
) -> PolyHoldScore:
    if own_distance is None or reference_floor is None:
        reference_margin = None
        reference_score = -2.0
    else:
        reference_margin = own_distance - reference_floor
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
        time_weight = _hold_orderbook_time_weight(remaining_sec)
        disagreement = max(0.0, (adverse_bid or 0.0) - bid)
        if disagreement > 0.0:
            loss_ratio = (position.entry_avg_price - bid) / position.entry_avg_price
            orderbook_score = -time_weight * _clamp(loss_ratio * 5.0 + disagreement * 12.0, 0.0, 4.5)
    settlement_bonus = 1.0 if hold_to_settlement and reference_margin is not None and reference_margin >= 0 else 0.0
    total = round(reference_score + trend_score + baseline_score + pnl_score + orderbook_score + settlement_bonus, 6)
    return PolyHoldScore(
        total=total,
        floor_bps=reference_floor,
        reference_margin_bps=reference_margin,
        reference_margin_score=round(reference_score, 6),
        trend_score=round(trend_score, 6),
        entry_baseline_score=round(baseline_score, 6),
        pnl_context_score=round(pnl_score, 6),
        orderbook_score=round(orderbook_score, 6),
        settlement_bonus=settlement_bonus,
    )


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
) -> StrategyDecision:
    return StrategyDecision(
        action=action,
        reason=reason,
        side=side,
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
        poly_hold_score=(hold_score.total if hold_score is not None else None),
        poly_hold_floor_bps=(hold_score.floor_bps if hold_score is not None else None),
        poly_hold_reference_margin_bps=(hold_score.reference_margin_bps if hold_score is not None else None),
        poly_hold_reference_margin_score=(hold_score.reference_margin_score if hold_score is not None else None),
        poly_hold_trend_score=(hold_score.trend_score if hold_score is not None else None),
        poly_hold_entry_baseline_score=(hold_score.entry_baseline_score if hold_score is not None else None),
        poly_hold_pnl_context_score=(hold_score.pnl_context_score if hold_score is not None else None),
        poly_hold_orderbook_score=(hold_score.orderbook_score if hold_score is not None else None),
        poly_hold_settlement_bonus=(hold_score.settlement_bonus if hold_score is not None else None),
        market_disagreement=market_disagreement,
        profit_now=profit_now,
    )


def evaluate_poly_entry(snapshot: MarketSnapshot, state: StrategyState, cfg: PolySourceConfig) -> StrategyDecision:
    if state.has_position:
        return _decision("skip", "already_holding", cfg=cfg, snapshot=snapshot)
    if state.loss_pause_remaining_windows > 0:
        return _decision("skip", "loss_pause", cfg=cfg, snapshot=snapshot)
    if state.entry_count >= cfg.max_entries_per_market:
        return _decision("skip", "max_entries", cfg=cfg, snapshot=snapshot)

    phase = _entry_phase(snapshot, cfg)
    if not phase.allowed:
        reason = "outside_entry_time" if phase.phase == "outside_window" else phase.phase
        return _decision("skip", reason, cfg=cfg, snapshot=snapshot)

    side = _raw_poly_side(snapshot)
    if side is None:
        return _decision("skip", "missing_poly_reference", cfg=cfg, snapshot=snapshot)
    distance = _distance_bps(snapshot, side)
    if distance is None or distance < cfg.poly_reference_distance_bps:
        return _decision("skip", "poly_reference_not_confirmed", side=side, distance_bps=distance, cfg=cfg, snapshot=snapshot)
    if cfg.max_poly_reference_distance_bps > 0 and distance > cfg.max_poly_reference_distance_bps:
        return _decision("skip", "poly_reference_distance_too_high", side=side, distance_bps=distance, cfg=cfg, snapshot=snapshot)
    trend = _trend_bps(snapshot, cfg.poly_trend_lookback_sec, side)
    if trend is None or trend < cfg.poly_return_bps:
        return _decision("skip", "poly_trend_not_confirmed", side=side, distance_bps=distance, trend_bps=trend, cfg=cfg, snapshot=snapshot)
    ask = _ask(snapshot, side)
    if ask is None:
        return _decision("skip", "missing_entry_price", side=side, distance_bps=distance, trend_bps=trend, cfg=cfg, snapshot=snapshot)
    if not _book_fresh(snapshot, side, cfg):
        return _decision("skip", "stale_entry_book", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, snapshot=snapshot)
    if ask > cfg.max_entry_ask:
        return _decision("skip", "poly_ask_too_high", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, snapshot=snapshot)
    max_fill = _max_entry_fill_price(cfg)
    if max_fill is not None and ask > max_fill:
        return _decision("skip", "poly_fill_cap_exceeded", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, snapshot=snapshot)
    score = _entry_score(distance, trend, ask, _bid(snapshot, side))
    if score.total < cfg.min_poly_entry_score:
        return _decision("skip", "poly_score_too_low", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, entry_score=score, snapshot=snapshot)
    limit = min(1.0, max_fill if max_fill is not None else 1.0)
    return _decision("enter", "poly_edge", side=side, price=ask, limit_price=limit, distance_bps=distance, trend_bps=trend, cfg=cfg, entry_score=score, snapshot=snapshot)


def evaluate_poly_exit(snapshot: MarketSnapshot, position: PositionSnapshot, cfg: PolySourceConfig, state: StrategyState | None = None) -> StrategyDecision:
    side = position.token_side
    bid = _bid(snapshot, side)
    bid_limit = _bid_limit(snapshot, side)
    if bid is None or bid_limit is None:
        return _decision("hold", "missing_exit_depth", side=side, price=bid, limit_price=bid_limit, cfg=cfg, snapshot=snapshot)
    profit_now = bid - position.entry_avg_price
    adverse_side = "down" if side == "up" else "up"
    adverse_distance = _distance_bps(snapshot, adverse_side)
    own_distance = _distance_bps(snapshot, side)
    trend = _trend_bps(snapshot, cfg.poly_trend_lookback_sec, side)
    hold_to_settlement = _hold_to_settlement(position, bid, bid_limit, profit_now, own_distance, trend, cfg)
    held_sec = snapshot.age_sec - position.entry_time
    if not _depth_ok(snapshot, side):
        return _decision("hold", "missing_exit_depth", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now)
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
    )
    if held_sec >= cfg.exit_min_hold_sec and hold_score.total < cfg.min_poly_hold_score:
        return _decision("exit", "poly_hold_score_exit", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now, hold_score=hold_score)
    if hold_to_settlement:
        return _decision("hold", "hold_to_settlement", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now, hold_score=hold_score)
    return _decision("hold", "poly_edge_intact", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now, hold_score=hold_score)


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
    if snapshot.age_sec < cfg.entry_start_age_sec or snapshot.age_sec > cfg.entry_end_age_sec:
        return _EntryPhase("outside_window", False)
    if snapshot.age_sec < cfg.early_to_core_age_sec:
        return _EntryPhase("early", True)
    if snapshot.age_sec < cfg.core_to_late_age_sec:
        return _EntryPhase("core", True)
    return _EntryPhase("late", True)
