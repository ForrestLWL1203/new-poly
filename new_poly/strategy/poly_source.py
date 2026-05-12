"""Polymarket-only entry and exit decisions."""

from __future__ import annotations

from dataclasses import dataclass

from .prob_edge import MarketSnapshot, StrategyDecision, required_edge_for_entry
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
    poly_trend_lookback_sec: float = 3.0
    poly_return_bps: float = 0.3
    max_entry_ask: float = 0.65
    max_entry_fill_price: float = 0.0
    min_poly_entry_score: float = 0.0
    entry_tick_size: float = 0.01
    buy_price_buffer_ticks: float = 2.0
    exit_reference_adverse_bps: float = 1.0
    exit_min_hold_sec: float = 3.0
    poly_trend_reversal_bps: float = 0.3
    market_disagrees_exit_threshold: float = 0.55
    market_disagrees_exit_min_age_sec: float = 3.0
    final_force_exit_remaining_sec: float = 30.0
    final_profit_hold_min_profit_ratio: float = 0.10
    profit_protection_start_remaining_sec: float = 90.0
    profit_protection_end_remaining_sec: float = 45.0
    profit_protection_min_profit: float = 0.08
    profit_protection_trend_weak_bps: float = 0.0
    late_depth_guard_remaining_sec: float = 90.0
    late_depth_min_bid_avg: float = 0.20
    late_depth_min_bid_limit: float = 0.15
    hold_to_settlement_enabled: bool = True
    hold_to_settlement_min_profit_ratio: float = 0.50
    hold_to_settlement_min_bid_avg: float = 0.80
    hold_to_settlement_min_bid_limit: float = 0.75
    hold_to_settlement_min_reference_distance_bps: float = 1.0
    hold_to_settlement_min_poly_return_bps: float = 0.0


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


def _entry_score(distance_bps: float, trend_bps: float, ask: float, bid: float | None) -> float:
    distance_score = min(max(distance_bps, 0.0), 5.0)
    trend_score = min(max(trend_bps, 0.0), 2.0) * 2.0
    market_score = 0.0
    if bid is not None:
        spread = max(0.0, ask - bid)
        market_score = max(0.0, 1.0 - spread / 0.05)
    return round(distance_score + trend_score + market_score, 6)


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
) -> StrategyDecision:
    return StrategyDecision(
        action=action,
        reason=reason,
        side=side,
        price=price,
        limit_price=limit_price,
        best_ask=price if action == "enter" else None,
        depth_limit_price=price if action == "enter" else None,
        edge=score,
        poly_reference_distance_bps=distance_bps,
        poly_return_bps=trend_bps,
        poly_trend_lookback_sec=cfg.poly_trend_lookback_sec,
        poly_return_since_entry_start_bps=(snapshot.poly_return_since_entry_start_bps if snapshot is not None else None),
        poly_entry_score=score,
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

    phase_cfg = _phase_adapter(cfg)
    phase = required_edge_for_entry(snapshot, phase_cfg)
    if not phase.allowed:
        reason = "outside_entry_time" if phase.phase == "outside_window" else phase.phase
        return _decision("skip", reason, cfg=cfg, snapshot=snapshot)

    side = _raw_poly_side(snapshot)
    if side is None:
        return _decision("skip", "missing_poly_reference", cfg=cfg, snapshot=snapshot)
    distance = _distance_bps(snapshot, side)
    if distance is None or distance < cfg.poly_reference_distance_bps:
        return _decision("skip", "poly_reference_not_confirmed", side=side, distance_bps=distance, cfg=cfg, snapshot=snapshot)
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
    if score < cfg.min_poly_entry_score:
        return _decision("skip", "poly_score_too_low", side=side, price=ask, distance_bps=distance, trend_bps=trend, cfg=cfg, score=score, snapshot=snapshot)
    tick = cfg.entry_tick_size if cfg.entry_tick_size > 0 else 0.01
    limit_cap = max_fill if max_fill is not None else 1.0
    limit = min(1.0, limit_cap, round(ask + cfg.buy_price_buffer_ticks * tick, 6))
    return _decision("enter", "poly_edge", side=side, price=ask, limit_price=limit, distance_bps=distance, trend_bps=trend, cfg=cfg, score=score, snapshot=snapshot)


def evaluate_poly_exit(snapshot: MarketSnapshot, position: PositionSnapshot, cfg: PolySourceConfig, state: StrategyState | None = None) -> StrategyDecision:
    side = position.token_side
    bid = _bid(snapshot, side)
    bid_limit = _bid_limit(snapshot, side)
    if bid is None or bid_limit is None:
        reason = "late_exit_depth_unavailable" if _in_late_depth_guard(snapshot, cfg) else "missing_exit_depth"
        return _decision("hold", reason, side=side, price=bid, limit_price=bid_limit, cfg=cfg, snapshot=snapshot)
    profit_now = bid - position.entry_avg_price
    adverse_side = "down" if side == "up" else "up"
    adverse_distance = _distance_bps(snapshot, adverse_side)
    own_distance = _distance_bps(snapshot, side)
    trend = _trend_bps(snapshot, cfg.poly_trend_lookback_sec, side)
    hold_to_settlement = _hold_to_settlement(position, bid, bid_limit, profit_now, own_distance, trend, cfg)
    held_sec = snapshot.age_sec - position.entry_time
    if not _depth_ok(snapshot, side):
        if _in_late_depth_guard(snapshot, cfg):
            return _decision("exit", "late_depth_risk_exit", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now)
        return _decision("hold", "missing_exit_depth", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now)
    if held_sec >= cfg.exit_min_hold_sec and adverse_distance is not None and adverse_distance >= cfg.exit_reference_adverse_bps:
        return _decision("exit", "reference_adverse_exit", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now)
    if held_sec >= cfg.exit_min_hold_sec and trend is not None and trend <= -cfg.poly_trend_reversal_bps and profit_now < 0:
        return _decision("exit", "poly_trend_reversal_exit", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now)
    disagreement = _market_disagreement(snapshot, position, bid, profit_now, cfg)
    if disagreement is not None:
        return _decision("exit", "market_disagrees_exit", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, market_disagreement=disagreement, profit_now=profit_now)
    if hold_to_settlement:
        if snapshot.remaining_sec <= cfg.profit_protection_start_remaining_sec:
            return _decision("hold", "hold_to_settlement", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now)
    elif _profit_protection_exit(snapshot, profit_now, trend, cfg):
        return _decision("exit", "profit_protection_exit", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now)
    elif _late_depth_risk(snapshot, bid, bid_limit, cfg):
        return _decision("exit", "late_depth_risk_exit", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now)
    if snapshot.remaining_sec <= cfg.final_force_exit_remaining_sec:
        if hold_to_settlement or _final_profit_hold(position, profit_now, cfg):
            return _decision("hold", "hold_to_settlement", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now)
        return _decision("exit", "final_force_exit", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now)
    return _decision("hold", "poly_edge_intact", side=side, price=bid, limit_price=bid_limit, distance_bps=own_distance, trend_bps=trend, cfg=cfg, snapshot=snapshot, profit_now=profit_now)


def _market_disagreement(snapshot: MarketSnapshot, position: PositionSnapshot, bid: float, profit_now: float, cfg: PolySourceConfig) -> float | None:
    if cfg.market_disagrees_exit_threshold <= 0:
        return None
    if cfg.market_disagrees_exit_min_age_sec > 0 and snapshot.age_sec - position.entry_time < cfg.market_disagrees_exit_min_age_sec:
        return None
    if position.entry_avg_price <= 0:
        return None
    ratio = bid / position.entry_avg_price
    return ratio if ratio <= cfg.market_disagrees_exit_threshold else None


def _final_profit_hold(position: PositionSnapshot, profit_now: float, cfg: PolySourceConfig) -> bool:
    return position.entry_avg_price > 0 and profit_now / position.entry_avg_price >= cfg.final_profit_hold_min_profit_ratio


def _in_late_depth_guard(snapshot: MarketSnapshot, cfg: PolySourceConfig) -> bool:
    return cfg.late_depth_guard_remaining_sec > 0 and snapshot.remaining_sec <= cfg.late_depth_guard_remaining_sec


def _profit_protection_exit(snapshot: MarketSnapshot, profit_now: float, trend_bps: float | None, cfg: PolySourceConfig) -> bool:
    if cfg.profit_protection_min_profit <= 0:
        return False
    if not (cfg.profit_protection_end_remaining_sec < snapshot.remaining_sec <= cfg.profit_protection_start_remaining_sec):
        return False
    if profit_now < cfg.profit_protection_min_profit:
        return False
    return trend_bps is not None and trend_bps <= cfg.profit_protection_trend_weak_bps


def _late_depth_risk(snapshot: MarketSnapshot, bid: float, bid_limit: float, cfg: PolySourceConfig) -> bool:
    if not _in_late_depth_guard(snapshot, cfg):
        return False
    return bid < cfg.late_depth_min_bid_avg or bid_limit < cfg.late_depth_min_bid_limit


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


def _phase_adapter(cfg: PolySourceConfig):
    from .prob_edge import EdgeConfig

    return EdgeConfig(
        entry_start_age_sec=cfg.entry_start_age_sec,
        entry_end_age_sec=cfg.entry_end_age_sec,
        early_to_core_age_sec=cfg.early_to_core_age_sec,
        core_to_late_age_sec=cfg.core_to_late_age_sec,
        final_no_entry_remaining_sec=cfg.final_no_entry_remaining_sec,
        max_entries_per_market=cfg.max_entries_per_market,
    )
