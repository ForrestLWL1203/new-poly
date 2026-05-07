"""Pure probability-edge entry and exit decisions."""

from __future__ import annotations

from dataclasses import dataclass

from .probability import binary_probabilities
from .state import PositionSnapshot, StrategyState


@dataclass(frozen=True)
class EdgeConfig:
    early_required_edge: float = 0.16
    core_required_edge: float = 0.14
    model_decay_buffer: float = 0.03
    overprice_buffer: float = 0.02
    entry_start_age_sec: float = 90.0
    entry_end_age_sec: float = 270.0
    early_to_core_age_sec: float = 120.0
    core_to_late_age_sec: float = 240.0
    dynamic_entry_enabled: bool = False
    fast_move_entry_start_age_sec: float = 70.0
    fast_move_min_abs_sk_usd: float = 80.0
    fast_move_required_edge: float = 0.22
    strong_move_entry_start_age_sec: float = 60.0
    strong_move_min_abs_sk_usd: float = 120.0
    strong_move_required_edge: float = 0.24
    final_no_entry_remaining_sec: float = 30.0
    max_entries_per_market: int = 2
    max_book_age_ms: float = 1000.0
    late_entry_enabled: bool = False
    late_required_edge: float = 0.10
    late_max_spread: float = 0.02
    defensive_profit_min: float = 0.03
    protection_profit_min: float = 0.01
    profit_protection_start_remaining_sec: float = 15.0
    profit_protection_end_remaining_sec: float = 30.0
    defensive_take_profit_start_remaining_sec: float = 30.0
    defensive_take_profit_end_remaining_sec: float = 60.0
    final_force_exit_remaining_sec: float = 30.0
    final_hold_min_prob: float = 0.98
    final_hold_min_bid_avg: float = 0.97
    final_hold_min_bid_limit: float = 0.95
    prob_stagnation_window_sec: float = 3.0
    prob_stagnation_epsilon: float = 0.002
    prob_drop_exit_window_sec: float = 0.0
    prob_drop_exit_threshold: float = 0.0
    min_fair_cap_margin_ticks: float = 0.0
    entry_tick_size: float = 0.01
    min_entry_model_prob: float = 0.0
    low_price_extra_edge_threshold: float = 0.0
    low_price_extra_edge: float = 0.0
    cross_source_max_bps: float = 0.0
    market_disagrees_exit_threshold: float = 0.0
    market_disagrees_exit_max_remaining_sec: float = 0.0
    market_disagrees_exit_min_loss: float = 0.0
    market_disagrees_exit_min_age_sec: float = 0.0
    market_disagrees_exit_max_profit: float = 0.01
    polymarket_divergence_exit_bps: float = 3.0
    polymarket_divergence_exit_min_age_sec: float = 3.0


@dataclass(frozen=True)
class EntryPhase:
    phase: str
    allowed: bool
    required_edge: float | None


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
    up_ask_safety_limit: float | None = None
    down_ask_safety_limit: float | None = None
    up_best_ask: float | None = None
    down_best_ask: float | None = None
    up_bid_avg: float | None = None
    down_bid_avg: float | None = None
    up_bid_limit: float | None = None
    down_bid_limit: float | None = None
    up_ask_depth_ok: bool = False
    down_ask_depth_ok: bool = False
    up_bid_depth_ok: bool = False
    down_bid_depth_ok: bool = False
    up_book_age_ms: float | None = None
    down_book_age_ms: float | None = None
    source_spread_bps: float | None = None
    polymarket_divergence_bps: float | None = None


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


def _missing_model_inputs(snapshot: MarketSnapshot) -> bool:
    return snapshot.s_price is None or snapshot.k_price is None or snapshot.sigma_eff is None


def _stale_books(snapshot: MarketSnapshot, cfg: EdgeConfig) -> bool:
    ages = [snapshot.up_book_age_ms, snapshot.down_book_age_ms]
    return any(age is None or age > cfg.max_book_age_ms for age in ages)


def _probs(snapshot: MarketSnapshot):
    assert snapshot.s_price is not None
    assert snapshot.k_price is not None
    assert snapshot.sigma_eff is not None
    return binary_probabilities(snapshot.s_price, snapshot.k_price, snapshot.sigma_eff, snapshot.remaining_sec)


def required_edge_for_entry(snapshot: MarketSnapshot, cfg: EdgeConfig) -> EntryPhase:
    if snapshot.remaining_sec <= cfg.final_no_entry_remaining_sec:
        return EntryPhase("final_no_entry", False, None)
    if snapshot.age_sec > cfg.entry_end_age_sec:
        return EntryPhase("outside_window", False, None)
    if snapshot.age_sec < cfg.entry_start_age_sec:
        if cfg.dynamic_entry_enabled and snapshot.s_price is not None and snapshot.k_price is not None:
            abs_sk = abs(snapshot.s_price - snapshot.k_price)
            if snapshot.age_sec >= cfg.strong_move_entry_start_age_sec and abs_sk >= cfg.strong_move_min_abs_sk_usd:
                return EntryPhase("strong_move", True, cfg.strong_move_required_edge)
            if snapshot.age_sec >= cfg.fast_move_entry_start_age_sec and abs_sk >= cfg.fast_move_min_abs_sk_usd:
                return EntryPhase("fast_move", True, cfg.fast_move_required_edge)
        return EntryPhase("outside_window", False, None)
    if snapshot.age_sec < cfg.early_to_core_age_sec:
        return EntryPhase("early", True, cfg.early_required_edge)
    if snapshot.age_sec < cfg.core_to_late_age_sec:
        return EntryPhase("core", True, cfg.core_required_edge)
    if not cfg.late_entry_enabled:
        return EntryPhase("late_disabled", False, None)
    return EntryPhase("late", True, cfg.late_required_edge)


def _spread_ok(snapshot: MarketSnapshot, side: str, cfg: EdgeConfig, phase: EntryPhase) -> bool:
    if phase.phase != "late":
        return True
    if side == "up":
        bid, ask = snapshot.up_bid_avg, snapshot.up_ask_avg
    else:
        bid, ask = snapshot.down_bid_avg, snapshot.down_ask_avg
    return bid is not None and ask is not None and ask - bid < cfg.late_max_spread


def _entry_depth_ok(depth_limit: float, safety_limit: float | None, fair_cap: float, cfg: EdgeConfig) -> bool:
    if depth_limit > fair_cap:
        return False
    if safety_limit is not None and safety_limit > fair_cap:
        return False
    tick = cfg.entry_tick_size if cfg.entry_tick_size > 0 else 0.01
    return (fair_cap - depth_limit) + 1e-12 >= cfg.min_fair_cap_margin_ticks * tick


def _entry_required_edge(base_edge: float, ask_avg: float, cfg: EdgeConfig) -> float:
    if cfg.low_price_extra_edge_threshold > 0.0 and cfg.low_price_extra_edge > 0.0 and ask_avg < cfg.low_price_extra_edge_threshold:
        return base_edge + cfg.low_price_extra_edge
    return base_edge


def _source_divergent(snapshot: MarketSnapshot, cfg: EdgeConfig) -> bool:
    return (
        cfg.cross_source_max_bps > 0.0
        and snapshot.source_spread_bps is not None
        and snapshot.source_spread_bps > cfg.cross_source_max_bps
    )


def _market_disagreement(snapshot: MarketSnapshot, position: PositionSnapshot, model_prob: float, bid: float, profit_now: float, cfg: EdgeConfig) -> float | None:
    if cfg.market_disagrees_exit_threshold <= 0.0:
        return None
    if cfg.market_disagrees_exit_max_remaining_sec > 0.0 and snapshot.remaining_sec > cfg.market_disagrees_exit_max_remaining_sec:
        return None
    if profit_now > cfg.market_disagrees_exit_max_profit:
        return None
    if cfg.market_disagrees_exit_min_loss > 0.0 and profit_now > -cfg.market_disagrees_exit_min_loss:
        return None
    if cfg.market_disagrees_exit_min_age_sec > 0.0 and snapshot.age_sec - position.entry_time < cfg.market_disagrees_exit_min_age_sec:
        return None
    if position.entry_model_prob <= 0.0 or model_prob <= 0.0:
        return None
    entry_ratio = position.entry_avg_price / position.entry_model_prob
    current_ratio = bid / model_prob
    disagreement = entry_ratio - current_ratio
    return disagreement if disagreement >= cfg.market_disagrees_exit_threshold else None


def _adverse_polymarket_divergence(snapshot: MarketSnapshot, position: PositionSnapshot, cfg: EdgeConfig) -> float | None:
    if cfg.polymarket_divergence_exit_bps <= 0.0:
        return None
    if snapshot.polymarket_divergence_bps is None:
        return None
    if cfg.polymarket_divergence_exit_min_age_sec > 0.0 and snapshot.age_sec - position.entry_time < cfg.polymarket_divergence_exit_min_age_sec:
        return None
    divergence = snapshot.polymarket_divergence_bps
    if position.token_side == "up" and divergence > cfg.polymarket_divergence_exit_bps:
        return divergence
    if position.token_side == "down" and divergence < -cfg.polymarket_divergence_exit_bps:
        return divergence
    return None


def evaluate_entry(snapshot: MarketSnapshot, state: StrategyState, cfg: EdgeConfig) -> StrategyDecision:
    if state.has_position:
        return StrategyDecision(action="skip", reason="already_holding")
    if state.entry_count >= cfg.max_entries_per_market:
        return StrategyDecision(action="skip", reason="max_entries")
    phase = required_edge_for_entry(snapshot, cfg)
    if not phase.allowed:
        reason = "late_entry_disabled" if phase.phase == "late_disabled" else phase.phase
        if reason == "outside_window":
            reason = "outside_entry_time"
        return StrategyDecision(action="skip", reason=reason, phase=phase.phase, required_edge=phase.required_edge)
    if _missing_model_inputs(snapshot):
        return StrategyDecision(action="skip", reason="missing_model_inputs", phase=phase.phase, required_edge=phase.required_edge)
    if _stale_books(snapshot, cfg):
        return StrategyDecision(action="skip", reason="stale_book", phase=phase.phase, required_edge=phase.required_edge)
    if _source_divergent(snapshot, cfg):
        return StrategyDecision(action="skip", reason="source_divergence", phase=phase.phase, required_edge=phase.required_edge)

    probs = _probs(snapshot)
    candidates: list[StrategyDecision] = []
    rejected_low_model_prob = False
    assert phase.required_edge is not None
    attempted_required_edges: list[float] = []
    if snapshot.up_ask_depth_ok and snapshot.up_ask_avg is not None and snapshot.up_ask_limit is not None:
        up_edge = probs.up - snapshot.up_ask_avg
        up_required_edge = _entry_required_edge(phase.required_edge, snapshot.up_ask_avg, cfg)
        attempted_required_edges.append(up_required_edge)
        up_fair_cap = probs.up - up_required_edge
        if up_edge >= up_required_edge:
            if probs.up < cfg.min_entry_model_prob:
                rejected_low_model_prob = True
            elif _entry_depth_ok(snapshot.up_ask_limit, snapshot.up_ask_safety_limit, up_fair_cap, cfg) and _spread_ok(snapshot, "up", cfg, phase):
                candidates.append(StrategyDecision("enter", "edge", "up", model_prob=probs.up, price=snapshot.up_ask_avg, limit_price=up_fair_cap, depth_limit_price=snapshot.up_ask_limit, best_ask=snapshot.up_best_ask, edge=up_edge, up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=up_required_edge))
    if snapshot.down_ask_depth_ok and snapshot.down_ask_avg is not None and snapshot.down_ask_limit is not None:
        down_edge = probs.down - snapshot.down_ask_avg
        down_required_edge = _entry_required_edge(phase.required_edge, snapshot.down_ask_avg, cfg)
        attempted_required_edges.append(down_required_edge)
        down_fair_cap = probs.down - down_required_edge
        if down_edge >= down_required_edge:
            if probs.down < cfg.min_entry_model_prob:
                rejected_low_model_prob = True
            elif _entry_depth_ok(snapshot.down_ask_limit, snapshot.down_ask_safety_limit, down_fair_cap, cfg) and _spread_ok(snapshot, "down", cfg, phase):
                candidates.append(StrategyDecision("enter", "edge", "down", model_prob=probs.down, price=snapshot.down_ask_avg, limit_price=down_fair_cap, depth_limit_price=snapshot.down_ask_limit, best_ask=snapshot.down_best_ask, edge=down_edge, up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=down_required_edge))
    if not candidates:
        effective_required_edge = max(attempted_required_edges) if attempted_required_edges else phase.required_edge
        if rejected_low_model_prob:
            return StrategyDecision(action="skip", reason="model_prob_too_low", up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=effective_required_edge)
        return StrategyDecision(action="skip", reason="edge_too_small", up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=effective_required_edge)
    return max(candidates, key=lambda item: item.edge or 0.0)


def evaluate_exit(snapshot: MarketSnapshot, position: PositionSnapshot, cfg: EdgeConfig, state: StrategyState | None = None) -> StrategyDecision:
    if _missing_model_inputs(snapshot):
        return StrategyDecision(action="exit", reason="risk_exit")

    probs = _probs(snapshot)
    if position.token_side == "up":
        model_prob = probs.up
        bid = snapshot.up_bid_avg
        bid_limit = snapshot.up_bid_limit
        depth_ok = snapshot.up_bid_depth_ok
    else:
        model_prob = probs.down
        bid = snapshot.down_bid_avg
        bid_limit = snapshot.down_bid_limit
        depth_ok = snapshot.down_bid_depth_ok

    if _stale_books(snapshot, cfg):
        if snapshot.remaining_sec > cfg.final_force_exit_remaining_sec:
            return StrategyDecision(action="hold", reason="stale_book_wait", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down)
        if depth_ok and bid is not None and bid_limit is not None:
            profit_now = bid - position.entry_avg_price
            return StrategyDecision(action="exit", reason="final_force_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now)
        return StrategyDecision(action="exit", reason="risk_exit", side=position.token_side, model_prob=model_prob, up_prob=probs.up, down_prob=probs.down)

    if not depth_ok or bid is None or bid_limit is None:
        return StrategyDecision(action="hold", reason="missing_exit_depth", side=position.token_side, model_prob=model_prob, up_prob=probs.up, down_prob=probs.down)
    profit_now = bid - position.entry_avg_price
    prob_delta_3s = state.prob_delta(snapshot.age_sec, model_prob, window_sec=cfg.prob_stagnation_window_sec) if state is not None else None
    prob_stagnant = False if prob_delta_3s is None else prob_delta_3s <= cfg.prob_stagnation_epsilon
    prob_drop_delta = state.prob_delta(snapshot.age_sec, model_prob, window_sec=cfg.prob_drop_exit_window_sec) if state is not None and cfg.prob_drop_exit_window_sec > 0.0 and cfg.prob_drop_exit_threshold > 0.0 else None
    market_disagreement = _market_disagreement(snapshot, position, model_prob, bid, profit_now, cfg)
    polymarket_divergence_bps = _adverse_polymarket_divergence(snapshot, position, cfg)
    if polymarket_divergence_bps is not None:
        return StrategyDecision(action="exit", reason="polymarket_divergence_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=polymarket_divergence_bps)
    if snapshot.remaining_sec <= cfg.final_force_exit_remaining_sec:
        strong_hold = model_prob >= cfg.final_hold_min_prob and bid >= cfg.final_hold_min_bid_avg and bid_limit >= cfg.final_hold_min_bid_limit
        if not strong_hold:
            return StrategyDecision(action="exit", reason="final_force_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=polymarket_divergence_bps)
    if cfg.profit_protection_start_remaining_sec < snapshot.remaining_sec <= cfg.profit_protection_end_remaining_sec and profit_now >= cfg.protection_profit_min:
        return StrategyDecision(action="exit", reason="profit_protection_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=polymarket_divergence_bps)
    if cfg.defensive_take_profit_start_remaining_sec < snapshot.remaining_sec <= cfg.defensive_take_profit_end_remaining_sec and profit_now >= cfg.defensive_profit_min and prob_stagnant:
        return StrategyDecision(action="exit", reason="defensive_take_profit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=polymarket_divergence_bps)
    if prob_drop_delta is not None and prob_drop_delta <= -cfg.prob_drop_exit_threshold and model_prob < position.entry_model_prob:
        return StrategyDecision(action="exit", reason="prob_drop_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=polymarket_divergence_bps)
    if market_disagreement is not None:
        return StrategyDecision(action="exit", reason="market_disagrees_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=polymarket_divergence_bps)
    if model_prob < position.entry_avg_price - cfg.model_decay_buffer:
        return StrategyDecision(action="exit", reason="logic_decay_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=polymarket_divergence_bps)
    if bid > model_prob + cfg.overprice_buffer:
        return StrategyDecision(action="exit", reason="market_overprice_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=polymarket_divergence_bps)
    return StrategyDecision(action="hold", reason="edge_intact", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=polymarket_divergence_bps)
