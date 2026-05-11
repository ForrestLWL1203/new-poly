"""Pure probability-edge entry and exit decisions."""

from __future__ import annotations

from dataclasses import dataclass

from .probability import binary_probabilities
from .state import PositionSnapshot, StrategyState


RISK_REENTRY_COOLDOWN_REASONS = frozenset(
    {
        "logic_decay_exit",
        "polymarket_divergence_exit",
        "reference_adverse_exit",
        "market_disagrees_exit",
        "risk_exit",
    }
)


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
    defensive_take_profit_enabled: bool = True
    defensive_profit_min: float = 0.03
    protection_profit_min: float = 0.01
    profit_protection_start_remaining_sec: float = 15.0
    profit_protection_end_remaining_sec: float = 30.0
    defensive_take_profit_start_remaining_sec: float = 30.0
    defensive_take_profit_end_remaining_sec: float = 60.0
    final_force_exit_remaining_sec: float = 30.0
    final_profit_hold_min_profit_ratio: float = 0.10
    final_model_hold_min_prob: float = 0.0
    final_hold_min_prob: float = 0.98
    final_hold_min_bid_avg: float = 0.97
    final_hold_min_bid_limit: float = 0.95
    hold_to_settlement_enabled: bool = False
    hold_to_settlement_min_profit_ratio: float = 2.0
    hold_to_settlement_min_model_prob: float = 0.90
    hold_to_settlement_min_bid_avg: float = 0.80
    hold_to_settlement_min_bid_limit: float = 0.75
    prob_stagnation_window_sec: float = 3.0
    prob_stagnation_epsilon: float = 0.002
    prob_drop_exit_window_sec: float = 0.0
    prob_drop_exit_threshold: float = 0.0
    min_fair_cap_margin_ticks: float = 0.0
    entry_tick_size: float = 0.01
    min_entry_model_prob: float = 0.0
    low_price_extra_edge_threshold: float = 0.0
    low_price_extra_edge: float = 0.0
    weak_sk_entry_filter_enabled: bool = False
    weak_sk_entry_min_ask: float = 0.35
    weak_sk_entry_min_abs_sk_bps: float = 2.0
    buy_cap_relax_enabled: bool = False
    buy_low_price_relax_max_ask: float = 0.25
    buy_low_price_relax_min_prob: float = 0.40
    buy_low_price_relax_retained_edge: float = 0.08
    buy_low_price_relax_max_extra_ticks: float = 8.0
    buy_mid_price_relax_max_ask: float = 0.65
    buy_mid_price_relax_min_prob: float = 0.60
    buy_mid_price_relax_retained_edge: float = 0.06
    buy_mid_price_relax_max_extra_ticks: float = 8.0
    buy_mid_strong_relax_min_prob: float = 0.75
    buy_mid_strong_relax_retained_edge: float = 0.05
    buy_mid_strong_relax_max_extra_ticks: float = 10.0
    buy_high_price_relax_min_ask: float = 0.65
    buy_high_price_relax_min_prob: float = 0.95
    buy_high_price_relax_retained_edge: float = 0.08
    buy_high_price_relax_max_extra_ticks: float = 4.0
    cross_source_max_bps: float = 0.0
    market_disagrees_exit_threshold: float = 0.0
    low_price_market_disagrees_entry_threshold: float = 0.0
    low_price_market_disagrees_exit_threshold: float = 0.0
    market_disagrees_exit_max_remaining_sec: float = 0.0
    market_disagrees_exit_min_loss: float = 0.0
    market_disagrees_exit_min_age_sec: float = 0.0
    market_disagrees_exit_max_profit: float = 0.01
    market_disagrees_exit_min_model_drop: float = 0.0
    polymarket_divergence_exit_bps: float = 3.0
    polymarket_divergence_exit_min_age_sec: float = 3.0
    entry_reference_confirm_bps: float = 0.0
    exit_reference_adverse_bps: float = 0.0
    logic_decay_reentry_cooldown_sec: float = 30.0


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


def _missing_model_inputs(snapshot: MarketSnapshot) -> bool:
    return snapshot.s_price is None or snapshot.k_price is None or snapshot.sigma_eff is None


def _exit_book_age_ms(snapshot: MarketSnapshot, side: str) -> float | None:
    if side == "up":
        return snapshot.up_bid_age_ms if snapshot.up_bid_age_ms is not None else snapshot.up_book_age_ms
    return snapshot.down_bid_age_ms if snapshot.down_bid_age_ms is not None else snapshot.down_book_age_ms


def _stale_exit_book(snapshot: MarketSnapshot, side: str, cfg: EdgeConfig) -> bool:
    age = _exit_book_age_ms(snapshot, side)
    return age is None or age > cfg.max_book_age_ms


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


def _entry_cap_ok(price: float, fair_cap: float, cfg: EdgeConfig) -> bool:
    if price > fair_cap:
        return False
    tick = cfg.entry_tick_size if cfg.entry_tick_size > 0 else 0.01
    return (fair_cap - price) + 1e-12 >= cfg.min_fair_cap_margin_ticks * tick


def _entry_required_edge(base_edge: float, ask_avg: float, cfg: EdgeConfig) -> float:
    if cfg.low_price_extra_edge_threshold > 0.0 and cfg.low_price_extra_edge > 0.0 and ask_avg < cfg.low_price_extra_edge_threshold:
        return base_edge + cfg.low_price_extra_edge
    return base_edge


def _weak_sk_entry_filter(snapshot: MarketSnapshot, ask: float, cfg: EdgeConfig) -> bool:
    if not cfg.weak_sk_entry_filter_enabled:
        return False
    if cfg.weak_sk_entry_min_abs_sk_bps <= 0.0:
        return False
    if ask <= cfg.weak_sk_entry_min_ask:
        return False
    if snapshot.s_price is None or snapshot.k_price is None or snapshot.k_price <= 0.0:
        return False
    sk_bps = abs(snapshot.s_price - snapshot.k_price) / snapshot.k_price * 10000.0
    return sk_bps < cfg.weak_sk_entry_min_abs_sk_bps


def _polymarket_reference_distance_bps(snapshot: MarketSnapshot, side: str) -> float | None:
    if snapshot.polymarket_price is None or snapshot.k_price is None or snapshot.k_price <= 0.0:
        return None
    if side == "up":
        return (snapshot.polymarket_price - snapshot.k_price) / snapshot.k_price * 10000.0
    return (snapshot.k_price - snapshot.polymarket_price) / snapshot.k_price * 10000.0


def _raw_binance_distance_bps(snapshot: MarketSnapshot) -> float | None:
    if snapshot.s_price is None or snapshot.k_price is None or snapshot.k_price <= 0.0:
        return None
    return (snapshot.s_price - snapshot.k_price) / snapshot.k_price * 10000.0


def _raw_reference_distance_bps(snapshot: MarketSnapshot) -> float | None:
    if snapshot.polymarket_price is None or snapshot.k_price is None or snapshot.k_price <= 0.0:
        return None
    return (snapshot.polymarket_price - snapshot.k_price) / snapshot.k_price * 10000.0


def _side_adjusted(value: float, side: str) -> float:
    return value if side == "up" else -value


def _favorable_gap_bps(snapshot: MarketSnapshot, side: str) -> float | None:
    if snapshot.polymarket_divergence_bps is None:
        return None
    return snapshot.polymarket_divergence_bps if side == "up" else -snapshot.polymarket_divergence_bps


def _gap_direction_supported(snapshot: MarketSnapshot, side: str) -> bool:
    favorable_gap = _favorable_gap_bps(snapshot, side)
    return favorable_gap is None or favorable_gap >= 0.0


def _entry_reference_confirmed(snapshot: MarketSnapshot, side: str, cfg: EdgeConfig) -> bool:
    if cfg.entry_reference_confirm_bps <= 0.0:
        return True
    distance_bps = _polymarket_reference_distance_bps(snapshot, side)
    return distance_bps is not None and distance_bps >= cfg.entry_reference_confirm_bps


def _gap_compression_from_entry_bps(snapshot: MarketSnapshot, position: PositionSnapshot) -> float | None:
    if position.entry_favorable_gap_bps is None:
        return None
    current = _favorable_gap_bps(snapshot, position.token_side)
    if current is None:
        return None
    return position.entry_favorable_gap_bps - current


def _adjusted_probability_shadow(snapshot: MarketSnapshot, side: str, raw_prob: float, state: StrategyState | None) -> tuple[float | None, float | None, str | None]:
    reference_distance = _polymarket_reference_distance_bps(snapshot, side)
    if reference_distance is None:
        return None, None, None

    adjustment = 0.0
    if reference_distance >= 2.0:
        adjustment += 0.04
    elif reference_distance >= 1.0:
        adjustment += 0.02
    elif reference_distance < 0.0:
        adjustment -= 0.08
    else:
        adjustment -= 0.02

    lead_follow_state: str | None = None
    baseline = state.reference_baseline if state is not None else None
    current_binance = _raw_binance_distance_bps(snapshot)
    current_reference = _raw_reference_distance_bps(snapshot)
    current_gap = snapshot.polymarket_divergence_bps
    if baseline is not None and current_binance is not None and current_reference is not None and current_gap is not None:
        delta_binance = _side_adjusted(current_binance - baseline.binance_distance_bps, side)
        delta_reference = _side_adjusted(current_reference - baseline.reference_distance_bps, side)
        delta_gap = _side_adjusted(current_gap - baseline.gap_bps, side)
        if delta_binance > 0.0 and delta_reference > 0.0:
            lead_follow_state = "both_confirming"
            adjustment += 0.03
        elif delta_binance > 0.0 and delta_reference <= 0.0:
            lead_follow_state = "binance_lead_not_followed"
        elif delta_reference > 0.0 and delta_binance <= 0.0:
            lead_follow_state = "reference_catching_up"
            adjustment += 0.04
        else:
            lead_follow_state = "both_weakening"
            adjustment -= 0.04
        if delta_gap > 2.0 and delta_reference <= 0.0:
            adjustment -= 0.02
            lead_follow_state = "gap_expanding_without_reference_follow"

    adjusted = max(0.0, min(1.0, raw_prob + adjustment))
    return adjusted, adjustment, lead_follow_state


def _relaxed_entry_fair_cap(model_prob: float, best_ask: float, required_edge: float, cfg: EdgeConfig) -> float:
    base_cap = model_prob - required_edge
    if not cfg.buy_cap_relax_enabled:
        return base_cap

    retained_edge: float | None = None
    max_extra_ticks: float | None = None
    if best_ask <= cfg.buy_low_price_relax_max_ask and model_prob >= cfg.buy_low_price_relax_min_prob:
        retained_edge = cfg.buy_low_price_relax_retained_edge
        max_extra_ticks = cfg.buy_low_price_relax_max_extra_ticks
    elif best_ask <= cfg.buy_mid_price_relax_max_ask and model_prob >= cfg.buy_mid_strong_relax_min_prob:
        retained_edge = cfg.buy_mid_strong_relax_retained_edge
        max_extra_ticks = cfg.buy_mid_strong_relax_max_extra_ticks
    elif best_ask <= cfg.buy_mid_price_relax_max_ask and model_prob >= cfg.buy_mid_price_relax_min_prob:
        retained_edge = cfg.buy_mid_price_relax_retained_edge
        max_extra_ticks = cfg.buy_mid_price_relax_max_extra_ticks
    elif best_ask > cfg.buy_high_price_relax_min_ask and model_prob >= cfg.buy_high_price_relax_min_prob:
        retained_edge = cfg.buy_high_price_relax_retained_edge
        max_extra_ticks = cfg.buy_high_price_relax_max_extra_ticks

    if retained_edge is None or max_extra_ticks is None:
        return base_cap
    tick = cfg.entry_tick_size if cfg.entry_tick_size > 0 else 0.01
    relaxed_cap = min(model_prob - retained_edge, best_ask + max(0.0, max_extra_ticks) * tick)
    return max(base_cap, relaxed_cap)


def _source_divergent(snapshot: MarketSnapshot, cfg: EdgeConfig) -> bool:
    return (
        cfg.cross_source_max_bps > 0.0
        and snapshot.source_spread_bps is not None
        and snapshot.source_spread_bps > cfg.cross_source_max_bps
    )


def _risk_exit_cooldown_reason(snapshot: MarketSnapshot, state: StrategyState, side: str, cfg: EdgeConfig) -> str | None:
    if not (
        cfg.logic_decay_reentry_cooldown_sec > 0.0
        and state.last_exit_reason in RISK_REENTRY_COOLDOWN_REASONS
        and state.last_exit_side == side
        and state.last_exit_age_sec is not None
        and snapshot.age_sec - state.last_exit_age_sec < cfg.logic_decay_reentry_cooldown_sec
    ):
        return None
    return "logic_decay_cooldown" if state.last_exit_reason == "logic_decay_exit" else "risk_exit_cooldown"


def _market_disagreement(snapshot: MarketSnapshot, position: PositionSnapshot, model_prob: float, bid: float, profit_now: float, cfg: EdgeConfig) -> float | None:
    threshold = cfg.market_disagrees_exit_threshold
    if (
        cfg.low_price_market_disagrees_entry_threshold > 0.0
        and cfg.low_price_market_disagrees_exit_threshold > 0.0
        and position.entry_avg_price <= cfg.low_price_market_disagrees_entry_threshold
    ):
        threshold = cfg.low_price_market_disagrees_exit_threshold
    if threshold <= 0.0:
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
    if model_prob >= position.entry_model_prob:
        return None
    if cfg.market_disagrees_exit_min_model_drop > 0.0 and position.entry_model_prob - model_prob < cfg.market_disagrees_exit_min_model_drop:
        return None
    if position.entry_avg_price <= 0.0:
        return None
    price_ratio = bid / position.entry_avg_price
    return price_ratio if price_ratio <= threshold else None


def _reference_adverse_exit(snapshot: MarketSnapshot, position: PositionSnapshot, cfg: EdgeConfig) -> bool:
    if cfg.exit_reference_adverse_bps <= 0.0:
        return False
    if cfg.polymarket_divergence_exit_min_age_sec > 0.0 and snapshot.age_sec - position.entry_time < cfg.polymarket_divergence_exit_min_age_sec:
        return False
    adverse_distance_bps = _polymarket_reference_distance_bps(
        snapshot,
        "down" if position.token_side == "up" else "up",
    )
    return adverse_distance_bps is not None and adverse_distance_bps >= cfg.exit_reference_adverse_bps


def _hold_to_settlement_candidate(
    position: PositionSnapshot,
    *,
    model_prob: float,
    bid: float,
    bid_limit: float,
    profit_now: float,
    cfg: EdgeConfig,
) -> bool:
    if not cfg.hold_to_settlement_enabled or position.entry_avg_price <= 0:
        return False
    profit_ratio = profit_now / position.entry_avg_price
    return (
        profit_ratio >= cfg.hold_to_settlement_min_profit_ratio
        and model_prob >= cfg.hold_to_settlement_min_model_prob
        and bid >= cfg.hold_to_settlement_min_bid_avg
        and bid_limit >= cfg.hold_to_settlement_min_bid_limit
    )


def _final_profit_hold_candidate(position: PositionSnapshot, profit_now: float, cfg: EdgeConfig) -> bool:
    if position.entry_avg_price <= 0 or cfg.final_profit_hold_min_profit_ratio <= 0:
        return False
    return profit_now / position.entry_avg_price >= cfg.final_profit_hold_min_profit_ratio


def _final_model_hold_candidate(model_prob: float, cfg: EdgeConfig) -> bool:
    return cfg.final_model_hold_min_prob > 0.0 and model_prob >= cfg.final_model_hold_min_prob


def evaluate_entry(snapshot: MarketSnapshot, state: StrategyState, cfg: EdgeConfig) -> StrategyDecision:
    if state.has_position:
        return StrategyDecision(action="skip", reason="already_holding")
    if state.loss_pause_remaining_windows > 0:
        return StrategyDecision(action="skip", reason="loss_pause")
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
    if _source_divergent(snapshot, cfg):
        return StrategyDecision(action="skip", reason="source_divergence", phase=phase.phase, required_edge=phase.required_edge)

    probs = _probs(snapshot)
    up_shadow, up_adjustment, up_lead_follow = _adjusted_probability_shadow(snapshot, "up", probs.up, state)
    down_shadow, down_adjustment, down_lead_follow = _adjusted_probability_shadow(snapshot, "down", probs.down, state)
    if probs.up >= probs.down:
        common_shadow = {
            "adjusted_up_prob_shadow": up_shadow,
            "adjusted_down_prob_shadow": down_shadow,
            "adjusted_model_prob_shadow": up_shadow,
            "prob_shadow_adjustment": up_adjustment,
            "lead_follow_state": up_lead_follow,
        }
    else:
        common_shadow = {
            "adjusted_up_prob_shadow": up_shadow,
            "adjusted_down_prob_shadow": down_shadow,
            "adjusted_model_prob_shadow": down_shadow,
            "prob_shadow_adjustment": down_adjustment,
            "lead_follow_state": down_lead_follow,
        }
    candidates: list[StrategyDecision] = []
    rejected_low_model_prob = False
    assert phase.required_edge is not None
    attempted_required_edges: list[float] = []
    rejected_cooldown_reason: str | None = None
    rejected_weak_sk_distance = False
    rejected_reference_confirmation = False
    rejected_gap_direction = False
    if snapshot.up_best_ask is not None:
        up_edge = probs.up - snapshot.up_best_ask
        up_required_edge = _entry_required_edge(phase.required_edge, snapshot.up_best_ask, cfg)
        attempted_required_edges.append(up_required_edge)
        up_fair_cap = _relaxed_entry_fair_cap(probs.up, snapshot.up_best_ask, up_required_edge, cfg)
        if up_edge >= up_required_edge:
            cooldown_reason = _risk_exit_cooldown_reason(snapshot, state, "up", cfg)
            if cooldown_reason is not None:
                rejected_cooldown_reason = cooldown_reason
            elif probs.up < cfg.min_entry_model_prob:
                rejected_low_model_prob = True
            elif not _entry_reference_confirmed(snapshot, "up", cfg):
                rejected_reference_confirmation = True
            elif not _gap_direction_supported(snapshot, "up"):
                rejected_gap_direction = True
            elif _entry_cap_ok(snapshot.up_best_ask, up_fair_cap, cfg) and _spread_ok(snapshot, "up", cfg, phase):
                if _weak_sk_entry_filter(snapshot, snapshot.up_best_ask, cfg):
                    rejected_weak_sk_distance = True
                else:
                    candidates.append(StrategyDecision("enter", "edge", "up", model_prob=probs.up, price=snapshot.up_best_ask, limit_price=up_fair_cap, depth_limit_price=snapshot.up_best_ask, best_ask=snapshot.up_best_ask, edge=up_edge, up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=up_required_edge, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=_favorable_gap_bps(snapshot, "up"), entry_reference_distance_bps=_polymarket_reference_distance_bps(snapshot, "up"), adjusted_up_prob_shadow=up_shadow, adjusted_down_prob_shadow=down_shadow, adjusted_model_prob_shadow=up_shadow, prob_shadow_adjustment=up_adjustment, lead_follow_state=up_lead_follow))
    if snapshot.down_best_ask is not None:
        down_edge = probs.down - snapshot.down_best_ask
        down_required_edge = _entry_required_edge(phase.required_edge, snapshot.down_best_ask, cfg)
        attempted_required_edges.append(down_required_edge)
        down_fair_cap = _relaxed_entry_fair_cap(probs.down, snapshot.down_best_ask, down_required_edge, cfg)
        if down_edge >= down_required_edge:
            cooldown_reason = _risk_exit_cooldown_reason(snapshot, state, "down", cfg)
            if cooldown_reason is not None:
                rejected_cooldown_reason = cooldown_reason
            elif probs.down < cfg.min_entry_model_prob:
                rejected_low_model_prob = True
            elif not _entry_reference_confirmed(snapshot, "down", cfg):
                rejected_reference_confirmation = True
            elif not _gap_direction_supported(snapshot, "down"):
                rejected_gap_direction = True
            elif _entry_cap_ok(snapshot.down_best_ask, down_fair_cap, cfg) and _spread_ok(snapshot, "down", cfg, phase):
                if _weak_sk_entry_filter(snapshot, snapshot.down_best_ask, cfg):
                    rejected_weak_sk_distance = True
                else:
                    candidates.append(StrategyDecision("enter", "edge", "down", model_prob=probs.down, price=snapshot.down_best_ask, limit_price=down_fair_cap, depth_limit_price=snapshot.down_best_ask, best_ask=snapshot.down_best_ask, edge=down_edge, up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=down_required_edge, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=_favorable_gap_bps(snapshot, "down"), entry_reference_distance_bps=_polymarket_reference_distance_bps(snapshot, "down"), adjusted_up_prob_shadow=up_shadow, adjusted_down_prob_shadow=down_shadow, adjusted_model_prob_shadow=down_shadow, prob_shadow_adjustment=down_adjustment, lead_follow_state=down_lead_follow))
    if not candidates:
        effective_required_edge = max(attempted_required_edges) if attempted_required_edges else phase.required_edge
        if rejected_cooldown_reason is not None:
            return StrategyDecision(action="skip", reason=rejected_cooldown_reason, up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=effective_required_edge, **common_shadow)
        if rejected_low_model_prob:
            return StrategyDecision(action="skip", reason="model_prob_too_low", up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=effective_required_edge, **common_shadow)
        if rejected_reference_confirmation:
            return StrategyDecision(action="skip", reason="reference_not_confirmed", up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=effective_required_edge, **common_shadow)
        if rejected_gap_direction:
            return StrategyDecision(action="skip", reason="gap_direction_conflict", up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=effective_required_edge, **common_shadow)
        if rejected_weak_sk_distance:
            return StrategyDecision(action="skip", reason="weak_sk_distance", up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=effective_required_edge, **common_shadow)
        return StrategyDecision(action="skip", reason="edge_too_small", up_prob=probs.up, down_prob=probs.down, phase=phase.phase, required_edge=effective_required_edge, **common_shadow)
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

    if _stale_exit_book(snapshot, position.token_side, cfg):
        if snapshot.remaining_sec > cfg.final_force_exit_remaining_sec:
            return StrategyDecision(action="hold", reason="stale_book_wait", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down)
        if depth_ok and bid is not None and bid_limit is not None:
            profit_now = bid - position.entry_avg_price
            if _final_profit_hold_candidate(position, profit_now, cfg):
                return StrategyDecision(action="hold", reason="final_profit_hold", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now)
            return StrategyDecision(action="exit", reason="final_force_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now)
        return StrategyDecision(action="exit", reason="risk_exit", side=position.token_side, model_prob=model_prob, up_prob=probs.up, down_prob=probs.down)

    if not depth_ok or bid is None or bid_limit is None:
        return StrategyDecision(action="hold", reason="missing_exit_depth", side=position.token_side, model_prob=model_prob, up_prob=probs.up, down_prob=probs.down)
    profit_now = bid - position.entry_avg_price
    prob_delta_3s = state.prob_delta(snapshot.age_sec, model_prob, window_sec=cfg.prob_stagnation_window_sec) if state is not None else None
    prob_stagnant = False if prob_delta_3s is None else prob_delta_3s <= cfg.prob_stagnation_epsilon
    prob_drop_delta = state.prob_delta(snapshot.age_sec, model_prob, window_sec=cfg.prob_drop_exit_window_sec) if state is not None and cfg.prob_drop_exit_window_sec > 0.0 and cfg.prob_drop_exit_threshold > 0.0 else None
    market_disagreement = _market_disagreement(snapshot, position, model_prob, bid, profit_now, cfg)
    reference_adverse = _reference_adverse_exit(snapshot, position, cfg)
    gap_compression = _gap_compression_from_entry_bps(snapshot, position)
    current_favorable_gap = _favorable_gap_bps(snapshot, position.token_side)
    gap_risk_flag = "gap_compressed_from_entry" if gap_compression is not None and gap_compression > 0.0 else None
    hold_to_settlement = _hold_to_settlement_candidate(position, model_prob=model_prob, bid=bid, bid_limit=bid_limit, profit_now=profit_now, cfg=cfg)
    if reference_adverse:
        return StrategyDecision(action="exit", reason="reference_adverse_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
    if snapshot.remaining_sec <= cfg.final_force_exit_remaining_sec:
        strong_hold = model_prob >= cfg.final_hold_min_prob and bid >= cfg.final_hold_min_bid_avg and bid_limit >= cfg.final_hold_min_bid_limit
        final_profit_hold = _final_profit_hold_candidate(position, profit_now, cfg)
        final_model_hold = _final_model_hold_candidate(model_prob, cfg)
        if not strong_hold and not hold_to_settlement and not final_profit_hold and not final_model_hold:
            return StrategyDecision(action="exit", reason="final_force_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
        if final_profit_hold and not hold_to_settlement:
            return StrategyDecision(action="hold", reason="final_profit_hold", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
        if final_model_hold and not hold_to_settlement:
            return StrategyDecision(action="hold", reason="final_model_hold", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
    if not hold_to_settlement and cfg.profit_protection_start_remaining_sec < snapshot.remaining_sec <= cfg.profit_protection_end_remaining_sec and profit_now >= cfg.protection_profit_min:
        return StrategyDecision(action="exit", reason="profit_protection_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
    if cfg.defensive_take_profit_enabled and not hold_to_settlement and cfg.defensive_take_profit_start_remaining_sec < snapshot.remaining_sec <= cfg.defensive_take_profit_end_remaining_sec and profit_now >= cfg.defensive_profit_min and prob_stagnant:
        return StrategyDecision(action="exit", reason="defensive_take_profit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
    if prob_drop_delta is not None and prob_drop_delta <= -cfg.prob_drop_exit_threshold and model_prob < position.entry_model_prob:
        return StrategyDecision(action="exit", reason="prob_drop_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
    if market_disagreement is not None:
        return StrategyDecision(action="exit", reason="market_disagrees_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
    if model_prob < position.entry_avg_price - cfg.model_decay_buffer:
        return StrategyDecision(action="exit", reason="logic_decay_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
    if hold_to_settlement:
        return StrategyDecision(action="hold", reason="hold_to_settlement", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
    if bid > model_prob + cfg.overprice_buffer:
        return StrategyDecision(action="exit", reason="market_overprice_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
    return StrategyDecision(action="hold", reason="edge_intact", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down, profit_now=profit_now, prob_stagnant=prob_stagnant, prob_delta_3s=prob_delta_3s, prob_drop_delta=prob_drop_delta, market_disagreement=market_disagreement, polymarket_divergence_bps=snapshot.polymarket_divergence_bps, favorable_gap_bps=current_favorable_gap, gap_compression_from_entry_bps=gap_compression, gap_risk_flag=gap_risk_flag)
