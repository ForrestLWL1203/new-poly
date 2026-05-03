"""Pure probability-edge entry and exit decisions."""

from __future__ import annotations

from dataclasses import dataclass

from .probability import binary_probabilities
from .state import PositionSnapshot, StrategyState


@dataclass(frozen=True)
class EdgeConfig:
    required_edge: float = 0.05
    model_decay_buffer: float = 0.02
    overprice_buffer: float = 0.02
    entry_start_age_sec: float = 40.0
    entry_end_age_sec: float = 270.0
    final_no_entry_remaining_sec: float = 30.0
    max_entries_per_market: int = 2
    max_book_age_ms: float = 1000.0


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
    up_ask_depth_ok: bool = False
    down_ask_depth_ok: bool = False
    up_bid_depth_ok: bool = False
    down_bid_depth_ok: bool = False
    up_book_age_ms: float | None = None
    down_book_age_ms: float | None = None


@dataclass(frozen=True)
class StrategyDecision:
    action: str
    reason: str
    side: str | None = None
    token_id: str | None = None
    model_prob: float | None = None
    price: float | None = None
    limit_price: float | None = None
    best_ask: float | None = None
    edge: float | None = None
    up_prob: float | None = None
    down_prob: float | None = None


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


def evaluate_entry(snapshot: MarketSnapshot, state: StrategyState, cfg: EdgeConfig) -> StrategyDecision:
    if state.has_position:
        return StrategyDecision(action="skip", reason="already_holding")
    if state.entry_count >= cfg.max_entries_per_market:
        return StrategyDecision(action="skip", reason="max_entries")
    if snapshot.age_sec < cfg.entry_start_age_sec or snapshot.age_sec > cfg.entry_end_age_sec:
        return StrategyDecision(action="skip", reason="outside_entry_time")
    if snapshot.remaining_sec <= cfg.final_no_entry_remaining_sec:
        return StrategyDecision(action="skip", reason="final_no_entry")
    if _missing_model_inputs(snapshot):
        return StrategyDecision(action="skip", reason="missing_model_inputs")
    if _stale_books(snapshot, cfg):
        return StrategyDecision(action="skip", reason="stale_book")

    probs = _probs(snapshot)
    candidates: list[StrategyDecision] = []
    if snapshot.up_ask_depth_ok and snapshot.up_ask_avg is not None and snapshot.up_ask_limit is not None:
        up_edge = probs.up - snapshot.up_ask_avg
        if up_edge >= cfg.required_edge:
            candidates.append(StrategyDecision("enter", "edge", "up", model_prob=probs.up, price=snapshot.up_ask_avg, limit_price=snapshot.up_ask_limit, best_ask=snapshot.up_best_ask, edge=up_edge, up_prob=probs.up, down_prob=probs.down))
    if snapshot.down_ask_depth_ok and snapshot.down_ask_avg is not None and snapshot.down_ask_limit is not None:
        down_edge = probs.down - snapshot.down_ask_avg
        if down_edge >= cfg.required_edge:
            candidates.append(StrategyDecision("enter", "edge", "down", model_prob=probs.down, price=snapshot.down_ask_avg, limit_price=snapshot.down_ask_limit, best_ask=snapshot.down_best_ask, edge=down_edge, up_prob=probs.up, down_prob=probs.down))
    if not candidates:
        return StrategyDecision(action="skip", reason="edge_too_small", up_prob=probs.up, down_prob=probs.down)
    return max(candidates, key=lambda item: item.edge or 0.0)


def evaluate_exit(snapshot: MarketSnapshot, position: PositionSnapshot, cfg: EdgeConfig) -> StrategyDecision:
    if _missing_model_inputs(snapshot):
        return StrategyDecision(action="exit", reason="risk_exit")
    if _stale_books(snapshot, cfg):
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

    if not depth_ok or bid is None or bid_limit is None:
        return StrategyDecision(action="hold", reason="missing_exit_depth", side=position.token_side, model_prob=model_prob, up_prob=probs.up, down_prob=probs.down)
    if model_prob < position.entry_avg_price - cfg.model_decay_buffer:
        return StrategyDecision(action="exit", reason="logic_decay_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down)
    if bid > model_prob + cfg.overprice_buffer:
        return StrategyDecision(action="exit", reason="market_overprice_exit", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down)
    return StrategyDecision(action="hold", reason="edge_intact", side=position.token_side, model_prob=model_prob, price=bid, limit_price=bid_limit, up_prob=probs.up, down_prob=probs.down)
