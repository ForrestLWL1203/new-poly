"""State containers for the probability-edge strategy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ReferenceBaseline:
    market_slug: str
    age_sec: float
    binance_distance_bps: float
    reference_distance_bps: float
    gap_bps: float


@dataclass
class PositionSnapshot:
    market_slug: str
    token_side: str
    token_id: str
    entry_time: float
    entry_avg_price: float
    filled_shares: float
    entry_model_prob: float
    entry_edge: float
    entry_polymarket_divergence_bps: float | None = None
    entry_favorable_gap_bps: float | None = None
    entry_reference_distance_bps: float | None = None
    last_model_prob: float | None = None
    last_executable_bid: float | None = None
    exit_status: str = "open"


@dataclass
class UnknownEntryOrder:
    market_slug: str
    token_side: str
    token_id: str
    amount_usd: float
    entry_time: float
    entry_avg_price: float
    entry_model_prob: float
    entry_edge: float
    entry_polymarket_divergence_bps: float | None = None
    entry_favorable_gap_bps: float | None = None
    entry_reference_distance_bps: float | None = None
    created_at_epoch_ms: int | None = None
    signal_price: float | None = None
    limit_price: float | None = None
    best_ask: float | None = None
    depth_limit_price: float | None = None
    safety_checked: bool = False


@dataclass
class StrategyState:
    current_market_slug: str | None = None
    open_position: PositionSnapshot | None = None
    entry_count: int = 0
    realized_pnl: float = 0.0
    peak_pnl: float = 0.0
    last_exit_reason: str | None = None
    last_exit_side: str | None = None
    last_exit_age_sec: float | None = None
    prob_history: list[tuple[float, float]] | None = None
    consecutive_losses: int = 0
    loss_pause_remaining_windows: int = 0
    loss_pause_started_market_slug: str | None = None
    fatal_stop_reason: str | None = None
    pending_execution: str | None = None
    pending_execution_market_slug: str | None = None
    pending_execution_task: object | None = None
    unresolved_unknown_entry: UnknownEntryOrder | None = None
    reference_baseline: ReferenceBaseline | None = None

    @property
    def has_position(self) -> bool:
        return self.open_position is not None and self.open_position.exit_status == "open"

    @property
    def has_pending_execution(self) -> bool:
        return self.pending_execution is not None

    @property
    def drawdown(self) -> float:
        return self.realized_pnl - self.peak_pnl

    def reset_for_market(self, market_slug: str) -> None:
        self.current_market_slug = market_slug
        self.open_position = None
        self.entry_count = 0
        self.last_exit_reason = None
        self.last_exit_side = None
        self.last_exit_age_sec = None
        self.prob_history = []
        self.pending_execution = None
        self.pending_execution_market_slug = None
        self.pending_execution_task = None
        self.unresolved_unknown_entry = None
        self.reference_baseline = None

    def record_reference_baseline(self, snapshot: Any) -> None:
        if self.reference_baseline is not None:
            return
        s_price = getattr(snapshot, "s_price", None)
        k_price = getattr(snapshot, "k_price", None)
        polymarket_price = getattr(snapshot, "polymarket_price", None)
        if s_price is None or k_price is None or polymarket_price is None or k_price <= 0:
            return
        self.reference_baseline = ReferenceBaseline(
            market_slug=str(getattr(snapshot, "market_slug", self.current_market_slug or "")),
            age_sec=float(getattr(snapshot, "age_sec", 0.0)),
            binance_distance_bps=(float(s_price) - float(k_price)) / float(k_price) * 10000.0,
            reference_distance_bps=(float(polymarket_price) - float(k_price)) / float(k_price) * 10000.0,
            gap_bps=(float(s_price) - float(polymarket_price)) / float(k_price) * 10000.0,
        )

    def record_entry(self, position: PositionSnapshot) -> None:
        self.open_position = position
        self.entry_count += 1
        self.prob_history = []

    def mark_pending_execution(self, kind: str, task: object | None = None) -> None:
        self.pending_execution = kind
        self.pending_execution_market_slug = self.current_market_slug
        self.pending_execution_task = task

    def clear_pending_execution(self) -> None:
        self.pending_execution = None
        self.pending_execution_market_slug = None
        self.pending_execution_task = None

    def record_unresolved_unknown_entry(self, order: UnknownEntryOrder) -> None:
        self.unresolved_unknown_entry = order

    def clear_unresolved_unknown_entry(self) -> None:
        self.unresolved_unknown_entry = None

    def record_partial_exit(self, exit_price: float, shares: float, reason: str, exit_age_sec: float | None = None) -> tuple[float, bool]:
        if self.open_position is None:
            return 0.0, True
        exit_side = self.open_position.token_side
        exit_shares = min(max(0.0, shares), self.open_position.filled_shares)
        pnl = (exit_price - self.open_position.entry_avg_price) * exit_shares
        self.realized_pnl += pnl
        self.peak_pnl = max(self.peak_pnl, self.realized_pnl)
        self.open_position.filled_shares -= exit_shares
        closed = self.open_position.filled_shares <= 1e-9
        if closed:
            self.open_position.exit_status = reason
            self.open_position = None
            self.prob_history = []
        self.last_exit_reason = reason
        self.last_exit_side = exit_side
        self.last_exit_age_sec = exit_age_sec
        return pnl, closed

    def record_exit(self, exit_price: float, reason: str, exit_age_sec: float | None = None) -> float:
        if self.open_position is None:
            return 0.0
        pnl, _closed = self.record_partial_exit(exit_price, self.open_position.filled_shares, reason, exit_age_sec)
        return pnl

    def detach_open_position_for_settlement(self) -> PositionSnapshot | None:
        if self.open_position is None:
            return None
        position = self.open_position
        position.exit_status = "pending_settlement"
        self.open_position = None
        self.prob_history = []
        return position

    def record_position_settlement(self, position: PositionSnapshot, winning_side: str) -> float:
        settlement_value = 1.0 if position.token_side == winning_side else 0.0
        pnl = (settlement_value - position.entry_avg_price) * position.filled_shares
        self.realized_pnl += pnl
        self.peak_pnl = max(self.peak_pnl, self.realized_pnl)
        position.exit_status = "settled"
        self.last_exit_reason = "settled"
        self.last_exit_side = position.token_side
        self.last_exit_age_sec = None
        self.prob_history = []
        return pnl

    def record_position_unsettled(self, position: PositionSnapshot, reason: str) -> float:
        position.exit_status = reason
        self.last_exit_reason = reason
        self.last_exit_side = position.token_side
        self.last_exit_age_sec = None
        self.prob_history = []
        return 0.0

    def record_settlement(self, winning_side: str) -> float:
        if self.open_position is None:
            return 0.0
        position = self.open_position
        self.open_position = None
        return self.record_position_settlement(position, winning_side)

    def record_model_prob(self, age_sec: float, model_prob: float, *, retention_sec: float = 5.0) -> None:
        if self.prob_history is None:
            self.prob_history = []
        self.prob_history.append((age_sec, model_prob))
        cutoff = age_sec - retention_sec
        self.prob_history = [(ts, prob) for ts, prob in self.prob_history if ts >= cutoff]

    def prob_delta(self, age_sec: float, model_prob: float, *, window_sec: float) -> float | None:
        if not self.prob_history:
            return None
        cutoff = age_sec - window_sec
        candidates = [(ts, prob) for ts, prob in self.prob_history if ts <= cutoff]
        if not candidates:
            return None
        _ts, old_prob = max(candidates, key=lambda item: item[0])
        return model_prob - old_prob

    def apply_closed_trade_risk(self, pnl: float, *, loss_limit: int, pause_windows: int) -> dict[str, object] | None:
        if loss_limit <= 0:
            return None
        if pnl > 0:
            self.consecutive_losses = 0
            return {"event": "loss_streak_updated", "consecutive_losses": 0, "pnl": pnl}
        if pnl == 0:
            return None
        self.consecutive_losses += 1
        if self.consecutive_losses >= loss_limit and pause_windows > 0:
            self.loss_pause_remaining_windows = max(self.loss_pause_remaining_windows, pause_windows)
            self.loss_pause_started_market_slug = self.current_market_slug
            return {
                "event": "loss_pause_started",
                "consecutive_losses": self.consecutive_losses,
                "loss_limit": loss_limit,
                "pause_windows": pause_windows,
                "pause_remaining_windows": self.loss_pause_remaining_windows,
                "pnl": pnl,
            }
        return {
            "event": "loss_streak_updated",
            "consecutive_losses": self.consecutive_losses,
            "loss_limit": loss_limit,
            "pnl": pnl,
        }

    def advance_loss_pause_after_window(self, market_slug: str) -> dict[str, object] | None:
        if self.loss_pause_remaining_windows <= 0:
            return None
        if self.loss_pause_started_market_slug == market_slug:
            return None
        self.loss_pause_remaining_windows = max(0, self.loss_pause_remaining_windows - 1)
        event = {
            "event": "loss_pause_window",
            "market_slug": market_slug,
            "pause_remaining_windows": self.loss_pause_remaining_windows,
            "consecutive_losses": self.consecutive_losses,
        }
        if self.loss_pause_remaining_windows == 0:
            self.consecutive_losses = 0
            self.loss_pause_started_market_slug = None
            event["pause_completed"] = True
        return event
