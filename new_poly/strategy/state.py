"""State containers for the probability-edge strategy."""

from __future__ import annotations

from dataclasses import dataclass


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
    last_model_prob: float | None = None
    last_executable_bid: float | None = None
    exit_status: str = "open"


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

    @property
    def has_position(self) -> bool:
        return self.open_position is not None and self.open_position.exit_status == "open"

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

    def record_entry(self, position: PositionSnapshot) -> None:
        self.open_position = position
        self.entry_count += 1
        self.prob_history = []

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

    def record_settlement(self, winning_side: str) -> float:
        if self.open_position is None:
            return 0.0
        settlement_value = 1.0 if self.open_position.token_side == winning_side else 0.0
        pnl = (settlement_value - self.open_position.entry_avg_price) * self.open_position.filled_shares
        self.realized_pnl += pnl
        self.peak_pnl = max(self.peak_pnl, self.realized_pnl)
        self.open_position.exit_status = "settled"
        self.open_position = None
        self.last_exit_reason = "settled"
        self.last_exit_side = None
        self.last_exit_age_sec = None
        self.prob_history = []
        return pnl

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
