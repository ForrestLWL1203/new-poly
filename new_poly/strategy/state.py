"""State containers for the probability-edge strategy."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


@dataclass
class ReferenceBaseline:
    market_slug: str
    age_sec: float
    binance_distance_bps: float
    reference_distance_bps: float
    gap_bps: float


@dataclass
class WindowOutcome:
    market_slug: str
    winning_side: str
    epoch: int | None = None


def _market_epoch(market_slug: str) -> int | None:
    match = re.search(r"(\d+)$", market_slug)
    return int(match.group(1)) if match else None


@dataclass
class DirectionState:
    market_slug: str
    observe_start_age_sec: float
    first_observed_age_sec: float | None = None
    last_observed_age_sec: float | None = None
    current_side: str | None = None
    dominant_side: str | None = None
    last_cross_age_sec: float | None = None
    cross_count_total: int = 0
    cross_ages: list[float] | None = None
    up_support_integral: float = 0.0
    down_support_integral: float = 0.0
    same_side_duration_sec: float = 0.0
    observed_sec: float = 0.0
    cross_count_recent: int = 0
    cross_rate_per_min: float = 0.0
    dominant_support_margin: float = 0.0
    quality: str = "insufficient_history"

    def update(self, snapshot: Any, cfg: Any) -> None:
        age = float(getattr(snapshot, "age_sec", 0.0))
        if age < self.observe_start_age_sec:
            return
        price = getattr(snapshot, "polymarket_price", None)
        k_price = getattr(snapshot, "k_price", None)
        if price is None or k_price is None or float(k_price) <= 0.0:
            return
        distance = (float(price) - float(k_price)) / float(k_price) * 10000.0
        if distance > 0.0:
            side = "up"
        elif distance < 0.0:
            side = "down"
        else:
            return

        if self.cross_ages is None:
            self.cross_ages = []
        if self.first_observed_age_sec is None:
            self.first_observed_age_sec = age
            self.current_side = side
            self.last_observed_age_sec = age
            self._refresh_quality(cfg)
            return

        previous_age = self.last_observed_age_sec if self.last_observed_age_sec is not None else age
        dt = max(0.0, age - previous_age)
        if self.current_side == "up":
            self.up_support_integral += abs(distance) * dt
        elif self.current_side == "down":
            self.down_support_integral += abs(distance) * dt

        if self.current_side is not None and side != self.current_side:
            self.cross_count_total += 1
            self.cross_ages.append(age)
            self.last_cross_age_sec = age
            self.current_side = side

        self.last_observed_age_sec = age
        self._refresh_quality(cfg)

    def _refresh_quality(self, cfg: Any) -> None:
        if self.first_observed_age_sec is None or self.last_observed_age_sec is None:
            self.quality = "insufficient_history"
            return
        self.observed_sec = max(0.0, self.last_observed_age_sec - self.first_observed_age_sec)
        recent_window = float(getattr(cfg, "direction_recent_window_sec", 30.0))
        recent_start = self.last_observed_age_sec - recent_window
        crosses = self.cross_ages or []
        self.cross_count_recent = sum(1 for age in crosses if age >= recent_start)
        self.cross_rate_per_min = self.cross_count_total / max(self.observed_sec / 60.0, 1e-9)
        if self.last_cross_age_sec is None:
            self.same_side_duration_sec = self.observed_sec
        else:
            self.same_side_duration_sec = max(0.0, self.last_observed_age_sec - self.last_cross_age_sec)
        self.dominant_side = "up" if self.up_support_integral >= self.down_support_integral else "down"
        self.dominant_support_margin = abs(self.up_support_integral - self.down_support_integral)

        min_observed = float(getattr(cfg, "direction_min_observed_sec", 0.0))
        if self.observed_sec < min_observed:
            self.quality = "insufficient_history"
            return
        choppy_recent = int(getattr(cfg, "direction_choppy_recent_crosses", 2))
        choppy_total = int(getattr(cfg, "direction_choppy_total_crosses", 0))
        choppy_rate = float(getattr(cfg, "direction_choppy_cross_rate_per_min", 1.5))
        if (
            self.cross_count_recent >= choppy_recent
            or (choppy_total > 0 and self.cross_count_total >= choppy_total)
            or self.cross_rate_per_min >= choppy_rate
        ):
            self.quality = "choppy"
            return
        fresh_sec = float(getattr(cfg, "direction_fresh_cross_sec", 20.0))
        if self.cross_count_total > 0 and self.same_side_duration_sec < fresh_sec:
            self.quality = "fresh_cross"
            return
        stable_sec = float(getattr(cfg, "direction_stable_min_same_side_sec", 30.0))
        stable_recent = int(getattr(cfg, "direction_stable_max_recent_crosses", 1))
        if (
            self.current_side is not None
            and self.current_side == self.dominant_side
            and self.same_side_duration_sec >= stable_sec
            and self.cross_count_recent <= stable_recent
        ):
            self.quality = "stable"
            return
        self.quality = "acceptable"


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
    entry_amount_usd: float | None = None
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
    direction_state: DirectionState | None = None
    window_outcomes: list[WindowOutcome] | None = None
    exit_pressure_count: int = 0
    exit_pressure_reason: str | None = None

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
        self.direction_state = None
        self.exit_pressure_count = 0
        self.exit_pressure_reason = None

    def record_direction_observation(self, snapshot: Any, cfg: Any) -> None:
        market_slug = str(getattr(snapshot, "market_slug", self.current_market_slug or ""))
        if self.direction_state is None or self.direction_state.market_slug != market_slug:
            self.direction_state = DirectionState(
                market_slug=market_slug,
                observe_start_age_sec=float(getattr(cfg, "direction_observe_start_age_sec", 30.0)),
            )
        self.direction_state.update(snapshot, cfg)

    def record_window_settlement(self, market_slug: str, winning_side: str | None) -> None:
        if winning_side not in {"up", "down"}:
            return
        if self.window_outcomes is None:
            self.window_outcomes = []
        if self.window_outcomes and self.window_outcomes[-1].market_slug == market_slug:
            self.window_outcomes[-1] = WindowOutcome(market_slug=market_slug, winning_side=winning_side, epoch=_market_epoch(market_slug))
        else:
            self.window_outcomes.append(WindowOutcome(market_slug=market_slug, winning_side=winning_side, epoch=_market_epoch(market_slug)))
        self.window_outcomes = self.window_outcomes[-8:]

    @property
    def prior_same_side_streak_len(self) -> int:
        if not self.window_outcomes:
            return 0
        side = self.window_outcomes[-1].winning_side
        streak = 0
        expected_epoch: int | None = None
        for outcome in reversed(self.window_outcomes):
            if outcome.winning_side != side:
                break
            if expected_epoch is not None and outcome.epoch is not None and outcome.epoch != expected_epoch:
                break
            streak += 1
            expected_epoch = outcome.epoch - 300 if outcome.epoch is not None else None
        return streak

    @property
    def prior_same_side_streak_side(self) -> str | None:
        if not self.window_outcomes:
            return None
        return self.window_outcomes[-1].winning_side

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
        self.exit_pressure_count = 0
        self.exit_pressure_reason = None

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
        if self.open_position.entry_amount_usd is not None:
            self.open_position.entry_amount_usd = max(
                0.0,
                self.open_position.entry_amount_usd - self.open_position.entry_avg_price * exit_shares,
            )
        closed = self.open_position.filled_shares <= 1e-9
        if closed:
            self.open_position.exit_status = reason
            self.open_position = None
            self.prob_history = []
            self.exit_pressure_count = 0
            self.exit_pressure_reason = None
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
        self.exit_pressure_count = 0
        self.exit_pressure_reason = None
        return position

    def record_position_settlement(self, position: PositionSnapshot, winning_side: str) -> float:
        self.record_window_settlement(position.market_slug, winning_side)
        settlement_value = 1.0 if position.token_side == winning_side else 0.0
        pnl = (settlement_value - position.entry_avg_price) * position.filled_shares
        self.realized_pnl += pnl
        self.peak_pnl = max(self.peak_pnl, self.realized_pnl)
        position.exit_status = "settled"
        self.last_exit_reason = "settled"
        self.last_exit_side = position.token_side
        self.last_exit_age_sec = None
        self.prob_history = []
        self.exit_pressure_count = 0
        self.exit_pressure_reason = None
        return pnl

    def record_position_unsettled(self, position: PositionSnapshot, reason: str) -> float:
        position.exit_status = reason
        self.last_exit_reason = reason
        self.last_exit_side = position.token_side
        self.last_exit_age_sec = None
        self.prob_history = []
        self.exit_pressure_count = 0
        self.exit_pressure_reason = None
        return 0.0

    def record_settlement(self, winning_side: str) -> float:
        if self.open_position is None:
            return 0.0
        position = self.open_position
        self.open_position = None
        self.record_window_settlement(position.market_slug, winning_side)
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
