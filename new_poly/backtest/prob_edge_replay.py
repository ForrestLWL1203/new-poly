"""Replay collector JSONL rows through the probability-edge strategy."""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass
from typing import Any, Iterable

from new_poly.strategy.prob_edge import EdgeConfig, MarketSnapshot, evaluate_entry, evaluate_exit
from new_poly.strategy.state import PositionSnapshot, StrategyState


@dataclass(frozen=True)
class BacktestConfig:
    amount_usd: float = 5.0
    early_required_edge: float = 0.12
    core_required_edge: float = 0.08
    entry_start_age_sec: float = 90.0
    entry_end_age_sec: float = 270.0
    max_book_age_ms: float = 1000.0
    late_entry_enabled: bool = False
    tick_size: float = 0.01
    buy_slippage_ticks: float = 0.0
    sell_slippage_ticks: float = 0.0

    def edge_config(self) -> EdgeConfig:
        return EdgeConfig(
            early_required_edge=self.early_required_edge,
            core_required_edge=self.core_required_edge,
            entry_start_age_sec=self.entry_start_age_sec,
            entry_end_age_sec=self.entry_end_age_sec,
            max_book_age_ms=self.max_book_age_ms,
            late_entry_enabled=self.late_entry_enabled,
        )


@dataclass(frozen=True)
class BacktestResult:
    summary: dict[str, Any]
    trades: list[dict[str, Any]]


def _float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _sigma(row: dict[str, Any]) -> float | None:
    return _float(row.get("sigma_eff")) or _float((row.get("volatility") or {}).get("sigma"))


def _token(row: dict[str, Any], side: str) -> dict[str, Any]:
    value = row.get(side)
    return value if isinstance(value, dict) else {}


def snapshot_from_row(row: dict[str, Any]) -> MarketSnapshot:
    up = _token(row, "up")
    down = _token(row, "down")
    return MarketSnapshot(
        market_slug=str(row.get("market_slug") or ""),
        age_sec=float(row.get("age_sec") or 0.0),
        remaining_sec=float(row.get("remaining_sec") or 0.0),
        s_price=_float(row.get("s_price")),
        k_price=_float(row.get("k_price")),
        sigma_eff=_sigma(row),
        up_ask_avg=_float(up.get("ask_avg")),
        down_ask_avg=_float(down.get("ask_avg")),
        up_ask_limit=_float(up.get("ask_limit")),
        down_ask_limit=_float(down.get("ask_limit")),
        up_best_ask=_float(up.get("ask")),
        down_best_ask=_float(down.get("ask")),
        up_bid_avg=_float(up.get("bid_avg")),
        down_bid_avg=_float(down.get("bid_avg")),
        up_bid_limit=_float(up.get("bid_limit")),
        down_bid_limit=_float(down.get("bid_limit")),
        up_ask_depth_ok=bool(up.get("ask_depth_ok")),
        down_ask_depth_ok=bool(down.get("ask_depth_ok")),
        up_bid_depth_ok=bool(up.get("bid_depth_ok")),
        down_bid_depth_ok=bool(down.get("bid_depth_ok")),
        up_book_age_ms=_float(up.get("book_age_ms")),
        down_book_age_ms=_float(down.get("book_age_ms")),
    )


def _settlement_side(rows: list[dict[str, Any]]) -> str | None:
    with_k = [row for row in rows if row.get("k_price") is not None and row.get("s_price") is not None]
    if not with_k:
        return None
    last = with_k[-1]
    return "up" if float(last["s_price"]) > float(last["k_price"]) else "down"


def _group_rows(rows: Iterable[dict[str, Any]]) -> Iterable[tuple[str, list[dict[str, Any]]]]:
    sorted_rows = sorted(rows, key=lambda item: (str(item.get("market_slug") or ""), float(item.get("age_sec") or 0.0)))
    for slug, group in itertools.groupby(sorted_rows, key=lambda item: str(item.get("market_slug") or "")):
        if slug:
            yield slug, list(group)


def _max_drawdown(equity_points: list[float]) -> float:
    peak = 0.0
    worst = 0.0
    for value in equity_points:
        peak = max(peak, value)
        worst = min(worst, value - peak)
    return round(worst, 6)


def _phase(age_sec: float) -> str:
    if age_sec < 120:
        return "early"
    if age_sec < 240:
        return "core"
    if age_sec < 270:
        return "late"
    return "no_entry"


def _entry_fill_price(decision, cfg: BacktestConfig) -> float | None:
    base = decision.depth_limit_price if decision.depth_limit_price is not None else decision.price
    if base is None:
        return None
    fill_price = round(base + cfg.buy_slippage_ticks * cfg.tick_size, 6)
    if decision.limit_price is not None and fill_price > decision.limit_price + 1e-12:
        return None
    return fill_price


def _exit_fill_price(decision, cfg: BacktestConfig) -> float | None:
    base = decision.limit_price if decision.limit_price is not None else decision.price
    if base is None:
        return None
    return round(max(0.0, base - cfg.sell_slippage_ticks * cfg.tick_size), 6)


def run_backtest(rows: Iterable[dict[str, Any]], config: BacktestConfig | None = None) -> BacktestResult:
    cfg = config or BacktestConfig()
    edge_cfg = cfg.edge_config()
    trades: list[dict[str, Any]] = []
    equity = 0.0
    equity_points: list[float] = []
    windows = 0
    entries = 0
    exits = 0
    settlements = 0

    for slug, group in _group_rows(rows):
        windows += 1
        state = StrategyState(current_market_slug=slug)
        state.reset_for_market(slug)
        active_trade: dict[str, Any] | None = None
        for row in group:
            snap = snapshot_from_row(row)
            if snap.k_price is None or snap.s_price is None or snap.sigma_eff is None:
                continue
            if state.has_position and state.open_position is not None:
                decision = evaluate_exit(snap, state.open_position, edge_cfg, state)
                if decision.model_prob is not None:
                    state.record_model_prob(snap.age_sec, decision.model_prob)
                if decision.action == "exit" and decision.price is not None and active_trade is not None:
                    exit_price = _exit_fill_price(decision, cfg)
                    if exit_price is None:
                        continue
                    pnl = state.record_exit(exit_price, decision.reason)
                    active_trade.update({
                        "exit_age_sec": snap.age_sec,
                        "exit_reason": decision.reason,
                        "exit_price": exit_price,
                        "pnl": pnl,
                        "hold_sec": snap.age_sec - active_trade["entry_age_sec"],
                    })
                    equity += pnl
                    equity_points.append(equity)
                    trades.append(active_trade)
                    active_trade = None
                    exits += 1
            else:
                decision = evaluate_entry(snap, state, edge_cfg)
                if decision.action == "enter" and decision.side is not None and decision.price is not None and decision.model_prob is not None and decision.edge is not None:
                    fill_price = _entry_fill_price(decision, cfg)
                    if fill_price is None:
                        continue
                    shares = cfg.amount_usd / fill_price
                    state.record_entry(PositionSnapshot(
                        market_slug=slug,
                        token_side=decision.side,
                        token_id=f"{slug}:{decision.side}",
                        entry_time=snap.age_sec,
                        entry_avg_price=fill_price,
                        filled_shares=shares,
                        entry_model_prob=decision.model_prob,
                        entry_edge=decision.model_prob - fill_price,
                    ))
                    active_trade = {
                        "market_slug": slug,
                        "entry_side": decision.side,
                        "entry_phase": _phase(snap.age_sec),
                        "entry_age_sec": snap.age_sec,
                        "entry_price": fill_price,
                        "entry_model_prob": decision.model_prob,
                        "entry_edge": decision.model_prob - fill_price,
                        "shares": shares,
                    }
                    entries += 1

        if state.has_position and state.open_position is not None and active_trade is not None:
            winning_side = _settlement_side(group)
            if winning_side is not None:
                pnl = state.record_settlement(winning_side)
                active_trade.update({
                    "exit_age_sec": 300.0,
                    "exit_reason": "settlement",
                    "exit_price": 1.0 if active_trade["entry_side"] == winning_side else 0.0,
                    "pnl": pnl,
                    "hold_sec": 300.0 - active_trade["entry_age_sec"],
                    "winning_side": winning_side,
                })
                equity += pnl
                equity_points.append(equity)
                trades.append(active_trade)
                settlements += 1

    wins = sum(1 for trade in trades if trade.get("pnl", 0.0) > 0)
    total_pnl = round(sum(float(trade.get("pnl") or 0.0) for trade in trades), 6)
    summary = {
        "windows": windows,
        "entries": entries,
        "closed_trades": len(trades),
        "exits": exits,
        "settlements": settlements,
        "win_rate": round(wins / len(trades), 4) if trades else 0.0,
        "total_pnl": total_pnl,
        "avg_pnl_per_trade": round(total_pnl / len(trades), 6) if trades else 0.0,
        "max_drawdown": _max_drawdown(equity_points),
        "avg_hold_sec": round(sum(float(trade.get("hold_sec") or 0.0) for trade in trades) / len(trades), 2) if trades else 0.0,
    }
    return BacktestResult(summary=summary, trades=trades)


def scan_configs(
    rows: Iterable[dict[str, Any]],
    *,
    early_edges: Iterable[float],
    core_edges: Iterable[float],
    entry_starts: Iterable[float],
    entry_ends: Iterable[float],
    base_config: BacktestConfig | None = None,
) -> list[dict[str, Any]]:
    materialized = list(rows)
    base = base_config or BacktestConfig()
    results: list[dict[str, Any]] = []
    for early, core, start, end in itertools.product(early_edges, core_edges, entry_starts, entry_ends):
        cfg = BacktestConfig(
            amount_usd=base.amount_usd,
            early_required_edge=float(early),
            core_required_edge=float(core),
            entry_start_age_sec=float(start),
            entry_end_age_sec=float(end),
            max_book_age_ms=base.max_book_age_ms,
            late_entry_enabled=base.late_entry_enabled,
            tick_size=base.tick_size,
            buy_slippage_ticks=base.buy_slippage_ticks,
            sell_slippage_ticks=base.sell_slippage_ticks,
        )
        result = run_backtest(materialized, cfg)
        results.append({
            "early_required_edge": early,
            "core_required_edge": core,
            "entry_start_age_sec": start,
            "entry_end_age_sec": end,
            **result.summary,
        })
    return sorted(results, key=lambda item: (item["total_pnl"], item["win_rate"], item["entries"]), reverse=True)
