"""Replay collector JSONL rows through the probability-edge strategy."""

from __future__ import annotations

import itertools
import math
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable

from new_poly.strategy.prob_edge import EdgeConfig, MarketSnapshot, evaluate_entry, evaluate_exit, required_edge_for_entry
from new_poly.strategy.state import PositionSnapshot, StrategyState


NORMAL_SELL_EXIT_REASONS = {
    "logic_decay_exit",
    "risk_exit",
    "market_overprice_exit",
    "market_disagrees_exit",
    "defensive_take_profit",
    "profit_protection_exit",
}
FINAL_FORCE_SELL_BUFFER_TICKS = 5.0


@dataclass(frozen=True)
class BacktestConfig:
    amount_usd: float = 5.0
    early_required_edge: float = 0.16
    core_required_edge: float = 0.14
    entry_start_age_sec: float = 90.0
    entry_end_age_sec: float = 270.0
    max_book_age_ms: float = 1000.0
    max_entries_per_market: int = 2
    late_entry_enabled: bool = False
    tick_size: float = 0.01
    buy_slippage_ticks: float = 0.0
    sell_slippage_ticks: float = 0.0
    sell_price_buffer_ticks: float = 4.0
    sell_retry_price_buffer_ticks: float = 5.0
    prob_drop_exit_window_sec: float = 0.0
    prob_drop_exit_threshold: float = 0.0
    final_force_exit_remaining_sec: float = 30.0
    settlement_boundary_usd: float = 5.0
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
    honor_order_events: bool = False

    def edge_config(self) -> EdgeConfig:
        return EdgeConfig(
            early_required_edge=self.early_required_edge,
            core_required_edge=self.core_required_edge,
            entry_start_age_sec=self.entry_start_age_sec,
            entry_end_age_sec=self.entry_end_age_sec,
            max_book_age_ms=self.max_book_age_ms,
            max_entries_per_market=self.max_entries_per_market,
            late_entry_enabled=self.late_entry_enabled,
            prob_drop_exit_window_sec=self.prob_drop_exit_window_sec,
            prob_drop_exit_threshold=self.prob_drop_exit_threshold,
            final_force_exit_remaining_sec=self.final_force_exit_remaining_sec,
            min_fair_cap_margin_ticks=self.min_fair_cap_margin_ticks,
            entry_tick_size=self.entry_tick_size,
            min_entry_model_prob=self.min_entry_model_prob,
            low_price_extra_edge_threshold=self.low_price_extra_edge_threshold,
            low_price_extra_edge=self.low_price_extra_edge,
            cross_source_max_bps=self.cross_source_max_bps,
            market_disagrees_exit_threshold=self.market_disagrees_exit_threshold,
            market_disagrees_exit_max_remaining_sec=self.market_disagrees_exit_max_remaining_sec,
            market_disagrees_exit_min_loss=self.market_disagrees_exit_min_loss,
            market_disagrees_exit_min_age_sec=self.market_disagrees_exit_min_age_sec,
            market_disagrees_exit_max_profit=self.market_disagrees_exit_max_profit,
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
    if row.get("volatility_stale") is True:
        return None
    return _float(row.get("sigma_eff")) or _float((row.get("volatility") or {}).get("sigma"))


def _token(row: dict[str, Any], side: str) -> dict[str, Any]:
    value = row.get(side)
    return value if isinstance(value, dict) else {}


def _warnings(row: dict[str, Any]) -> set[str]:
    value = row.get("warnings")
    if isinstance(value, list):
        return {str(item) for item in value}
    return set()


def snapshot_from_row(row: dict[str, Any]) -> MarketSnapshot:
    up = _token(row, "up")
    down = _token(row, "down")
    warnings = _warnings(row)
    s_price = None if "polymarket_ws_open_disagrees_with_api" in warnings else _float(row.get("s_price"))
    return MarketSnapshot(
        market_slug=str(row.get("market_slug") or ""),
        age_sec=float(row.get("age_sec") or 0.0),
        remaining_sec=float(row.get("remaining_sec") or 0.0),
        s_price=s_price,
        k_price=_float(row.get("k_price")),
        sigma_eff=_sigma(row),
        up_ask_avg=_float(up.get("ask_avg")),
        down_ask_avg=_float(down.get("ask_avg")),
        up_ask_limit=_float(up.get("ask_limit")),
        down_ask_limit=_float(down.get("ask_limit")),
        up_ask_safety_limit=_float(up.get("ask_safety_limit")),
        down_ask_safety_limit=_float(down.get("ask_safety_limit")),
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
        source_spread_bps=_float(row.get("source_spread_bps") or (row.get("analysis") or {}).get("price_sources", {}).get("source_spread_bps")),
    )


def _settlement(rows: list[dict[str, Any]], *, boundary_usd: float) -> dict[str, Any]:
    if not rows:
        return {"winning_side": None, "settlement_uncertain": True}
    last = rows[-1]
    s_price = _float(last.get("s_price"))
    k_price = _float(last.get("k_price"))
    if s_price is None or k_price is None:
        return {"winning_side": None, "settlement_uncertain": True}
    return {
        "winning_side": "up" if s_price > k_price else "down",
        "settlement_uncertain": abs(s_price - k_price) < boundary_usd,
        "settlement_price": s_price,
        "settlement_k_price": k_price,
    }


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


def _entry_fill_price(decision, cfg: BacktestConfig) -> float | None:
    base = decision.depth_limit_price if decision.depth_limit_price is not None else decision.price
    if base is None:
        return None
    fill_price = round(base + cfg.buy_slippage_ticks * cfg.tick_size, 6)
    if decision.limit_price is not None and fill_price > decision.limit_price + 1e-12:
        return None
    return fill_price


def _exit_fill_price(decision, cfg: BacktestConfig) -> float | None:
    executable = decision.price
    floor_base = decision.limit_price if decision.limit_price is not None else decision.price
    if executable is None or floor_base is None:
        return None
    if decision.reason == "final_force_exit":
        # Mirror the first live emergency floor. Replay does not simulate the
        # retry ladder, so it uses attempt 1 for deterministic comparison.
        strategy_floor_ticks = FINAL_FORCE_SELL_BUFFER_TICKS
    elif decision.reason in NORMAL_SELL_EXIT_REASONS:
        strategy_floor_ticks = cfg.sell_price_buffer_ticks
    else:
        strategy_floor_ticks = 0.0
    fak_floor = floor_base - strategy_floor_ticks * cfg.tick_size
    slipped = executable - cfg.sell_slippage_ticks * cfg.tick_size
    return round(min(1.0, max(cfg.tick_size, fak_floor, slipped)), 6)


def _entry_fill_price_from_snapshot(decision, snap: MarketSnapshot, cfg: BacktestConfig) -> float | None:
    if decision.side == "up":
        depth_limit = snap.up_ask_limit
    elif decision.side == "down":
        depth_limit = snap.down_ask_limit
    else:
        depth_limit = None
    if depth_limit is None:
        return None
    fill_price = round(depth_limit + cfg.buy_slippage_ticks * cfg.tick_size, 6)
    if decision.limit_price is not None and fill_price > decision.limit_price + 1e-12:
        return None
    return fill_price


def _exit_fill_price_from_snapshot(decision, snap: MarketSnapshot, cfg: BacktestConfig) -> float | None:
    if decision.side == "up":
        bid_avg = snap.up_bid_avg
        bid_limit = snap.up_bid_limit
        depth_ok = snap.up_bid_depth_ok
    elif decision.side == "down":
        bid_avg = snap.down_bid_avg
        bid_limit = snap.down_bid_limit
        depth_ok = snap.down_bid_depth_ok
    else:
        return None
    if not depth_ok or bid_avg is None or bid_limit is None:
        return None
    if decision.reason == "final_force_exit":
        strategy_floor_ticks = FINAL_FORCE_SELL_BUFFER_TICKS
    elif decision.reason in NORMAL_SELL_EXIT_REASONS:
        strategy_floor_ticks = cfg.sell_price_buffer_ticks
    else:
        strategy_floor_ticks = 0.0
    fak_floor = bid_limit - strategy_floor_ticks * cfg.tick_size
    slipped = bid_avg - cfg.sell_slippage_ticks * cfg.tick_size
    return round(min(1.0, max(cfg.tick_size, fak_floor, slipped)), 6)


def _order_success(row: dict[str, Any]) -> bool:
    order = row.get("order")
    return isinstance(order, dict) and bool(order.get("success"))


def _analysis(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("analysis")
    return value if isinstance(value, dict) else {}


def _order(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("order")
    return value if isinstance(value, dict) else {}


def _event_entry(row: dict[str, Any], slug: str, cfg: BacktestConfig) -> tuple[PositionSnapshot, dict[str, Any]] | None:
    if row.get("event") != "entry" or not _order_success(row):
        return None
    analysis = _analysis(row)
    order = _order(row)
    side = analysis.get("entry_side")
    entry_price = _float(analysis.get("entry_price")) or _float(order.get("avg_price"))
    model_prob = _float(analysis.get("entry_model_prob")) or _float((row.get("decision") or {}).get("model_prob"))
    edge = _float(analysis.get("entry_edge_signal")) or _float((row.get("decision") or {}).get("edge")) or 0.0
    if side not in {"up", "down"} or entry_price is None or model_prob is None:
        return None
    shares = _float(order.get("filled_size")) or cfg.amount_usd / entry_price
    age_sec = float(row.get("age_sec") or 0.0)
    position = PositionSnapshot(
        market_slug=slug,
        token_side=side,
        token_id=f"{slug}:{side}",
        entry_time=age_sec,
        entry_avg_price=entry_price,
        filled_shares=shares,
        entry_model_prob=model_prob,
        entry_edge=edge,
    )
    trade = {
        "market_slug": slug,
        "entry_side": side,
        "entry_phase": analysis.get("entry_phase"),
        "entry_age_sec": age_sec,
        "entry_price": entry_price,
        "entry_model_prob": model_prob,
        "entry_edge": edge,
        "entry_edge_at_fill": model_prob - entry_price,
        "shares": shares,
    }
    return position, trade


def _event_exit_price(row: dict[str, Any]) -> float | None:
    return _float(row.get("exit_price")) or _float(_analysis(row).get("exit_price")) or _float(_order(row).get("avg_price"))


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
    unsettled = 0
    settlement_uncertain = 0
    skip_reasons: Counter[str] = Counter()

    for slug, group in _group_rows(rows):
        windows += 1
        state = StrategyState(current_market_slug=slug)
        state.reset_for_market(slug)
        active_trade: dict[str, Any] | None = None
        skip_next_row = False
        for index, row in enumerate(group):
            if skip_next_row:
                skip_next_row = False
                continue
            snap = snapshot_from_row(row)
            if cfg.honor_order_events:
                if row.get("event") == "order_no_fill":
                    intent = str(row.get("order_intent") or "order")
                    skip_reasons[f"{intent}_no_fill"] += 1
                    continue
                if not state.has_position:
                    event_entry = _event_entry(row, slug, cfg)
                    if event_entry is not None:
                        position, active_trade = event_entry
                        state.record_entry(position)
                        entries += 1
                        continue
                elif row.get("event") == "exit" and _order_success(row) and active_trade is not None:
                    exit_price = _event_exit_price(row)
                    if exit_price is not None:
                        reason = str(row.get("exit_reason") or _analysis(row).get("exit_reason") or (row.get("decision") or {}).get("reason") or "event_exit")
                        pnl = state.record_exit(exit_price, reason)
                        exit_age = float(row.get("age_sec") or 0.0)
                        active_trade.update({
                            "exit_age_sec": exit_age,
                            "exit_reason": reason,
                            "exit_price": exit_price,
                            "pnl": pnl,
                            "hold_sec": exit_age - active_trade["entry_age_sec"],
                        })
                        equity += pnl
                        equity_points.append(equity)
                        trades.append(active_trade)
                        active_trade = None
                        exits += 1
                        continue
            if state.has_position and state.open_position is not None:
                decision = evaluate_exit(snap, state.open_position, edge_cfg, state)
                if decision.model_prob is not None:
                    state.record_model_prob(
                        snap.age_sec,
                        decision.model_prob,
                        retention_sec=max(edge_cfg.prob_stagnation_window_sec, edge_cfg.prob_drop_exit_window_sec, 5.0),
                    )
                if decision.action == "exit" and decision.price is not None and active_trade is not None:
                    exit_price = _exit_fill_price(decision, cfg)
                    retry_snap: MarketSnapshot | None = None
                    if exit_price is None and index + 1 < len(group):
                        retry_snap = snapshot_from_row(group[index + 1])
                        exit_price = _exit_fill_price_from_snapshot(decision, retry_snap, cfg)
                    if exit_price is None:
                        skip_reasons["exit_no_fill"] += 1
                        continue
                    exit_age = retry_snap.age_sec if retry_snap is not None else snap.age_sec
                    if retry_snap is not None:
                        skip_next_row = True
                    pnl = state.record_exit(exit_price, decision.reason)
                    active_trade.update({
                        "exit_age_sec": exit_age,
                        "exit_reason": decision.reason,
                        "exit_price": exit_price,
                        "pnl": pnl,
                        "hold_sec": exit_age - active_trade["entry_age_sec"],
                    })
                    equity += pnl
                    equity_points.append(equity)
                    trades.append(active_trade)
                    active_trade = None
                    exits += 1
            else:
                decision = evaluate_entry(snap, state, edge_cfg)
                if decision.action == "skip":
                    skip_reasons[decision.reason] += 1
                if decision.action == "enter" and decision.side is not None and decision.price is not None and decision.model_prob is not None and decision.edge is not None:
                    fill_price = _entry_fill_price(decision, cfg)
                    fill_snap = snap
                    if fill_price is None:
                        if index + 1 < len(group):
                            retry_snap = snapshot_from_row(group[index + 1])
                            retry_decision = evaluate_entry(retry_snap, state, edge_cfg)
                            if (
                                retry_decision.action == "enter"
                                and retry_decision.side == decision.side
                                and retry_decision.price is not None
                                and retry_decision.model_prob is not None
                                and retry_decision.edge is not None
                            ):
                                retry_fill = _entry_fill_price_from_snapshot(retry_decision, retry_snap, cfg)
                                if retry_fill is not None:
                                    decision = retry_decision
                                    fill_price = retry_fill
                                    fill_snap = retry_snap
                                    skip_next_row = True
                        if fill_price is None:
                            skip_reasons["entry_no_fill"] += 1
                            continue
                    shares = cfg.amount_usd / fill_price
                    state.record_entry(PositionSnapshot(
                        market_slug=slug,
                        token_side=decision.side,
                        token_id=f"{slug}:{decision.side}",
                        entry_time=fill_snap.age_sec,
                        entry_avg_price=fill_price,
                        filled_shares=shares,
                        entry_model_prob=decision.model_prob,
                        entry_edge=decision.edge,
                    ))
                    entry_phase = decision.phase or required_edge_for_entry(fill_snap, edge_cfg).phase
                    active_trade = {
                        "market_slug": slug,
                        "entry_side": decision.side,
                        "entry_phase": entry_phase,
                        "entry_age_sec": fill_snap.age_sec,
                        "entry_price": fill_price,
                        "entry_model_prob": decision.model_prob,
                        "entry_edge": decision.edge,
                        "entry_edge_at_fill": decision.model_prob - fill_price,
                        "shares": shares,
                    }
                    entries += 1

        if state.has_position and state.open_position is not None and active_trade is not None:
            settlement = _settlement(group, boundary_usd=cfg.settlement_boundary_usd)
            winning_side = settlement.get("winning_side")
            if winning_side is not None:
                pnl = state.record_settlement(winning_side)
                is_uncertain = bool(settlement.get("settlement_uncertain"))
                active_trade.update({
                    "exit_age_sec": 300.0,
                    "exit_reason": "settlement",
                    "exit_price": 1.0 if active_trade["entry_side"] == winning_side else 0.0,
                    "pnl": pnl,
                    "hold_sec": 300.0 - active_trade["entry_age_sec"],
                    "winning_side": winning_side,
                    "settlement_uncertain": is_uncertain,
                    "settlement_price": settlement.get("settlement_price"),
                    "settlement_k_price": settlement.get("settlement_k_price"),
                })
                equity += pnl
                equity_points.append(equity)
                trades.append(active_trade)
                settlements += 1
                if is_uncertain:
                    settlement_uncertain += 1
            else:
                pnl = state.record_exit(state.open_position.entry_avg_price, "unsettled_no_settlement_side")
                active_trade.update({
                    "exit_age_sec": 300.0,
                    "exit_reason": "unsettled_no_settlement_side",
                    "exit_price": active_trade["entry_price"],
                    "pnl": pnl,
                    "hold_sec": 300.0 - active_trade["entry_age_sec"],
                    "settlement_uncertain": True,
                })
                equity += pnl
                equity_points.append(equity)
                trades.append(active_trade)
                unsettled += 1

    wins = sum(1 for trade in trades if trade.get("pnl", 0.0) > 0)
    total_pnl = round(sum(float(trade.get("pnl") or 0.0) for trade in trades), 6)
    summary = {
        "windows": windows,
        "entries": entries,
        "closed_trades": len(trades),
        "exits": exits,
        "settlements": settlements,
        "unsettled": unsettled,
        "settlement_uncertain": settlement_uncertain,
        "win_rate": round(wins / len(trades), 4) if trades else 0.0,
        "total_pnl": total_pnl,
        "avg_pnl_per_trade": round(total_pnl / len(trades), 6) if trades else 0.0,
        "max_drawdown": _max_drawdown(equity_points),
        "avg_hold_sec": round(sum(float(trade.get("hold_sec") or 0.0) for trade in trades) / len(trades), 2) if trades else 0.0,
        "skip_reason_counts": dict(sorted(skip_reasons.items(), key=lambda pair: (-pair[1], pair[0]))),
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
    min_entries: int = 0,
    sort_by: str = "pnl",
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
            max_entries_per_market=base.max_entries_per_market,
            late_entry_enabled=base.late_entry_enabled,
            tick_size=base.tick_size,
            buy_slippage_ticks=base.buy_slippage_ticks,
            sell_slippage_ticks=base.sell_slippage_ticks,
            sell_price_buffer_ticks=base.sell_price_buffer_ticks,
            sell_retry_price_buffer_ticks=base.sell_retry_price_buffer_ticks,
            prob_drop_exit_window_sec=base.prob_drop_exit_window_sec,
            prob_drop_exit_threshold=base.prob_drop_exit_threshold,
            final_force_exit_remaining_sec=base.final_force_exit_remaining_sec,
            settlement_boundary_usd=base.settlement_boundary_usd,
            min_fair_cap_margin_ticks=base.min_fair_cap_margin_ticks,
            entry_tick_size=base.entry_tick_size,
            min_entry_model_prob=base.min_entry_model_prob,
            low_price_extra_edge_threshold=base.low_price_extra_edge_threshold,
            low_price_extra_edge=base.low_price_extra_edge,
            cross_source_max_bps=base.cross_source_max_bps,
            market_disagrees_exit_threshold=base.market_disagrees_exit_threshold,
            market_disagrees_exit_max_remaining_sec=base.market_disagrees_exit_max_remaining_sec,
            market_disagrees_exit_min_loss=base.market_disagrees_exit_min_loss,
            market_disagrees_exit_min_age_sec=base.market_disagrees_exit_min_age_sec,
            market_disagrees_exit_max_profit=base.market_disagrees_exit_max_profit,
            honor_order_events=base.honor_order_events,
        )
        result = run_backtest(materialized, cfg)
        if result.summary["entries"] < min_entries:
            continue
        results.append({
            "early_required_edge": early,
            "core_required_edge": core,
            "entry_start_age_sec": start,
            "entry_end_age_sec": end,
            **result.summary,
        })
    if sort_by == "win_rate":
        key = lambda item: (item["win_rate"], item["entries"], item["total_pnl"])
    elif sort_by == "avg_pnl":
        key = lambda item: (item["avg_pnl_per_trade"], item["win_rate"], item["entries"])
    else:
        key = lambda item: (item["total_pnl"], item["win_rate"], item["entries"])
    return sorted(results, key=key, reverse=True)
