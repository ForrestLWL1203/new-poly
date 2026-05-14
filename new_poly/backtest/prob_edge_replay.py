"""Replay collector JSONL rows through the probability-edge strategy."""

from __future__ import annotations

import math
import bisect
import itertools
from collections import Counter
from dataclasses import dataclass, replace
from typing import Any, Iterable

from new_poly.strategy.prob_edge import MarketSnapshot
from new_poly.strategy.poly_source import PolySourceConfig, evaluate_poly_entry, evaluate_poly_exit
from new_poly.strategy.state import PositionSnapshot, StrategyState
from new_poly.trading.execution import sell_aggression_ticks


@dataclass(frozen=True)
class BacktestConfig:
    strategy_mode: str = "poly_single_source"
    amount_usd: float = 5.0
    early_to_core_age_sec: float = 120.0
    core_to_late_age_sec: float = 240.0
    entry_start_age_sec: float = 90.0
    entry_end_age_sec: float = 270.0
    final_no_entry_remaining_sec: float = 30.0
    pre_entry_observation_start_age_sec: float = 0.0
    early_value_entry_enabled: bool = False
    early_value_start_age_sec: float = 60.0
    early_value_end_age_sec: float = 120.0
    early_value_min_reference_distance_bps: float = 2.5
    early_value_min_poly_return_bps: float = 0.5
    early_value_min_entry_score: float = 5.5
    early_value_max_entry_ask: float = 0.60
    early_value_max_spread: float = 0.06
    early_value_hold_protection_enabled: bool = False
    max_book_age_ms: float = 1000.0
    max_entries_per_market: int = 2
    entry_tick_size: float = 0.01
    buy_slippage_ticks: float = 0.0
    sell_slippage_ticks: float = 0.0
    sell_price_buffer_ticks: float = 5.0
    sell_retry_price_buffer_ticks: float = 8.0
    sell_dynamic_buffer_enabled: bool = True
    sell_profit_exit_buffer_ticks: float = 5.0
    sell_profit_exit_retry_buffer_ticks: float = 8.0
    sell_risk_exit_buffer_ticks: float = 8.0
    sell_risk_exit_retry_buffer_ticks: float = 12.0
    sell_force_exit_buffer_ticks: float = 10.0
    sell_force_exit_retry_buffer_ticks: float = 15.0
    hold_to_settlement_enabled: bool = False
    hold_to_settlement_min_profit_ratio: float = 2.0
    hold_to_settlement_min_bid_avg: float = 0.80
    hold_to_settlement_min_bid_limit: float = 0.75
    settlement_boundary_usd: float = 5.0
    honor_order_events: bool = False
    poly_reference_distance_bps: float = 0.5
    max_poly_reference_distance_bps: float = 0.0
    poly_trend_lookback_sec: float = 3.0
    poly_return_bps: float = 0.3
    max_entry_ask: float = 0.65
    max_entry_fill_price: float = 0.0
    min_poly_entry_score: float = 0.0
    min_poly_hold_score: float = 0.0
    poly_score_component_logs: str = "compact"
    poly_buy_price_buffer_ticks: float = 2.0
    reference_distance_exit_remaining_sec: tuple[float, ...] = (120.0, 90.0, 70.0, 45.0, 30.0)
    reference_distance_exit_min_bps: tuple[float, ...] = (-2.0, -1.0, 0.25, 0.75, 1.0)
    poly_exit_min_hold_sec: float = 3.0
    poly_hold_to_settlement_min_reference_distance_bps: float = 1.0
    poly_hold_to_settlement_min_poly_return_bps: float = 0.0
    compute_poly_returns: bool = True

    def poly_source_config(self) -> PolySourceConfig:
        return PolySourceConfig(
            entry_start_age_sec=self.entry_start_age_sec,
            entry_end_age_sec=self.entry_end_age_sec,
            final_no_entry_remaining_sec=self.final_no_entry_remaining_sec,
            pre_entry_observation_start_age_sec=self.pre_entry_observation_start_age_sec,
            early_to_core_age_sec=self.early_to_core_age_sec,
            core_to_late_age_sec=self.core_to_late_age_sec,
            early_value_entry_enabled=self.early_value_entry_enabled,
            early_value_start_age_sec=self.early_value_start_age_sec,
            early_value_end_age_sec=self.early_value_end_age_sec,
            early_value_min_reference_distance_bps=self.early_value_min_reference_distance_bps,
            early_value_min_poly_return_bps=self.early_value_min_poly_return_bps,
            early_value_min_entry_score=self.early_value_min_entry_score,
            early_value_max_entry_ask=self.early_value_max_entry_ask,
            early_value_max_spread=self.early_value_max_spread,
            early_value_hold_protection_enabled=self.early_value_hold_protection_enabled,
            max_entries_per_market=self.max_entries_per_market,
            max_book_age_ms=self.max_book_age_ms,
            poly_reference_distance_bps=self.poly_reference_distance_bps,
            max_poly_reference_distance_bps=self.max_poly_reference_distance_bps,
            poly_trend_lookback_sec=self.poly_trend_lookback_sec,
            poly_return_bps=self.poly_return_bps,
            max_entry_ask=self.max_entry_ask,
            max_entry_fill_price=self.max_entry_fill_price,
            min_poly_entry_score=self.min_poly_entry_score,
            min_poly_hold_score=self.min_poly_hold_score,
            poly_score_component_logs=self.poly_score_component_logs,
            entry_tick_size=self.entry_tick_size,
            buy_price_buffer_ticks=self.poly_buy_price_buffer_ticks,
            reference_distance_exit_remaining_sec=self.reference_distance_exit_remaining_sec,
            reference_distance_exit_min_bps=self.reference_distance_exit_min_bps,
            exit_min_hold_sec=self.poly_exit_min_hold_sec,
            hold_to_settlement_enabled=self.hold_to_settlement_enabled,
            hold_to_settlement_min_profit_ratio=self.hold_to_settlement_min_profit_ratio,
            hold_to_settlement_min_bid_avg=self.hold_to_settlement_min_bid_avg,
            hold_to_settlement_min_bid_limit=self.hold_to_settlement_min_bid_limit,
            hold_to_settlement_min_reference_distance_bps=self.poly_hold_to_settlement_min_reference_distance_bps,
            hold_to_settlement_min_poly_return_bps=self.poly_hold_to_settlement_min_poly_return_bps,
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


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _float(value)
        if parsed is not None:
            return parsed
    return None


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
    analysis_sources = (row.get("analysis") or {}).get("price_sources", {})
    reference = row.get("reference") if isinstance(row.get("reference"), dict) else {}
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
        up_best_ask=_float(up.get("ask")),
        down_best_ask=_float(down.get("ask")),
        up_bid_avg=_float(up.get("bid_avg")),
        down_bid_avg=_float(down.get("bid_avg")),
        up_bid_limit=_float(up.get("bid_limit")),
        down_bid_limit=_float(down.get("bid_limit")),
        up_bid_depth_ok=bool(up.get("bid_depth_ok")),
        down_bid_depth_ok=bool(down.get("bid_depth_ok")),
        up_book_age_ms=_float(up.get("book_age_ms")),
        down_book_age_ms=_float(down.get("book_age_ms")),
        up_bid_age_ms=_float(up.get("bid_age_ms")),
        down_bid_age_ms=_float(down.get("bid_age_ms")),
        source_spread_bps=_first_float(row.get("source_spread_bps"), analysis_sources.get("source_spread_bps")),
        polymarket_divergence_bps=_first_float(
            row.get("polymarket_divergence_bps"),
            row.get("lead_binance_vs_polymarket_bps"),
            reference.get("polymarket_divergence_bps"),
            reference.get("lead_binance_vs_polymarket_bps"),
            analysis_sources.get("polymarket_divergence_bps"),
            analysis_sources.get("lead_binance_vs_polymarket_bps"),
        ),
        polymarket_price=_first_float(row.get("polymarket_price"), reference.get("polymarket_price"), analysis_sources.get("polymarket_price")),
        polymarket_price_age_sec=_first_float(row.get("polymarket_price_age_sec"), reference.get("polymarket_price_age_sec"), analysis_sources.get("polymarket_price_age_sec")),
        polymarket_return_1s_bps=_first_float(row.get("polymarket_return_1s_bps"), row.get("lead_polymarket_return_1s_bps"), reference.get("lead_polymarket_return_1s_bps"), analysis_sources.get("lead_polymarket_return_1s_bps")),
        polymarket_return_3s_bps=_first_float(row.get("polymarket_return_3s_bps"), row.get("lead_polymarket_return_3s_bps"), reference.get("lead_polymarket_return_3s_bps"), analysis_sources.get("lead_polymarket_return_3s_bps")),
        polymarket_return_5s_bps=_first_float(row.get("polymarket_return_5s_bps"), row.get("lead_polymarket_return_5s_bps"), reference.get("lead_polymarket_return_5s_bps"), analysis_sources.get("lead_polymarket_return_5s_bps")),
        polymarket_return_10s_bps=_first_float(row.get("polymarket_return_10s_bps"), row.get("lead_polymarket_return_10s_bps"), reference.get("lead_polymarket_return_10s_bps"), analysis_sources.get("lead_polymarket_return_10s_bps")),
        polymarket_return_15s_bps=_first_float(row.get("polymarket_return_15s_bps"), row.get("lead_polymarket_return_15s_bps"), reference.get("lead_polymarket_return_15s_bps"), analysis_sources.get("lead_polymarket_return_15s_bps")),
        poly_return_since_entry_start_bps=_first_float(row.get("poly_return_since_entry_start_bps"), reference.get("poly_return_since_entry_start_bps"), analysis_sources.get("poly_return_since_entry_start_bps")),
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


def _poly_price_from_row(row: dict[str, Any]) -> float | None:
    reference = row.get("reference") if isinstance(row.get("reference"), dict) else {}
    analysis_sources = (row.get("analysis") or {}).get("price_sources", {})
    return _first_float(row.get("polymarket_price"), reference.get("polymarket_price"), analysis_sources.get("polymarket_price"))


def _computed_poly_return_from_series(
    *,
    ages: list[float | None],
    searchable_ages: list[float],
    prices: list[float | None],
    index: int,
    lookback_sec: float,
) -> float | None:
    current_age = ages[index]
    current_price = prices[index]
    if current_age is None or current_price is None or current_price <= 0:
        return None
    target_age = current_age - lookback_sec
    previous_index = bisect.bisect_right(searchable_ages, target_age, hi=index) - 1
    while previous_index >= 0 and (ages[previous_index] is None or prices[previous_index] is None or (prices[previous_index] or 0.0) <= 0):
        previous_index -= 1
    if previous_index < 0:
        return None
    previous_age = ages[previous_index]
    previous_price = prices[previous_index]
    if previous_age is None or previous_price is None:
        return None
    if current_age - previous_age > lookback_sec + 2.0:
        return None
    return (current_price - previous_price) / previous_price * 10000.0


def _with_computed_poly_returns(rows: list[dict[str, Any]], *, entry_start_age_sec: float) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    ages = [_float(row.get("age_sec")) for row in rows]
    searchable_ages = [age if age is not None else float("-inf") for age in ages]
    prices = [_poly_price_from_row(row) for row in rows]
    for index, row in enumerate(rows):
        updated = dict(row)
        for lookback in (1.0, 3.0, 5.0, 10.0, 15.0):
            key = f"lead_polymarket_return_{int(lookback)}s_bps"
            if _first_float(updated.get(key), updated.get(f"polymarket_return_{int(lookback)}s_bps")) is None:
                value = _computed_poly_return_from_series(
                    ages=ages,
                    searchable_ages=searchable_ages,
                    prices=prices,
                    index=index,
                    lookback_sec=lookback,
                )
                if value is not None:
                    updated[key] = value
        age = _float(updated.get("age_sec"))
        if age is not None and age > entry_start_age_sec and _first_float(updated.get("poly_return_since_entry_start_bps")) is None:
            value = _computed_poly_return_from_series(
                ages=ages,
                searchable_ages=searchable_ages,
                prices=prices,
                index=index,
                lookback_sec=age - entry_start_age_sec,
            )
            if value is not None:
                updated["poly_return_since_entry_start_bps"] = value
        enriched.append(updated)
    return enriched


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
    fill_price = round(base + cfg.buy_slippage_ticks * cfg.entry_tick_size, 6)
    if decision.limit_price is not None and fill_price > decision.limit_price + 1e-12:
        return None
    return fill_price


def _exit_fill_price(decision, cfg: BacktestConfig) -> float | None:
    executable = decision.price
    floor_base = decision.limit_price if decision.limit_price is not None else decision.price
    if executable is None or floor_base is None:
        return None
    # Replay does not simulate the retry ladder, so it mirrors attempt 1.
    strategy_floor_ticks = sell_aggression_ticks(
        decision.reason,
        0,
        sell_dynamic_buffer_enabled=cfg.sell_dynamic_buffer_enabled,
        sell_price_buffer_ticks=cfg.sell_price_buffer_ticks,
        sell_retry_price_buffer_ticks=cfg.sell_retry_price_buffer_ticks,
        sell_profit_exit_buffer_ticks=cfg.sell_profit_exit_buffer_ticks,
        sell_profit_exit_retry_buffer_ticks=cfg.sell_profit_exit_retry_buffer_ticks,
        sell_risk_exit_buffer_ticks=cfg.sell_risk_exit_buffer_ticks,
        sell_risk_exit_retry_buffer_ticks=cfg.sell_risk_exit_retry_buffer_ticks,
        sell_force_exit_buffer_ticks=cfg.sell_force_exit_buffer_ticks,
        sell_force_exit_retry_buffer_ticks=cfg.sell_force_exit_retry_buffer_ticks,
    )
    fak_floor = floor_base - strategy_floor_ticks * cfg.entry_tick_size
    slipped = executable - cfg.sell_slippage_ticks * cfg.entry_tick_size
    return round(min(1.0, max(cfg.entry_tick_size, fak_floor, slipped)), 6)


def _entry_fill_price_from_snapshot(decision, snap: MarketSnapshot, cfg: BacktestConfig) -> float | None:
    if decision.side == "up":
        base = snap.up_best_ask
    elif decision.side == "down":
        base = snap.down_best_ask
    else:
        return None
    if base is None:
        return None
    fill_price = round(base + cfg.buy_slippage_ticks * cfg.entry_tick_size, 6)
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
    strategy_floor_ticks = sell_aggression_ticks(
        decision.reason,
        0,
        sell_dynamic_buffer_enabled=cfg.sell_dynamic_buffer_enabled,
        sell_price_buffer_ticks=cfg.sell_price_buffer_ticks,
        sell_retry_price_buffer_ticks=cfg.sell_retry_price_buffer_ticks,
        sell_profit_exit_buffer_ticks=cfg.sell_profit_exit_buffer_ticks,
        sell_profit_exit_retry_buffer_ticks=cfg.sell_profit_exit_retry_buffer_ticks,
        sell_risk_exit_buffer_ticks=cfg.sell_risk_exit_buffer_ticks,
        sell_risk_exit_retry_buffer_ticks=cfg.sell_risk_exit_retry_buffer_ticks,
        sell_force_exit_buffer_ticks=cfg.sell_force_exit_buffer_ticks,
        sell_force_exit_retry_buffer_ticks=cfg.sell_force_exit_retry_buffer_ticks,
    )
    fak_floor = bid_limit - strategy_floor_ticks * cfg.entry_tick_size
    slipped = bid_avg - cfg.sell_slippage_ticks * cfg.entry_tick_size
    return round(min(1.0, max(cfg.entry_tick_size, fak_floor, slipped)), 6)


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
    entry_polymarket_divergence_bps = _float(analysis.get("entry_polymarket_divergence_bps"))
    entry_favorable_gap_bps = _float(analysis.get("entry_favorable_gap_bps"))
    entry_reference_distance_bps = _float(analysis.get("entry_reference_distance_bps"))
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
        entry_polymarket_divergence_bps=entry_polymarket_divergence_bps,
        entry_favorable_gap_bps=entry_favorable_gap_bps,
        entry_reference_distance_bps=entry_reference_distance_bps,
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
        "partial_exits": [],
        "partial_pnl": 0.0,
    }
    return position, trade


def _event_exit_price(row: dict[str, Any]) -> float | None:
    return _float(row.get("exit_price")) or _float(_analysis(row).get("exit_price")) or _float(_order(row).get("avg_price"))


def _annotate_trade_settlement(trade: dict[str, Any], settlement: dict[str, Any]) -> None:
    winning_side = settlement.get("winning_side")
    if winning_side is None:
        return
    shares = float(trade.get("shares") or 0.0)
    entry_price = float(trade.get("entry_price") or 0.0)
    entry_side = trade.get("entry_side")
    settle_only_pnl = (1.0 - entry_price) * shares if entry_side == winning_side else -entry_price * shares
    trade["winning_side"] = winning_side
    trade["direction_correct"] = entry_side == winning_side
    trade["settle_only_pnl"] = round(settle_only_pnl, 6)
    trade["settlement_uncertain"] = bool(settlement.get("settlement_uncertain"))
    trade["settlement_price"] = settlement.get("settlement_price")
    trade["settlement_k_price"] = settlement.get("settlement_k_price")


def run_backtest(rows: Iterable[dict[str, Any]], config: BacktestConfig | None = None) -> BacktestResult:
    cfg = config or BacktestConfig()
    poly_cfg = cfg.poly_source_config()
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
        if cfg.compute_poly_returns:
            group = _with_computed_poly_returns(group, entry_start_age_sec=cfg.entry_start_age_sec)
        group_settlement = _settlement(group, boundary_usd=cfg.settlement_boundary_usd)
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
                    intent = str(row.get("order_intent") or row.get("exit_intent") or "order")
                    skip_reasons[f"{intent}_no_fill"] += 1
                    continue
                if not state.has_position:
                    event_entry = _event_entry(row, slug, cfg)
                    if event_entry is not None:
                        position, active_trade = event_entry
                        state.record_entry(position)
                        entries += 1
                        continue
                elif row.get("event") in {"exit", "partial_exit", "position_reduce"} and _order_success(row) and active_trade is not None:
                    exit_price = _event_exit_price(row)
                    if exit_price is not None:
                        reason = str(row.get("exit_reason") or _analysis(row).get("exit_reason") or (row.get("decision") or {}).get("reason") or "event_exit")
                        filled = _float(_order(row).get("filled_size"))
                        if row.get("event") in {"partial_exit", "position_reduce"} and filled is not None:
                            pnl, closed = state.record_partial_exit(exit_price, filled, reason, float(row.get("age_sec") or 0.0))
                            active_trade["partial_pnl"] = round(float(active_trade.get("partial_pnl") or 0.0) + pnl, 6)
                            active_trade.setdefault("partial_exits", []).append({
                                "age_sec": float(row.get("age_sec") or 0.0),
                                "reason": reason,
                                "price": exit_price,
                                "shares": filled,
                                "pnl": pnl,
                            })
                            if not closed:
                                continue
                        else:
                            pnl = state.record_exit(exit_price, reason, float(row.get("age_sec") or 0.0))
                        exit_age = float(row.get("age_sec") or 0.0)
                        total_pnl = round(float(active_trade.get("partial_pnl") or 0.0) + pnl, 6)
                        active_trade.update({
                            "exit_age_sec": exit_age,
                            "exit_reason": reason,
                            "exit_price": exit_price,
                            "pnl": total_pnl,
                            "hold_sec": exit_age - active_trade["entry_age_sec"],
                        })
                        _annotate_trade_settlement(active_trade, group_settlement)
                        equity += total_pnl
                        equity_points.append(equity)
                        trades.append(active_trade)
                        active_trade = None
                        exits += 1
                        continue
            if state.has_position and state.open_position is not None:
                decision = evaluate_poly_exit(snap, state.open_position, poly_cfg, state)
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
                    pnl = state.record_exit(exit_price, decision.reason, exit_age)
                    active_trade.update({
                        "exit_age_sec": exit_age,
                        "exit_reason": decision.reason,
                        "exit_price": exit_price,
                        "pnl": pnl,
                        "hold_sec": exit_age - active_trade["entry_age_sec"],
                        "poly_hold_score": decision.poly_hold_score,
                        "poly_hold_floor_bps": decision.poly_hold_floor_bps,
                        "poly_hold_reference_margin_bps": decision.poly_hold_reference_margin_bps,
                    })
                    _annotate_trade_settlement(active_trade, group_settlement)
                    equity += pnl
                    equity_points.append(equity)
                    trades.append(active_trade)
                    active_trade = None
                    exits += 1
            else:
                state.record_reference_baseline(snap)
                decision = evaluate_poly_entry(snap, state, poly_cfg)
                if decision.action == "skip":
                    skip_reasons[decision.reason] += 1
                if decision.action == "enter" and decision.side is not None and decision.price is not None and decision.edge is not None:
                    fill_price = _entry_fill_price(decision, cfg)
                    fill_snap = snap
                    if fill_price is None:
                        if index + 1 < len(group):
                            retry_snap = snapshot_from_row(group[index + 1])
                            retry_fill = _entry_fill_price_from_snapshot(decision, retry_snap, cfg)
                            if retry_fill is not None:
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
                        entry_model_prob=decision.model_prob if decision.model_prob is not None else 0.0,
                        entry_edge=decision.edge if decision.edge is not None else 0.0,
                        entry_polymarket_divergence_bps=decision.polymarket_divergence_bps,
                        entry_favorable_gap_bps=decision.favorable_gap_bps,
                        entry_reference_distance_bps=decision.entry_reference_distance_bps or decision.poly_reference_distance_bps,
                    ))
                    active_trade = {
                        "market_slug": slug,
                        "entry_side": decision.side,
                        "entry_phase": decision.phase,
                        "entry_age_sec": fill_snap.age_sec,
                        "entry_price": fill_price,
                        "entry_model_prob": decision.model_prob,
                        "entry_edge": decision.edge,
                        "entry_edge_at_fill": (decision.model_prob - fill_price) if decision.model_prob is not None else None,
                        "poly_entry_score": decision.poly_entry_score,
                        "poly_entry_distance_score": decision.poly_entry_distance_score,
                        "poly_entry_trend_score": decision.poly_entry_trend_score,
                        "poly_entry_price_quality_score": decision.poly_entry_price_quality_score,
                        "poly_entry_market_quality_score": decision.poly_entry_market_quality_score,
                        "poly_entry_overextended": decision.poly_entry_overextended,
                        "poly_reference_distance_bps": decision.poly_reference_distance_bps,
                        "poly_return_bps": decision.poly_return_bps,
                        "poly_trend_lookback_sec": decision.poly_trend_lookback_sec,
                        "poly_return_since_entry_start_bps": decision.poly_return_since_entry_start_bps,
                        "shares": shares,
                    }
                    entries += 1

        if state.has_position and state.open_position is not None and active_trade is not None:
            settlement = group_settlement
            winning_side = settlement.get("winning_side")
            if winning_side is not None:
                pnl = state.record_settlement(winning_side)
                total_pnl = round(float(active_trade.get("partial_pnl") or 0.0) + pnl, 6)
                is_uncertain = bool(settlement.get("settlement_uncertain"))
                active_trade.update({
                    "exit_age_sec": 300.0,
                    "exit_reason": "settlement",
                    "exit_price": 1.0 if active_trade["entry_side"] == winning_side else 0.0,
                    "pnl": total_pnl,
                    "hold_sec": 300.0 - active_trade["entry_age_sec"],
                    "winning_side": winning_side,
                    "settlement_uncertain": is_uncertain,
                    "settlement_price": settlement.get("settlement_price"),
                    "settlement_k_price": settlement.get("settlement_k_price"),
                })
                _annotate_trade_settlement(active_trade, settlement)
                equity += total_pnl
                equity_points.append(equity)
                trades.append(active_trade)
                settlements += 1
                if is_uncertain:
                    settlement_uncertain += 1
            else:
                pnl = state.record_exit(state.open_position.entry_avg_price, "unsettled_no_settlement_side", 300.0)
                total_pnl = round(float(active_trade.get("partial_pnl") or 0.0) + pnl, 6)
                active_trade.update({
                    "exit_age_sec": 300.0,
                    "exit_reason": "unsettled_no_settlement_side",
                    "exit_price": active_trade["entry_price"],
                    "pnl": total_pnl,
                    "hold_sec": 300.0 - active_trade["entry_age_sec"],
                    "settlement_uncertain": True,
                })
                equity += total_pnl
                equity_points.append(equity)
                trades.append(active_trade)
                unsettled += 1

    wins = sum(1 for trade in trades if trade.get("pnl", 0.0) > 0)
    total_pnl = round(sum(float(trade.get("pnl") or 0.0) for trade in trades), 6)
    settle_only_values = [float(trade["settle_only_pnl"]) for trade in trades if trade.get("settle_only_pnl") is not None]
    direction_known = [trade for trade in trades if trade.get("direction_correct") is not None]
    side_counts = Counter(str(trade.get("entry_side")) for trade in trades if trade.get("entry_side") in {"up", "down"})
    exit_reason_counts = Counter(str(trade.get("exit_reason")) for trade in trades if trade.get("exit_reason"))
    summary = {
        "strategy_mode": cfg.strategy_mode,
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
        "settle_only_total_pnl": round(sum(settle_only_values), 6) if settle_only_values else 0.0,
        "direction_accuracy": round(sum(1 for trade in direction_known if trade.get("direction_correct")) / len(direction_known), 4) if direction_known else 0.0,
        "side_counts": dict(sorted(side_counts.items())),
        "exit_reason_counts": dict(sorted(exit_reason_counts.items())),
        "max_drawdown": _max_drawdown(equity_points),
        "avg_hold_sec": round(sum(float(trade.get("hold_sec") or 0.0) for trade in trades) / len(trades), 2) if trades else 0.0,
        "skip_reason_counts": dict(sorted(skip_reasons.items(), key=lambda pair: (-pair[1], pair[0]))),
    }
    return BacktestResult(summary=summary, trades=trades)


def scan_poly_source_configs(
    rows: Iterable[dict[str, Any]],
    *,
    reference_distances: Iterable[float],
    max_reference_distances: Iterable[float] = (0.0,),
    trend_lookbacks: Iterable[float],
    return_thresholds: Iterable[float],
    max_entry_asks: Iterable[float],
    min_scores: Iterable[float],
    min_hold_scores: Iterable[float] = (0.0,),
    base_config: BacktestConfig | None = None,
    min_entries: int = 0,
    sort_by: str = "pnl",
) -> list[dict[str, Any]]:
    base = base_config or BacktestConfig()
    materialized: list[dict[str, Any]] = []
    for _slug, group in _group_rows(rows):
        materialized.extend(_with_computed_poly_returns(group, entry_start_age_sec=base.entry_start_age_sec))
    results: list[dict[str, Any]] = []
    for distance, max_distance, lookback, return_bps, max_ask, min_score, min_hold_score in itertools.product(
        reference_distances,
        max_reference_distances,
        trend_lookbacks,
        return_thresholds,
        max_entry_asks,
        min_scores,
        min_hold_scores,
    ):
        cfg = replace(
            base,
            compute_poly_returns=False,
            poly_reference_distance_bps=float(distance),
            max_poly_reference_distance_bps=float(max_distance),
            poly_trend_lookback_sec=float(lookback),
            poly_return_bps=float(return_bps),
            max_entry_ask=float(max_ask),
            min_poly_entry_score=float(min_score),
            min_poly_hold_score=float(min_hold_score),
        )
        result = run_backtest(materialized, cfg)
        if result.summary["entries"] < min_entries:
            continue
        results.append({
            "poly_reference_distance_bps": distance,
            "max_poly_reference_distance_bps": max_distance,
            "poly_trend_lookback_sec": lookback,
            "poly_return_bps": return_bps,
            "max_entry_ask": max_ask,
            "min_poly_entry_score": min_score,
            "min_poly_hold_score": min_hold_score,
            **result.summary,
        })
    if sort_by == "win_rate":
        key = lambda item: (item["win_rate"], item["direction_accuracy"], item["entries"], item["total_pnl"])
    elif sort_by == "direction_accuracy":
        key = lambda item: (item["direction_accuracy"], item["total_pnl"], item["entries"])
    else:
        key = lambda item: (item["total_pnl"], item["win_rate"], item["direction_accuracy"], item["entries"])
    return sorted(results, key=key, reverse=True)
