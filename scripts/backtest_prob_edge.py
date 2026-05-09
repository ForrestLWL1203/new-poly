#!/usr/bin/env python3
"""Backtest the BTC 5m probability-edge strategy from collector JSONL."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.backtest.prob_edge_replay import BacktestConfig, run_backtest, scan_configs


def _float_list(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _counts(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = str(item.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda pair: (-pair[1], pair[0])))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay probability-edge strategy from collector JSONL")
    parser.add_argument("--jsonl", type=Path, required=True)
    parser.add_argument("--amount-usd", type=float, default=5.0)
    parser.add_argument("--early-required-edge", type=float, default=0.16)
    parser.add_argument("--core-required-edge", type=float, default=0.14)
    parser.add_argument("--model-decay-buffer", type=float, default=0.03)
    parser.add_argument("--entry-start-age-sec", type=float, default=90.0)
    parser.add_argument("--entry-end-age-sec", type=float, default=270.0)
    parser.add_argument("--dynamic-entry", action="store_true")
    parser.add_argument("--fast-move-entry-start-age-sec", type=float, default=70.0)
    parser.add_argument("--fast-move-min-abs-sk-usd", type=float, default=80.0)
    parser.add_argument("--fast-move-required-edge", type=float, default=0.22)
    parser.add_argument("--strong-move-entry-start-age-sec", type=float, default=60.0)
    parser.add_argument("--strong-move-min-abs-sk-usd", type=float, default=120.0)
    parser.add_argument("--strong-move-required-edge", type=float, default=0.24)
    parser.add_argument("--max-entries-per-market", type=int, default=2)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--min-fair-cap-margin-ticks", type=float, default=0.0)
    parser.add_argument("--entry-tick-size", type=float, default=0.01)
    parser.add_argument("--min-entry-model-prob", type=float, default=0.0)
    parser.add_argument("--low-price-extra-edge-threshold", type=float, default=0.0)
    parser.add_argument("--low-price-extra-edge", type=float, default=0.0)
    parser.add_argument("--buy-cap-relax", dest="buy_cap_relax_enabled", action="store_true", default=False)
    parser.add_argument("--buy-low-price-relax-max-ask", type=float, default=0.25)
    parser.add_argument("--buy-low-price-relax-min-prob", type=float, default=0.40)
    parser.add_argument("--buy-low-price-relax-retained-edge", type=float, default=0.08)
    parser.add_argument("--buy-low-price-relax-max-extra-ticks", type=float, default=8.0)
    parser.add_argument("--buy-mid-price-relax-max-ask", type=float, default=0.65)
    parser.add_argument("--buy-mid-price-relax-min-prob", type=float, default=0.60)
    parser.add_argument("--buy-mid-price-relax-retained-edge", type=float, default=0.06)
    parser.add_argument("--buy-mid-price-relax-max-extra-ticks", type=float, default=8.0)
    parser.add_argument("--buy-mid-strong-relax-min-prob", type=float, default=0.75)
    parser.add_argument("--buy-mid-strong-relax-retained-edge", type=float, default=0.05)
    parser.add_argument("--buy-mid-strong-relax-max-extra-ticks", type=float, default=10.0)
    parser.add_argument("--buy-high-price-relax-min-ask", type=float, default=0.65)
    parser.add_argument("--buy-high-price-relax-min-prob", type=float, default=0.95)
    parser.add_argument("--buy-high-price-relax-retained-edge", type=float, default=0.08)
    parser.add_argument("--buy-high-price-relax-max-extra-ticks", type=float, default=4.0)
    parser.add_argument("--slippage-ticks", type=float, default=0.0, help="Apply the same BUY/SELL slippage ticks")
    parser.add_argument("--buy-slippage-ticks", type=float)
    parser.add_argument("--sell-slippage-ticks", type=float)
    parser.add_argument("--sell-price-buffer-ticks", type=float, default=5.0)
    parser.add_argument("--sell-retry-price-buffer-ticks", type=float, default=8.0)
    parser.add_argument("--no-sell-dynamic-buffer", dest="sell_dynamic_buffer_enabled", action="store_false", default=True)
    parser.add_argument("--sell-profit-exit-buffer-ticks", type=float, default=5.0)
    parser.add_argument("--sell-profit-exit-retry-buffer-ticks", type=float, default=8.0)
    parser.add_argument("--sell-risk-exit-buffer-ticks", type=float, default=8.0)
    parser.add_argument("--sell-risk-exit-retry-buffer-ticks", type=float, default=12.0)
    parser.add_argument("--sell-force-exit-buffer-ticks", type=float, default=10.0)
    parser.add_argument("--sell-force-exit-retry-buffer-ticks", type=float, default=15.0)
    parser.add_argument("--prob-drop-exit-window-sec", type=float, default=0.0)
    parser.add_argument("--prob-drop-exit-threshold", type=float, default=0.0)
    parser.add_argument("--final-force-exit-remaining-sec", type=float, default=30.0)
    parser.add_argument("--defensive-take-profit", dest="defensive_take_profit_enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--cross-source-max-bps", type=float, default=0.0)
    parser.add_argument("--market-disagrees-exit-threshold", type=float, default=0.0)
    parser.add_argument("--low-price-market-disagrees-entry-threshold", type=float, default=0.0)
    parser.add_argument("--low-price-market-disagrees-exit-threshold", type=float, default=0.0)
    parser.add_argument("--market-disagrees-exit-max-remaining-sec", type=float, default=0.0)
    parser.add_argument("--market-disagrees-exit-min-loss", type=float, default=0.0)
    parser.add_argument("--market-disagrees-exit-min-age-sec", type=float, default=0.0)
    parser.add_argument("--market-disagrees-exit-max-profit", type=float, default=0.01)
    parser.add_argument("--polymarket-divergence-exit-bps", type=float, default=3.0)
    parser.add_argument("--polymarket-divergence-exit-min-age-sec", type=float, default=3.0)
    parser.add_argument("--honor-order-events", action="store_true", help="For paper/live strategy JSONL, replay actual entry/exit/no-fill events instead of idealized fills.")
    parser.add_argument("--settlement-boundary-usd", type=float, default=5.0)
    parser.add_argument("--no-grid", action="store_true")
    parser.add_argument("--early-grid", default="0.14,0.16,0.18")
    parser.add_argument("--core-grid", default="0.12,0.14,0.16")
    parser.add_argument("--entry-start-grid", default="25,40,60")
    parser.add_argument("--entry-end-grid", default="210,240,270")
    parser.add_argument("--grid-min-entries", type=int, default=0)
    parser.add_argument("--grid-sort-by", choices=("pnl", "win_rate", "avg_pnl"), default="pnl")
    parser.add_argument("--top-n", type=int, default=10)
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    rows = load_rows(args.jsonl)
    buy_slippage_ticks = args.slippage_ticks if args.buy_slippage_ticks is None else args.buy_slippage_ticks
    sell_slippage_ticks = args.slippage_ticks if args.sell_slippage_ticks is None else args.sell_slippage_ticks
    cfg = BacktestConfig(
        amount_usd=args.amount_usd,
        early_required_edge=args.early_required_edge,
        core_required_edge=args.core_required_edge,
        model_decay_buffer=args.model_decay_buffer,
        entry_start_age_sec=args.entry_start_age_sec,
        entry_end_age_sec=args.entry_end_age_sec,
        dynamic_entry_enabled=args.dynamic_entry,
        fast_move_entry_start_age_sec=args.fast_move_entry_start_age_sec,
        fast_move_min_abs_sk_usd=args.fast_move_min_abs_sk_usd,
        fast_move_required_edge=args.fast_move_required_edge,
        strong_move_entry_start_age_sec=args.strong_move_entry_start_age_sec,
        strong_move_min_abs_sk_usd=args.strong_move_min_abs_sk_usd,
        strong_move_required_edge=args.strong_move_required_edge,
        max_entries_per_market=args.max_entries_per_market,
        tick_size=args.tick_size,
        buy_slippage_ticks=buy_slippage_ticks,
        sell_slippage_ticks=sell_slippage_ticks,
        sell_price_buffer_ticks=args.sell_price_buffer_ticks,
        sell_retry_price_buffer_ticks=args.sell_retry_price_buffer_ticks,
        sell_dynamic_buffer_enabled=args.sell_dynamic_buffer_enabled,
        sell_profit_exit_buffer_ticks=args.sell_profit_exit_buffer_ticks,
        sell_profit_exit_retry_buffer_ticks=args.sell_profit_exit_retry_buffer_ticks,
        sell_risk_exit_buffer_ticks=args.sell_risk_exit_buffer_ticks,
        sell_risk_exit_retry_buffer_ticks=args.sell_risk_exit_retry_buffer_ticks,
        sell_force_exit_buffer_ticks=args.sell_force_exit_buffer_ticks,
        sell_force_exit_retry_buffer_ticks=args.sell_force_exit_retry_buffer_ticks,
        prob_drop_exit_window_sec=args.prob_drop_exit_window_sec,
        prob_drop_exit_threshold=args.prob_drop_exit_threshold,
        final_force_exit_remaining_sec=args.final_force_exit_remaining_sec,
        defensive_take_profit_enabled=args.defensive_take_profit_enabled,
        cross_source_max_bps=args.cross_source_max_bps,
        market_disagrees_exit_threshold=args.market_disagrees_exit_threshold,
        low_price_market_disagrees_entry_threshold=args.low_price_market_disagrees_entry_threshold,
        low_price_market_disagrees_exit_threshold=args.low_price_market_disagrees_exit_threshold,
        market_disagrees_exit_max_remaining_sec=args.market_disagrees_exit_max_remaining_sec,
        market_disagrees_exit_min_loss=args.market_disagrees_exit_min_loss,
        market_disagrees_exit_min_age_sec=args.market_disagrees_exit_min_age_sec,
        market_disagrees_exit_max_profit=args.market_disagrees_exit_max_profit,
        polymarket_divergence_exit_bps=args.polymarket_divergence_exit_bps,
        polymarket_divergence_exit_min_age_sec=args.polymarket_divergence_exit_min_age_sec,
        honor_order_events=args.honor_order_events,
        settlement_boundary_usd=args.settlement_boundary_usd,
        min_fair_cap_margin_ticks=args.min_fair_cap_margin_ticks,
        entry_tick_size=args.entry_tick_size,
        min_entry_model_prob=args.min_entry_model_prob,
        low_price_extra_edge_threshold=args.low_price_extra_edge_threshold,
        low_price_extra_edge=args.low_price_extra_edge,
        buy_cap_relax_enabled=args.buy_cap_relax_enabled,
        buy_low_price_relax_max_ask=args.buy_low_price_relax_max_ask,
        buy_low_price_relax_min_prob=args.buy_low_price_relax_min_prob,
        buy_low_price_relax_retained_edge=args.buy_low_price_relax_retained_edge,
        buy_low_price_relax_max_extra_ticks=args.buy_low_price_relax_max_extra_ticks,
        buy_mid_price_relax_max_ask=args.buy_mid_price_relax_max_ask,
        buy_mid_price_relax_min_prob=args.buy_mid_price_relax_min_prob,
        buy_mid_price_relax_retained_edge=args.buy_mid_price_relax_retained_edge,
        buy_mid_price_relax_max_extra_ticks=args.buy_mid_price_relax_max_extra_ticks,
        buy_mid_strong_relax_min_prob=args.buy_mid_strong_relax_min_prob,
        buy_mid_strong_relax_retained_edge=args.buy_mid_strong_relax_retained_edge,
        buy_mid_strong_relax_max_extra_ticks=args.buy_mid_strong_relax_max_extra_ticks,
        buy_high_price_relax_min_ask=args.buy_high_price_relax_min_ask,
        buy_high_price_relax_min_prob=args.buy_high_price_relax_min_prob,
        buy_high_price_relax_retained_edge=args.buy_high_price_relax_retained_edge,
        buy_high_price_relax_max_extra_ticks=args.buy_high_price_relax_max_extra_ticks,
    )
    result = run_backtest(rows, cfg)
    payload: dict[str, Any] = {
        "source": str(args.jsonl),
        "default_config": cfg.__dict__,
        "summary": result.summary,
        "exit_reasons": _counts(result.trades, "exit_reason"),
        "entry_phases": _counts(result.trades, "entry_phase"),
        "entry_sides": _counts(result.trades, "entry_side"),
        "sample_trades": result.trades[:5],
    }
    if not args.no_grid:
        payload["grid_top"] = scan_configs(
            rows,
            early_edges=_float_list(args.early_grid),
            core_edges=_float_list(args.core_grid),
            entry_starts=_float_list(args.entry_start_grid),
            entry_ends=_float_list(args.entry_end_grid),
            base_config=cfg,
            min_entries=max(0, args.grid_min_entries),
            sort_by=args.grid_sort_by,
        )[: max(0, args.top_n)]
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
