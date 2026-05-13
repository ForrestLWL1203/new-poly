#!/usr/bin/env python3
"""Backtest the BTC 5m probability-edge strategy from collector JSONL."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.backtest.prob_edge_replay import BacktestConfig, run_backtest, scan_poly_source_configs


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
    parser.add_argument("--entry-start-age-sec", type=float, default=90.0)
    parser.add_argument("--entry-end-age-sec", type=float, default=270.0)
    parser.add_argument("--final-no-entry-remaining-sec", type=float, default=30.0)
    parser.add_argument("--max-entries-per-market", type=int, default=2)
    parser.add_argument("--entry-tick-size", type=float, default=0.01)
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
    parser.add_argument("--hold-to-settlement", dest="hold_to_settlement_enabled", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--hold-to-settlement-min-profit-ratio", type=float, default=2.0)
    parser.add_argument("--hold-to-settlement-min-bid-avg", type=float, default=0.80)
    parser.add_argument("--hold-to-settlement-min-bid-limit", type=float, default=0.75)
    parser.add_argument("--honor-order-events", action="store_true", help="For paper/live strategy JSONL, replay actual entry/exit/no-fill events instead of idealized fills.")
    parser.add_argument("--poly-reference-distance-bps", type=float, default=0.5)
    parser.add_argument("--max-poly-reference-distance-bps", type=float, default=0.0)
    parser.add_argument("--poly-trend-lookback-sec", type=float, default=3.0)
    parser.add_argument("--poly-return-bps", type=float, default=0.3)
    parser.add_argument("--max-entry-ask", type=float, default=0.65)
    parser.add_argument("--max-entry-fill-price", type=float, default=0.0)
    parser.add_argument("--min-poly-entry-score", type=float, default=0.0)
    parser.add_argument("--min-poly-hold-score", type=float, default=0.0)
    parser.add_argument("--poly-score-component-logs", choices=("compact", "full"), default="compact")
    parser.add_argument("--reference-distance-exit-remaining-sec", default="120,90,70,45,30")
    parser.add_argument("--reference-distance-exit-min-bps", default="-2,-1,0.25,0.75,1")
    parser.add_argument("--poly-exit-min-hold-sec", type=float, default=3.0)
    parser.add_argument("--poly-hold-to-settlement-min-reference-distance-bps", type=float, default=1.0)
    parser.add_argument("--poly-hold-to-settlement-min-poly-return-bps", type=float, default=0.0)
    parser.add_argument("--settlement-boundary-usd", type=float, default=5.0)
    parser.add_argument("--no-grid", action="store_true")
    parser.add_argument("--poly-reference-distance-grid", default="0.5,1.0,1.5,2.0,3.0,4.0")
    parser.add_argument("--max-poly-reference-distance-grid", default="0,3.5,4.0,5.0")
    parser.add_argument("--poly-trend-lookback-grid", default="1,3,5,10,15")
    parser.add_argument("--poly-return-grid", default="0.0,0.1,0.2,0.3,0.5")
    parser.add_argument("--max-entry-ask-grid", default="0.55,0.65,0.75,0.85")
    parser.add_argument("--min-poly-entry-score-grid", default="0.0,2.0,4.0,4.5,5.0")
    parser.add_argument("--min-poly-hold-score-grid", default="0.0")
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
        entry_start_age_sec=args.entry_start_age_sec,
        entry_end_age_sec=args.entry_end_age_sec,
        final_no_entry_remaining_sec=args.final_no_entry_remaining_sec,
        max_entries_per_market=args.max_entries_per_market,
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
        hold_to_settlement_enabled=args.hold_to_settlement_enabled,
        hold_to_settlement_min_profit_ratio=args.hold_to_settlement_min_profit_ratio,
        hold_to_settlement_min_bid_avg=args.hold_to_settlement_min_bid_avg,
        hold_to_settlement_min_bid_limit=args.hold_to_settlement_min_bid_limit,
        honor_order_events=args.honor_order_events,
        poly_reference_distance_bps=args.poly_reference_distance_bps,
        max_poly_reference_distance_bps=args.max_poly_reference_distance_bps,
        poly_trend_lookback_sec=args.poly_trend_lookback_sec,
        poly_return_bps=args.poly_return_bps,
        max_entry_ask=args.max_entry_ask,
        max_entry_fill_price=args.max_entry_fill_price,
        min_poly_entry_score=args.min_poly_entry_score,
        min_poly_hold_score=args.min_poly_hold_score,
        poly_score_component_logs=args.poly_score_component_logs,
        reference_distance_exit_remaining_sec=tuple(_float_list(args.reference_distance_exit_remaining_sec)),
        reference_distance_exit_min_bps=tuple(_float_list(args.reference_distance_exit_min_bps)),
        poly_exit_min_hold_sec=args.poly_exit_min_hold_sec,
        poly_hold_to_settlement_min_reference_distance_bps=args.poly_hold_to_settlement_min_reference_distance_bps,
        poly_hold_to_settlement_min_poly_return_bps=args.poly_hold_to_settlement_min_poly_return_bps,
        settlement_boundary_usd=args.settlement_boundary_usd,
        entry_tick_size=args.entry_tick_size,
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
        payload["grid_top"] = scan_poly_source_configs(
            rows,
            reference_distances=_float_list(args.poly_reference_distance_grid),
            max_reference_distances=_float_list(args.max_poly_reference_distance_grid),
            trend_lookbacks=_float_list(args.poly_trend_lookback_grid),
            return_thresholds=_float_list(args.poly_return_grid),
            max_entry_asks=_float_list(args.max_entry_ask_grid),
            min_scores=_float_list(args.min_poly_entry_score_grid),
            min_hold_scores=_float_list(args.min_poly_hold_score_grid),
            base_config=cfg,
            min_entries=max(0, args.grid_min_entries),
            sort_by=args.grid_sort_by,
        )[: max(0, args.top_n)]
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
