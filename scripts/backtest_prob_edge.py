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
    parser.add_argument("--early-required-edge", type=float, default=0.12)
    parser.add_argument("--core-required-edge", type=float, default=0.08)
    parser.add_argument("--entry-start-age-sec", type=float, default=90.0)
    parser.add_argument("--entry-end-age-sec", type=float, default=270.0)
    parser.add_argument("--max-entries-per-market", type=int, default=2)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--min-fair-cap-margin-ticks", type=float, default=0.0)
    parser.add_argument("--entry-tick-size", type=float, default=0.01)
    parser.add_argument("--slippage-ticks", type=float, default=0.0, help="Apply the same BUY/SELL slippage ticks")
    parser.add_argument("--buy-slippage-ticks", type=float)
    parser.add_argument("--sell-slippage-ticks", type=float)
    parser.add_argument("--sell-price-buffer-ticks", type=float, default=3.0)
    parser.add_argument("--sell-retry-price-buffer-ticks", type=float, default=5.0)
    parser.add_argument("--settlement-boundary-usd", type=float, default=5.0)
    parser.add_argument("--no-grid", action="store_true")
    parser.add_argument("--early-grid", default="0.08,0.10,0.12")
    parser.add_argument("--core-grid", default="0.04,0.05,0.06,0.07,0.08")
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
        entry_start_age_sec=args.entry_start_age_sec,
        entry_end_age_sec=args.entry_end_age_sec,
        max_entries_per_market=args.max_entries_per_market,
        tick_size=args.tick_size,
        buy_slippage_ticks=buy_slippage_ticks,
        sell_slippage_ticks=sell_slippage_ticks,
        sell_price_buffer_ticks=args.sell_price_buffer_ticks,
        sell_retry_price_buffer_ticks=args.sell_retry_price_buffer_ticks,
        settlement_boundary_usd=args.settlement_boundary_usd,
        min_fair_cap_margin_ticks=args.min_fair_cap_margin_ticks,
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
