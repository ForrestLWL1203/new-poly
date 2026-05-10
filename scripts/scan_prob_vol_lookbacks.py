#!/usr/bin/env python3
"""Scan Binance RV lookbacks against BTC 5m probability calibration."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from new_poly.backtest.volatility_calibration import (  # noqa: E402
    CalibrationConfig,
    evaluate_lookbacks,
    parse_row_time,
)


BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"


def _load_jsonl(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
    return rows


def _parse_lookbacks(text: str) -> list[int]:
    values = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        value = int(part)
        if value <= 0:
            raise argparse.ArgumentTypeError("lookbacks must be positive minutes")
        values.append(value)
    if not values:
        raise argparse.ArgumentTypeError("at least one lookback is required")
    return sorted(set(values))


def _request_klines(start_ms: int, end_ms: int, *, symbol: str, timeout_sec: float) -> list[list[Any]]:
    url = BINANCE_KLINES_URL + "?" + urllib.parse.urlencode({
        "symbol": symbol.upper(),
        "interval": "1m",
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000,
    })
    req = urllib.request.Request(url, headers={"User-Agent": "new-poly/0.1"})
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError(f"unexpected Binance kline response: {payload!r}")
    return payload


def fetch_historical_klines(
    *,
    start_ms: int,
    end_ms: int,
    symbol: str = "BTCUSDT",
    timeout_sec: float = 10.0,
    sleep_sec: float = 0.05,
) -> list[list[Any]]:
    rows: list[list[Any]] = []
    cursor = start_ms
    while cursor <= end_ms:
        chunk = _request_klines(cursor, end_ms, symbol=symbol, timeout_sec=timeout_sec)
        if not chunk:
            break
        rows.extend(chunk)
        try:
            last_open_ms = int(chunk[-1][0])
        except (TypeError, ValueError, IndexError) as exc:
            raise RuntimeError("Binance kline row missing open time") from exc
        next_cursor = last_open_ms + 60_000
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        time.sleep(max(0.0, sleep_sec))
    dedup: dict[int, list[Any]] = {}
    for row in rows:
        try:
            dedup[int(row[0])] = row
        except (TypeError, ValueError, IndexError):
            continue
    return [dedup[key] for key in sorted(dedup)]


def _time_bounds_ms(rows: list[dict[str, Any]], *, max_lookback_minutes: int) -> tuple[int, int]:
    timestamps = [ts for row in rows if (ts := parse_row_time(row)) is not None]
    if not timestamps:
        raise RuntimeError("no parseable row ts values found")
    start_sec = min(timestamps) - (max_lookback_minutes + 2) * 60
    end_sec = max(timestamps)
    return int(start_sec * 1000), int(end_sec * 1000)


def _print_summary(results: dict[int, dict[str, Any]]) -> None:
    ranked = sorted(
        results.items(),
        key=lambda item: (
            item[1]["brier"] if item[1]["brier"] is not None else float("inf"),
            item[1]["log_loss"] if item[1]["log_loss"] is not None else float("inf"),
        ),
    )
    print("lookback_min samples brier log_loss mean_pred outcome_up_rate missing_sigma")
    for lookback, metrics in ranked:
        print(
            f"{lookback:>12} "
            f"{metrics['samples']:>7} "
            f"{metrics['brier']!s:>6} "
            f"{metrics['log_loss']!s:>8} "
            f"{metrics['mean_pred']!s:>9} "
            f"{metrics['outcome_up_rate']!s:>15} "
            f"{metrics['missing_sigma']:>13}"
        )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Calibrate BTC 5m probability formula across Binance RV lookbacks")
    parser.add_argument("jsonl", nargs="+", type=Path, help="Collector/paper/live JSONL files")
    parser.add_argument("--lookbacks", type=_parse_lookbacks, default=_parse_lookbacks("1,5,10,30,60,120"))
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--out", type=Path, default=None, help="Optional JSON output path")
    parser.add_argument("--klines-json", type=Path, default=None, help="Use cached Binance kline JSON instead of fetching")
    parser.add_argument("--save-klines-json", type=Path, default=None, help="Save fetched Binance kline JSON")
    parser.add_argument("--min-age-sec", type=float, default=None)
    parser.add_argument("--max-age-sec", type=float, default=None)
    parser.add_argument("--rv-ewma-half-life-minutes", type=float, default=10.0)
    parser.add_argument("--rv-floor-annual", type=float, default=0.20)
    parser.add_argument("--rv-cap-annual", type=float, default=2.50)
    parser.add_argument("--timeout-sec", type=float, default=10.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    rows = _load_jsonl(args.jsonl)
    if not rows:
        raise SystemExit("no JSONL rows loaded")

    if args.klines_json is not None:
        with args.klines_json.open("r", encoding="utf-8") as handle:
            klines = json.load(handle)
    else:
        start_ms, end_ms = _time_bounds_ms(rows, max_lookback_minutes=max(args.lookbacks))
        klines = fetch_historical_klines(
            start_ms=start_ms,
            end_ms=end_ms,
            symbol=args.symbol,
            timeout_sec=args.timeout_sec,
        )
        if args.save_klines_json is not None:
            args.save_klines_json.parent.mkdir(parents=True, exist_ok=True)
            with args.save_klines_json.open("w", encoding="utf-8") as handle:
                json.dump(klines, handle)

    config = CalibrationConfig(
        ewma_half_life_minutes=args.rv_ewma_half_life_minutes,
        floor_annual=args.rv_floor_annual,
        cap_annual=args.rv_cap_annual,
        min_age_sec=args.min_age_sec,
        max_age_sec=args.max_age_sec,
    )
    results = evaluate_lookbacks(rows, klines, args.lookbacks, config=config)
    payload = {
        "inputs": [str(path) for path in args.jsonl],
        "lookbacks": args.lookbacks,
        "config": {
            "rv_ewma_half_life_minutes": args.rv_ewma_half_life_minutes,
            "rv_floor_annual": args.rv_floor_annual,
            "rv_cap_annual": args.rv_cap_annual,
            "min_age_sec": args.min_age_sec,
            "max_age_sec": args.max_age_sec,
        },
        "results": {str(lookback): metrics for lookback, metrics in results.items()},
    }

    _print_summary(results)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
