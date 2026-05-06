#!/usr/bin/env python3
"""Live data collector for BTC 5m probability-edge research."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.market.binance import BinancePriceFeed
from new_poly.market.coinbase import CoinbaseBtcPriceFeed
from new_poly.market.deribit import DvolSnapshot, fetch_dvol_snapshot
from new_poly.market.market import MarketWindow
from new_poly.market.prob_edge_data import (
    DEFAULT_MAX_DVOL_AGE_SEC,
    WindowPrices,
    compact_float,
    effective_price,
    find_following_window,
    find_initial_window,
    is_chainlink_btc_resolution,
    lead_delta,
    polymarket_open_disagrees,
    price_return_bps,
    refresh_binance_open,
    refresh_coinbase_open,
    refresh_k_price,
    refresh_polymarket_open,
    side_vs_k,
    token_state,
    window_bucket,
)
from new_poly.market.polymarket_live import PolymarketChainlinkBtcPriceFeed
from new_poly.market.series import MarketSeries
from new_poly.market.stream import PriceStream


class JsonlWriter:
    def __init__(self, path: Path | None) -> None:
        self.handle = None
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.handle = path.open("a", encoding="utf-8")

    def write(self, row: dict[str, Any]) -> None:
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
        print(line, flush=True)
        if self.handle is not None:
            self.handle.write(line + "\n")
            self.handle.flush()

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()


class WindowLimitTracker:
    def __init__(self, limit: int | None) -> None:
        self.limit = limit
        self.seen: list[str] = []

    def observe(self, slug: str, *, count: bool) -> bool:
        if self.limit is None or not count:
            return False
        if slug not in self.seen:
            self.seen.append(slug)
        return len(self.seen) > self.limit

    def reached(self) -> bool:
        return self.limit is not None and len(self.seen) >= self.limit


def build_row(
    *,
    window: MarketWindow,
    prices: WindowPrices,
    feed: BinancePriceFeed | None,
    coinbase_feed: CoinbaseBtcPriceFeed | None,
    polymarket_feed: PolymarketChainlinkBtcPriceFeed | None,
    stream: PriceStream,
    now: dt.datetime,
    depth_notional: float,
    depth_safety_multiplier: float,
    sigma_eff: float | None,
    sigma_source: str,
    volatility_stale: bool,
    paired_buffer: float,
    volatility: DvolSnapshot | None,
    coinbase_enabled: bool = True,
    polymarket_price_enabled: bool = True,
) -> dict[str, Any]:
    age_sec = (now - window.start_time).total_seconds()
    remaining_sec = (window.end_time - now).total_seconds()
    price = effective_price(
        feed,
        coinbase_feed,
        prices,
        coinbase_enabled=coinbase_enabled,
        polymarket_feed=polymarket_feed,
        polymarket_enabled=polymarket_price_enabled,
    )
    price_source, s_price, basis_bps = price.source, price.effective, price.basis_bps
    now_ts = now.timestamp()
    lead_binance_usd, lead_binance_bps = lead_delta(price.binance, price.polymarket)
    lead_coinbase_usd, lead_coinbase_bps = lead_delta(price.coinbase, price.polymarket)
    lead_proxy_usd, lead_proxy_bps = lead_delta(price.proxy, price.polymarket)
    lead_binance_side = side_vs_k(price.binance, prices.k_price)
    lead_coinbase_side = side_vs_k(price.coinbase, prices.k_price)
    lead_proxy_side = side_vs_k(price.proxy, prices.k_price)
    lead_polymarket_side = side_vs_k(price.polymarket, prices.k_price)
    good_resolution = is_chainlink_btc_resolution(window.resolution_source, window.description)
    up = token_state(stream, window.up_token, depth_notional, depth_safety_multiplier)
    down = token_state(stream, window.down_token, depth_notional, depth_safety_multiplier)
    ask_sum = up["ask_avg"] + down["ask_avg"] if up["ask_avg"] is not None and down["ask_avg"] is not None else None
    bid_sum = up["bid_avg"] + down["bid_avg"] if up["bid_avg"] is not None and down["bid_avg"] is not None else None
    warnings: list[str] = []
    if prices.k_timed_out:
        warnings.append("missing_k_timeout")
    elif prices.k_price is None:
        warnings.append("missing_k")
    if s_price is None:
        warnings.append("missing_effective_price")
    if up["book_age_ms"] is None or down["book_age_ms"] is None:
        warnings.append("missing_book")
    if up["bid"] is None or up["ask"] is None or down["bid"] is None or down["ask"] is None:
        warnings.append("one_sided_book")
    if not up["ask_depth_ok"] or not down["ask_depth_ok"]:
        warnings.append("depth_below_target_notional")
    if prices.binance_open_source == "rest_kline":
        warnings.append("binance_open_rest_fallback")
    if prices.coinbase_open_source == "rest_candle":
        warnings.append("coinbase_open_rest_fallback")
    ws_api_open_disagree = polymarket_open_disagrees(prices)
    if ws_api_open_disagree:
        warnings.append("polymarket_ws_open_disagrees_with_api")
    if polymarket_price_enabled and price.polymarket is None:
        warnings.append("missing_polymarket_reference")
    if not good_resolution:
        warnings.append("unexpected_resolution_source")
    row = {
        "ts": now.astimezone().isoformat(),
        "market_slug": window.slug,
        "window_start": window.start_time.isoformat(),
        "window_end": window.end_time.isoformat(),
        "age_sec": int(round(age_sec)),
        "remaining_sec": int(round(remaining_sec)),
        "window_bucket": window_bucket(age_sec, remaining_sec),
        "resolution_source": window.resolution_source,
        "settlement_aligned": bool(good_resolution and price.polymarket is not None and not ws_api_open_disagree),
        "sigma_source": sigma_source if sigma_eff is not None else "missing",
        "sigma_eff": compact_float(sigma_eff),
        "volatility_stale": volatility_stale,
        "volatility": volatility.to_json() if volatility is not None else None,
        "price_source": price_source,
        "s_price": compact_float(s_price, 2),
        "k_price": compact_float(prices.k_price, 2),
        "k_source": prices.k_source,
        "binance_price": compact_float(price.binance, 2),
        "coinbase_price": compact_float(price.coinbase, 2),
        "polymarket_price": compact_float(price.polymarket, 2),
        "polymarket_price_age_sec": compact_float(price.polymarket_age_sec, 3),
        "proxy_price": compact_float(price.proxy, 2),
        "polymarket_open_price": compact_float(prices.polymarket_open_price, 2),
        "polymarket_open_source": prices.polymarket_open_source,
        "polymarket_open_delta_ms": prices.polymarket_open_delta_ms,
        "binance_open_price": compact_float(prices.binance_open_price, 2),
        "binance_open_source": prices.binance_open_source,
        "binance_open_delta_ms": prices.binance_open_delta_ms,
        "coinbase_open_price": compact_float(prices.coinbase_open_price, 2),
        "coinbase_open_source": prices.coinbase_open_source,
        "coinbase_open_delta_ms": prices.coinbase_open_delta_ms,
        "proxy_open_price": compact_float(price.proxy_open, 2),
        "basis_bps": compact_float(basis_bps, 3),
        "source_spread_usd": compact_float(price.spread_usd, 2),
        "source_spread_bps": compact_float(price.spread_bps, 3),
        "lead_binance_vs_polymarket_usd": compact_float(lead_binance_usd, 2),
        "lead_binance_vs_polymarket_bps": compact_float(lead_binance_bps, 3),
        "polymarket_divergence_bps": compact_float(lead_proxy_bps if coinbase_enabled and lead_proxy_bps is not None else lead_binance_bps, 3),
        "lead_coinbase_vs_polymarket_usd": compact_float(lead_coinbase_usd, 2),
        "lead_coinbase_vs_polymarket_bps": compact_float(lead_coinbase_bps, 3),
        "lead_proxy_vs_polymarket_usd": compact_float(lead_proxy_usd, 2),
        "lead_proxy_vs_polymarket_bps": compact_float(lead_proxy_bps, 3),
        "lead_binance_return_1s_bps": compact_float(price_return_bps(feed, now_ts=now_ts, lookback_sec=1.0), 3),
        "lead_binance_return_3s_bps": compact_float(price_return_bps(feed, now_ts=now_ts, lookback_sec=3.0), 3),
        "lead_binance_return_5s_bps": compact_float(price_return_bps(feed, now_ts=now_ts, lookback_sec=5.0), 3),
        "lead_coinbase_return_1s_bps": compact_float(price_return_bps(coinbase_feed, now_ts=now_ts, lookback_sec=1.0), 3),
        "lead_coinbase_return_3s_bps": compact_float(price_return_bps(coinbase_feed, now_ts=now_ts, lookback_sec=3.0), 3),
        "lead_coinbase_return_5s_bps": compact_float(price_return_bps(coinbase_feed, now_ts=now_ts, lookback_sec=5.0), 3),
        "lead_polymarket_return_1s_bps": compact_float(price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=1.0), 3),
        "lead_polymarket_return_3s_bps": compact_float(price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=3.0), 3),
        "lead_polymarket_return_5s_bps": compact_float(price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=5.0), 3),
        "lead_binance_side": lead_binance_side,
        "lead_coinbase_side": lead_coinbase_side,
        "lead_proxy_side": lead_proxy_side,
        "lead_polymarket_side": lead_polymarket_side,
        "lead_binance_side_disagrees_with_polymarket": (
            lead_binance_side != lead_polymarket_side
            if lead_binance_side is not None and lead_polymarket_side is not None
            else None
        ),
        "lead_coinbase_side_disagrees_with_polymarket": (
            lead_coinbase_side != lead_polymarket_side
            if lead_coinbase_side is not None and lead_polymarket_side is not None
            else None
        ),
        "lead_proxy_side_disagrees_with_polymarket": (
            lead_proxy_side != lead_polymarket_side
            if lead_proxy_side is not None and lead_polymarket_side is not None
            else None
        ),
        "depth_notional": compact_float(depth_notional, 2),
        "depth_safety_multiplier": compact_float(depth_safety_multiplier, 3),
        "up": up,
        "down": down,
        "yes_no_sum": {
            "ask_sum": compact_float(ask_sum),
            "bid_sum": compact_float(bid_sum),
            "ask_arb": bool(ask_sum is not None and ask_sum < 1.0 - paired_buffer),
            "bid_lock": bool(bid_sum is not None and bid_sum > 1.0 + paired_buffer),
        },
    }
    if warnings:
        row["warnings"] = warnings
    return row


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emit compact JSONL data rows for BTC 5m probability-edge research.")
    parser.add_argument("--depth-notional", type=float, default=5.0)
    parser.add_argument("--depth-safety-multiplier", type=float, default=1.5)
    parser.add_argument("--order-notional", type=float, dest="depth_notional", help=argparse.SUPPRESS)
    parser.add_argument("--max-book-age-ms", type=int, default=1000)
    parser.add_argument("--jsonl", type=Path)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--paired-buffer", type=float, default=0.01)
    parser.add_argument("--sigma-eff", type=float, default=None)
    parser.add_argument("--sigma-source", default="missing")
    parser.add_argument("--collect-dvol", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--dvol-refresh-sec", type=float, default=0.0, help="Refresh Deribit DVOL every N seconds. Default 0 means fetch once at startup.")
    parser.add_argument("--max-dvol-age-sec", type=float, default=DEFAULT_MAX_DVOL_AGE_SEC)
    parser.add_argument("--warmup-timeout-sec", type=float, default=8.0)
    parser.add_argument("--windows", type=int, default=None)
    parser.add_argument("--include-current-window", action="store_true", help="Start from the in-progress window instead of waiting for the next full one.")
    parser.add_argument("--coinbase", dest="coinbase_enabled", action="store_true", default=False)
    parser.add_argument("--no-coinbase", dest="coinbase_enabled", action="store_false")
    parser.add_argument("--polymarket-price", dest="polymarket_price_enabled", action="store_true", default=True)
    parser.add_argument("--no-polymarket-price", dest="polymarket_price_enabled", action="store_false")
    parser.add_argument("--polymarket-stale-reconnect-sec", type=float, default=5.0)
    return parser


async def _noop_price_update(_update) -> None:
    return None


async def run(args: argparse.Namespace) -> int:
    writer = JsonlWriter(args.jsonl)
    feed: BinancePriceFeed | None = None
    coinbase_feed: CoinbaseBtcPriceFeed | None = None
    polymarket_feed = (
        PolymarketChainlinkBtcPriceFeed(max_history_sec=15.0, stale_reconnect_sec=args.polymarket_stale_reconnect_sec)
        if args.polymarket_price_enabled
        else None
    )
    series = MarketSeries.from_known("btc-updown-5m")
    stream = PriceStream(on_price=_noop_price_update)
    tracker = WindowLimitTracker(args.windows)
    volatility = await asyncio.to_thread(fetch_dvol_snapshot) if args.collect_dvol else None
    next_dvol_refresh = time.monotonic() + args.dvol_refresh_sec if args.collect_dvol and args.dvol_refresh_sec > 0 else None
    try:
        window = find_initial_window(series, include_current=args.include_current_window)
        prices = WindowPrices()
        feed = BinancePriceFeed("btcusdt")
        await feed.start()
        if polymarket_feed is not None:
            await polymarket_feed.start()
        if args.coinbase_enabled and coinbase_feed is None:
            coinbase_feed = CoinbaseBtcPriceFeed()
            await coinbase_feed.start()
        await stream.connect([window.up_token, window.down_token])
        first = True
        deadline = time.monotonic() + max(0.0, args.warmup_timeout_sec)
        while True:
            if first:
                while time.monotonic() < deadline:
                    if feed.latest_price is not None:
                        break
                    await asyncio.sleep(0.1)
                first = False
            now = dt.datetime.now(dt.timezone.utc)
            age_sec = (now - window.start_time).total_seconds()
            await refresh_k_price(window, prices, age_sec)
            if polymarket_feed is not None:
                await refresh_polymarket_open(polymarket_feed, window, prices, age_sec)
            if feed is not None:
                await refresh_binance_open(feed, window, prices, age_sec)
            if coinbase_feed is not None:
                await refresh_coinbase_open(coinbase_feed, window, prices, age_sec)
            if next_dvol_refresh is not None and time.monotonic() >= next_dvol_refresh:
                volatility = await asyncio.to_thread(fetch_dvol_snapshot)
                next_dvol_refresh = time.monotonic() + args.dvol_refresh_sec
            volatility_stale = (
                args.collect_dvol
                and volatility is not None
                and time.monotonic() - volatility.fetched_at > args.max_dvol_age_sec
            )
            sigma_eff = None if volatility_stale else args.sigma_eff
            sigma_source = args.sigma_source
            if args.collect_dvol and volatility is not None and sigma_eff is None and not volatility_stale:
                sigma_eff = volatility.sigma
                sigma_source = volatility.source
            if tracker.observe(window.slug, count=prices.k_price is not None):
                return 0
            row = build_row(
                window=window,
                prices=prices,
                feed=feed,
                coinbase_feed=coinbase_feed,
                polymarket_feed=polymarket_feed,
                stream=stream,
                now=now,
                depth_notional=args.depth_notional,
                depth_safety_multiplier=args.depth_safety_multiplier,
                sigma_eff=sigma_eff,
                sigma_source=sigma_source,
                volatility_stale=volatility_stale,
                paired_buffer=args.paired_buffer,
                volatility=volatility,
                coinbase_enabled=args.coinbase_enabled,
                polymarket_price_enabled=args.polymarket_price_enabled,
            )
            if args.verbose:
                row["tokens"] = {"up": window.up_token, "down": window.down_token}
            writer.write(row)
            if args.once:
                return 0
            await asyncio.sleep(args.interval_sec)
            if dt.datetime.now(dt.timezone.utc) >= window.end_time:
                if tracker.reached():
                    return 0
                window = find_following_window(window, series)
                prices = WindowPrices()
                await asyncio.wait_for(stream.switch_tokens([window.up_token, window.down_token]), timeout=8.0)
                first = True
                deadline = time.monotonic() + max(0.0, args.warmup_timeout_sec)
    except Exception as exc:
        writer.write({"ts": dt.datetime.now().astimezone().isoformat(), "error": str(exc), "settlement_aligned": False})
        return 1
    finally:
        closers = [stream.close()]
        if feed is not None:
            closers.append(feed.stop())
        if polymarket_feed is not None:
            closers.append(polymarket_feed.stop())
        if coinbase_feed is not None:
            closers.append(coinbase_feed.stop())
        for closer in closers:
            try:
                await asyncio.wait_for(closer, timeout=5.0)
            except Exception:
                pass
        writer.close()


def main() -> int:
    return asyncio.run(run(build_arg_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
