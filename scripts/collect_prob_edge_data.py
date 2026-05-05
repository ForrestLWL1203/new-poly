#!/usr/bin/env python3
"""Live data collector for BTC 5m probability-edge research."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import math
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.market.binance import BinancePriceFeed
from new_poly.market.coinbase import CoinbaseBtcPriceFeed
from new_poly.market.deribit import DvolSnapshot, fetch_dvol_snapshot
from new_poly.market.market import MarketWindow, find_next_window, find_window_after
from new_poly.market.polymarket_live import PolymarketChainlinkBtcPriceFeed
from new_poly.market.series import MarketSeries
from new_poly.market.stream import PriceStream

POLYMARKET_CRYPTO_PRICE_API = "https://polymarket.com/api/crypto/crypto-price"
K_RETRY_AGES_SEC = (5.0, 8.0, 12.0, 20.0, 30.0, 40.0)
K_RETRY_TIMEOUT_SEC = 40.0
BTC_OPEN_LOOKAROUND_SEC = 5.0
DEFAULT_MAX_DVOL_AGE_SEC = 900.0


@dataclass
class WindowPrices:
    k_price: float | None = None
    k_source: str = "missing"
    k_timed_out: bool = False
    attempted_slots: set[float] | None = None
    binance_open_price: float | None = None
    binance_open_source: str = "missing"
    binance_open_delta_ms: int | None = None
    binance_open_rest_attempted: bool = False
    coinbase_open_price: float | None = None
    coinbase_open_source: str = "missing"
    coinbase_open_delta_ms: int | None = None
    coinbase_open_rest_attempted: bool = False
    polymarket_open_price: float | None = None
    polymarket_open_source: str = "missing"
    polymarket_open_delta_ms: int | None = None

    def __post_init__(self) -> None:
        if self.attempted_slots is None:
            self.attempted_slots = set()


@dataclass(frozen=True)
class EffectivePrice:
    source: str
    effective: float | None
    basis_bps: float | None
    proxy: float | None = None
    proxy_open: float | None = None
    binance: float | None = None
    coinbase: float | None = None
    polymarket: float | None = None
    polymarket_open: float | None = None
    polymarket_age_sec: float | None = None
    spread_usd: float | None = None
    spread_bps: float | None = None


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


def iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_float(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(float(value)):
        return None
    return round(float(value), digits)


def window_bucket(age_sec: float, remaining_sec: float) -> str:
    if remaining_sec <= 0:
        return "closed"
    if age_sec < 25:
        return "warmup"
    if age_sec < 120:
        return "early"
    if age_sec < 240:
        return "core"
    if age_sec < 270:
        return "late"
    return "no_entry"


def is_chainlink_btc_resolution(*values: str | None) -> bool:
    text = " ".join(str(value or "").lower() for value in values)
    return ("chainlink" in text or "chain.link" in text) and ("btc" in text or "bitcoin" in text) and "usd" in text


def extract_crypto_prices_from_api_response(data: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(data, dict) or data.get("openPrice") is None:
        return None
    try:
        return {
            "openPrice": float(data["openPrice"]),
            "completed": bool(data.get("completed")),
            "incomplete": bool(data.get("incomplete")),
            "cached": bool(data.get("cached")),
        }
    except (TypeError, ValueError):
        return None


def crypto_price_api_url(window: MarketWindow) -> str:
    return POLYMARKET_CRYPTO_PRICE_API + "?" + urllib.parse.urlencode({
        "symbol": "BTC",
        "eventStartTime": iso_z(window.start_time),
        "variant": "fiveminute",
        "endDate": iso_z(window.end_time),
    })


def fetch_crypto_price_api(window: MarketWindow) -> dict[str, Any] | None:
    try:
        req = urllib.request.Request(
            crypto_price_api_url(window),
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None
    return extract_crypto_prices_from_api_response(raw)


async def refresh_k_price(window: MarketWindow, prices: WindowPrices, age_sec: float) -> None:
    if prices.k_price is not None or prices.k_timed_out:
        return
    assert prices.attempted_slots is not None
    eligible = [slot for slot in K_RETRY_AGES_SEC if slot <= age_sec and slot not in prices.attempted_slots]
    if not eligible:
        if age_sec > K_RETRY_TIMEOUT_SEC and K_RETRY_TIMEOUT_SEC in prices.attempted_slots:
            prices.k_timed_out = True
        return
    prices.attempted_slots.add(max(eligible))
    api_data = await asyncio.to_thread(fetch_crypto_price_api, window)
    if api_data is not None:
        prices.k_price = api_data["openPrice"]
        prices.k_source = "polymarket_crypto_price_api"
        return


async def refresh_binance_open(feed: BinancePriceFeed, window: MarketWindow, prices: WindowPrices, age_sec: float) -> None:
    if prices.binance_open_price is not None:
        return
    if age_sec < -BTC_OPEN_LOOKAROUND_SEC:
        return
    start = float(window.start_epoch)
    first_after = feed.first_price_at_or_after(start, max_forward_sec=BTC_OPEN_LOOKAROUND_SEC)
    if first_after is not None:
        prices.binance_open_price = first_after
        prices.binance_open_source = "ws_first_after"
        prices.binance_open_delta_ms = None
        return
    last_before = feed.price_at_or_before(start, max_backward_sec=BTC_OPEN_LOOKAROUND_SEC)
    if last_before is not None:
        prices.binance_open_price = last_before
        prices.binance_open_source = "ws_last_before"
        prices.binance_open_delta_ms = None
        return
    if age_sec >= 10.0 and not prices.binance_open_rest_attempted:
        prices.binance_open_rest_attempted = True
        rest_open = await feed.fetch_open_at(start)
        if rest_open is not None:
            prices.binance_open_price = rest_open
            prices.binance_open_source = "rest_kline"


async def refresh_coinbase_open(feed: CoinbaseBtcPriceFeed, window: MarketWindow, prices: WindowPrices, age_sec: float) -> None:
    if prices.coinbase_open_price is not None:
        return
    if age_sec < -BTC_OPEN_LOOKAROUND_SEC:
        return
    start = float(window.start_epoch)
    first_after = feed.first_price_at_or_after(start, max_forward_sec=BTC_OPEN_LOOKAROUND_SEC)
    if first_after is not None:
        prices.coinbase_open_price = first_after
        prices.coinbase_open_source = "ws_first_after"
        prices.coinbase_open_delta_ms = None
        return
    last_before = feed.price_at_or_before(start, max_backward_sec=BTC_OPEN_LOOKAROUND_SEC)
    if last_before is not None:
        prices.coinbase_open_price = last_before
        prices.coinbase_open_source = "ws_last_before"
        prices.coinbase_open_delta_ms = None
        return
    if age_sec >= 10.0 and not prices.coinbase_open_rest_attempted:
        prices.coinbase_open_rest_attempted = True
        rest_open = await feed.fetch_open_at(start)
        if rest_open is not None:
            prices.coinbase_open_price = rest_open
            prices.coinbase_open_source = "rest_candle"


async def refresh_polymarket_open(feed: PolymarketChainlinkBtcPriceFeed, window: MarketWindow, prices: WindowPrices, age_sec: float) -> None:
    if prices.polymarket_open_price is not None:
        return
    if age_sec < -BTC_OPEN_LOOKAROUND_SEC:
        return
    start = float(window.start_epoch)
    first_after = feed.first_price_at_or_after(start, max_forward_sec=BTC_OPEN_LOOKAROUND_SEC)
    if first_after is not None:
        prices.polymarket_open_price = first_after
        prices.polymarket_open_source = "ws_first_after"
        prices.polymarket_open_delta_ms = None
        return
    last_before = feed.price_at_or_before(start, max_backward_sec=BTC_OPEN_LOOKAROUND_SEC)
    if last_before is not None:
        prices.polymarket_open_price = last_before
        prices.polymarket_open_source = "ws_last_before"
        prices.polymarket_open_delta_ms = None


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _source_name(*, has_binance: bool, has_coinbase: bool, basis_adjusted: bool) -> str:
    if has_binance and has_coinbase:
        return "proxy_multi_source_basis_adjusted" if basis_adjusted else "proxy_multi_source"
    if has_binance:
        return "proxy_binance_basis_adjusted" if basis_adjusted else "proxy_binance"
    if has_coinbase:
        return "proxy_coinbase_basis_adjusted" if basis_adjusted else "proxy_coinbase"
    return "missing"


def effective_price(
    feed: BinancePriceFeed | None,
    coinbase_feed: CoinbaseBtcPriceFeed | None,
    prices: WindowPrices,
    *,
    coinbase_enabled: bool = True,
    polymarket_feed: PolymarketChainlinkBtcPriceFeed | None = None,
    polymarket_enabled: bool = True,
    max_polymarket_age_sec: float = 3.0,
) -> EffectivePrice:
    binance_latest = feed.latest_price if feed is not None else None
    coinbase_latest = coinbase_feed.latest_price if coinbase_enabled and coinbase_feed is not None else None
    all_latest_values = [value for value in (binance_latest, coinbase_latest) if value is not None]
    all_proxy = _mean(all_latest_values)
    polymarket_latest = polymarket_feed.latest_price if polymarket_enabled and polymarket_feed is not None else None
    polymarket_age = (
        polymarket_feed.latest_age_sec()
        if polymarket_enabled and polymarket_feed is not None and hasattr(polymarket_feed, "latest_age_sec")
        else None
    )
    polymarket_fresh = (
        polymarket_latest is not None
        and (polymarket_age is None or polymarket_age <= max_polymarket_age_sec)
    )

    def _with_polymarket(base: EffectivePrice) -> EffectivePrice:
        return EffectivePrice(
            base.source,
            base.effective,
            base.basis_bps,
            proxy=base.proxy,
            proxy_open=base.proxy_open,
            binance=base.binance,
            coinbase=base.coinbase,
            polymarket=polymarket_latest,
            polymarket_open=prices.polymarket_open_price,
            polymarket_age_sec=polymarket_age,
            spread_usd=base.spread_usd,
            spread_bps=base.spread_bps,
        )

    if all_proxy is None:
        if polymarket_fresh:
            basis_bps = (
                ((prices.polymarket_open_price - prices.k_price) / prices.k_price) * 10_000.0
                if prices.k_price is not None and prices.polymarket_open_price is not None
                else None
            )
            return EffectivePrice(
                "polymarket_chainlink",
                polymarket_latest,
                basis_bps,
                polymarket=polymarket_latest,
                polymarket_open=prices.polymarket_open_price,
                polymarket_age_sec=polymarket_age,
            )
        return EffectivePrice(
            "missing",
            None,
            None,
            binance=binance_latest,
            coinbase=coinbase_latest,
            polymarket=polymarket_latest,
            polymarket_open=prices.polymarket_open_price,
            polymarket_age_sec=polymarket_age,
        )

    paired_sources = [
        (binance_latest, prices.binance_open_price),
        (coinbase_latest, prices.coinbase_open_price if coinbase_enabled else None),
    ]
    basis_pairs = [(latest, open_price) for latest, open_price in paired_sources if latest is not None and open_price is not None]
    has_paired_binance = binance_latest is not None and prices.binance_open_price is not None
    has_paired_coinbase = coinbase_enabled and coinbase_latest is not None and prices.coinbase_open_price is not None
    spread_usd = abs(binance_latest - coinbase_latest) if binance_latest is not None and coinbase_latest is not None else None
    spread_bps = (spread_usd / all_proxy) * 10_000.0 if spread_usd is not None and all_proxy else None

    if polymarket_fresh:
        basis_bps = (
            ((prices.polymarket_open_price - prices.k_price) / prices.k_price) * 10_000.0
            if prices.k_price is not None and prices.polymarket_open_price is not None
            else None
        )
        return EffectivePrice(
            "polymarket_chainlink",
            polymarket_latest,
            basis_bps,
            proxy=all_proxy,
            proxy_open=_mean([open_price for _latest, open_price in basis_pairs]) if basis_pairs else None,
            binance=binance_latest,
            coinbase=coinbase_latest,
            polymarket=polymarket_latest,
            polymarket_open=prices.polymarket_open_price,
            polymarket_age_sec=polymarket_age,
            spread_usd=spread_usd,
            spread_bps=spread_bps,
        )

    if prices.k_price is not None and basis_pairs:
        proxy = _mean([latest for latest, _open in basis_pairs])
        proxy_open = _mean([open_price for _latest, open_price in basis_pairs])
        assert proxy is not None
        assert proxy_open is not None
        basis = proxy_open - prices.k_price
        effective = proxy - basis
        return _with_polymarket(EffectivePrice(
            _source_name(has_binance=has_paired_binance, has_coinbase=has_paired_coinbase, basis_adjusted=True),
            effective,
            (basis / prices.k_price) * 10_000.0,
            proxy=proxy,
            proxy_open=proxy_open,
            binance=binance_latest,
            coinbase=coinbase_latest,
            spread_usd=spread_usd,
            spread_bps=spread_bps,
        ))
    return _with_polymarket(EffectivePrice(
        _source_name(has_binance=binance_latest is not None, has_coinbase=coinbase_latest is not None, basis_adjusted=False),
        all_proxy,
        None,
        proxy=all_proxy,
        proxy_open=None,
        binance=binance_latest,
        coinbase=coinbase_latest,
        spread_usd=spread_usd,
        spread_bps=spread_bps,
    ))


def avg_price_for_notional(levels: list[tuple[float, float]], target_notional: float) -> tuple[float | None, bool, float, float | None]:
    shares = 0.0
    notional = 0.0
    limit_price = None
    for price, size in levels:
        if price <= 0 or size <= 0:
            continue
        take_shares = min(size, max(0.0, target_notional - notional) / price)
        shares += take_shares
        notional += take_shares * price
        if take_shares > 0:
            limit_price = price
        if notional >= target_notional - 1e-9:
            break
    avg = notional / shares if shares > 0 else None
    return compact_float(avg), notional >= target_notional - 1e-9, notional, compact_float(limit_price)


def token_state(stream: PriceStream, token_id: str, depth_notional: float, depth_safety_multiplier: float = 1.0) -> dict[str, Any]:
    asks = stream.get_latest_ask_levels_with_size(token_id)
    bids = stream.get_latest_bid_levels_with_size(token_id)
    ask_avg, ask_ok, _, ask_limit = avg_price_for_notional(asks, depth_notional)
    bid_avg, bid_ok, _, bid_limit = avg_price_for_notional(bids, depth_notional)
    safety_notional = depth_notional * max(1.0, float(depth_safety_multiplier))
    _, ask_safety_ok, _, ask_safety_limit = avg_price_for_notional(asks, safety_notional)
    return {
        "bid": compact_float(stream.get_latest_best_bid(token_id)),
        "ask": compact_float(stream.get_latest_best_ask(token_id)),
        "book_age_ms": compact_float((stream.get_latest_best_ask_age(token_id) or 0) * 1000, 0) if asks or bids else None,
        "ask_avg": ask_avg,
        "bid_avg": bid_avg,
        "ask_limit": ask_limit,
        "ask_safety_limit": ask_safety_limit,
        "bid_limit": bid_limit,
        "stable_depth_usd": compact_float(sum(price * size for price, size in asks), 4),
        "ask_depth_ok": ask_ok and ask_safety_ok,
        "bid_depth_ok": bid_ok,
    }


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
    max_polymarket_price_age_sec: float = 3.0,
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
        max_polymarket_age_sec=max_polymarket_price_age_sec,
    )
    price_source, s_price, basis_bps = price.source, price.effective, price.basis_bps
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
    if polymarket_price_enabled and price.source != "polymarket_chainlink":
        warnings.append("polymarket_price_fallback")
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
        "settlement_aligned": bool(good_resolution and price.source == "polymarket_chainlink"),
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


def find_initial_window(
    series: MarketSeries,
    *,
    include_current: bool = False,
    now: dt.datetime | None = None,
) -> MarketWindow:
    window = find_next_window(series)
    if window is None:
        raise RuntimeError("no live/future BTC 5m market found")
    current_time = now or dt.datetime.now(dt.timezone.utc)
    if not include_current and window.start_time <= current_time < window.end_time:
        return find_following_window(window, series)
    return window


def find_following_window(window: MarketWindow, series: MarketSeries) -> MarketWindow:
    next_window = find_window_after(window.end_epoch, series)
    if next_window is None or next_window.start_epoch <= window.start_epoch:
        raise RuntimeError("no advancing BTC 5m market found")
    return next_window


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
    parser.add_argument("--coinbase", dest="coinbase_enabled", action="store_true", default=True)
    parser.add_argument("--no-coinbase", dest="coinbase_enabled", action="store_false")
    parser.add_argument("--polymarket-price", dest="polymarket_price_enabled", action="store_true", default=True)
    parser.add_argument("--no-polymarket-price", dest="polymarket_price_enabled", action="store_false")
    parser.add_argument("--max-polymarket-price-age-sec", type=float, default=3.0)
    parser.add_argument("--polymarket-backup-after-sec", type=float, default=180.0)
    return parser


async def _noop_price_update(_update) -> None:
    return None


async def run(args: argparse.Namespace) -> int:
    writer = JsonlWriter(args.jsonl)
    feed: BinancePriceFeed | None = None
    coinbase_feed: CoinbaseBtcPriceFeed | None = None
    polymarket_feed = PolymarketChainlinkBtcPriceFeed() if args.polymarket_price_enabled else None
    series = MarketSeries.from_known("btc-updown-5m")
    stream = PriceStream(on_price=_noop_price_update)
    tracker = WindowLimitTracker(args.windows)
    volatility = await asyncio.to_thread(fetch_dvol_snapshot) if args.collect_dvol else None
    next_dvol_refresh = time.monotonic() + args.dvol_refresh_sec if args.collect_dvol and args.dvol_refresh_sec > 0 else None
    backup_started = False
    polymarket_unhealthy_since: float | None = time.monotonic() if polymarket_feed is not None else None

    async def ensure_backup_started() -> None:
        nonlocal feed, coinbase_feed, backup_started
        if backup_started:
            return
        feed = BinancePriceFeed("btcusdt")
        await feed.start()
        if args.coinbase_enabled:
            coinbase_feed = CoinbaseBtcPriceFeed()
            await coinbase_feed.start()
        backup_started = True

    try:
        window = find_initial_window(series, include_current=args.include_current_window)
        prices = WindowPrices()
        if polymarket_feed is not None:
            await polymarket_feed.start()
        if polymarket_feed is None:
            await ensure_backup_started()
        await stream.connect([window.up_token, window.down_token])
        first = True
        deadline = time.monotonic() + max(0.0, args.warmup_timeout_sec)
        while True:
            if first:
                while time.monotonic() < deadline:
                    if polymarket_feed is not None:
                        if polymarket_feed.latest_price is not None:
                            break
                    elif feed.latest_price is not None or (coinbase_feed is not None and coinbase_feed.latest_price is not None):
                        break
                    await asyncio.sleep(0.1)
                first = False
            now = dt.datetime.now(dt.timezone.utc)
            age_sec = (now - window.start_time).total_seconds()
            await refresh_k_price(window, prices, age_sec)
            if polymarket_feed is not None:
                await refresh_polymarket_open(polymarket_feed, window, prices, age_sec)
                pm_age = polymarket_feed.latest_age_sec()
                pm_healthy = polymarket_feed.latest_price is not None and (pm_age is None or pm_age <= args.max_polymarket_price_age_sec)
                if pm_healthy:
                    polymarket_unhealthy_since = None
                elif polymarket_unhealthy_since is None:
                    polymarket_unhealthy_since = time.monotonic()
                if (
                    not backup_started
                    and polymarket_unhealthy_since is not None
                    and time.monotonic() - polymarket_unhealthy_since >= args.polymarket_backup_after_sec
                ):
                    await ensure_backup_started()
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
                max_polymarket_price_age_sec=args.max_polymarket_price_age_sec,
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
