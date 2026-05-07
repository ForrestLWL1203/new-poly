"""Strategy-neutral BTC 5m data helpers shared by collector and bot."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import math
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from new_poly.market.binance import BinancePriceFeed
from new_poly.market.coinbase import CoinbaseBtcPriceFeed
from new_poly.market.market import MarketWindow, find_next_window, find_window_after
from new_poly.market.polymarket_live import PolymarketChainlinkBtcPriceFeed
from new_poly.market.series import MarketSeries
from new_poly.market.stream import PriceStream

POLYMARKET_CRYPTO_PRICE_API = "https://polymarket.com/api/crypto/crypto-price"
K_RETRY_AGES_SEC = (5.0, 8.0, 12.0, 20.0, 30.0, 40.0)
K_RETRY_TIMEOUT_SEC = 40.0
BTC_OPEN_LOOKAROUND_SEC = 5.0
DEFAULT_MAX_DVOL_AGE_SEC = 900.0
POLYMARKET_OPEN_DISAGREE_TOLERANCE_USD = 1.0


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


def iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_float(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(float(value)):
        return None
    return round(float(value), digits)


def price_return_bps(feed: Any, *, now_ts: float, lookback_sec: float) -> float | None:
    latest = getattr(feed, "latest_price", None) if feed is not None else None
    if latest is None or latest <= 0:
        return None
    if not hasattr(feed, "price_at_or_before"):
        return None
    try:
        previous = feed.price_at_or_before(now_ts - lookback_sec, max_backward_sec=lookback_sec + 2.0)
    except TypeError:
        previous = feed.price_at_or_before(now_ts - lookback_sec)
    if previous is None or previous <= 0:
        return None
    return ((latest - previous) / previous) * 10_000.0


def lead_delta(price: float | None, polymarket_price: float | None) -> tuple[float | None, float | None]:
    if price is None or polymarket_price is None or polymarket_price <= 0:
        return None, None
    delta = price - polymarket_price
    return delta, (delta / polymarket_price) * 10_000.0


def side_vs_k(price: float | None, k_price: float | None) -> str | None:
    if price is None or k_price is None:
        return None
    return "up" if price >= k_price else "down"


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


def polymarket_open_disagrees(prices: WindowPrices, *, tolerance_usd: float = POLYMARKET_OPEN_DISAGREE_TOLERANCE_USD) -> bool:
    return (
        prices.polymarket_open_price is not None
        and prices.k_price is not None
        and abs(prices.polymarket_open_price - prices.k_price) > tolerance_usd
    )


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


def _latest_best_bid(stream: PriceStream, token_id: str, top_max_age_sec: float | None) -> float | None:
    try:
        return stream.get_latest_best_bid(token_id, max_age_sec=top_max_age_sec)
    except TypeError:
        return stream.get_latest_best_bid(token_id)


def _latest_best_ask(stream: PriceStream, token_id: str, top_max_age_sec: float | None) -> float | None:
    try:
        return stream.get_latest_best_ask(token_id, max_age_sec=top_max_age_sec)
    except TypeError:
        return stream.get_latest_best_ask(token_id)


def token_state(
    stream: PriceStream,
    token_id: str,
    depth_notional: float,
    depth_safety_multiplier: float = 1.0,
    *,
    top_max_age_sec: float | None = None,
    include_ask_safety: bool = True,
) -> dict[str, Any]:
    asks = stream.get_latest_ask_levels_with_size(token_id)
    bids = stream.get_latest_bid_levels_with_size(token_id)
    ask_avg, ask_ok, _, ask_limit = avg_price_for_notional(asks, depth_notional)
    bid_avg, bid_ok, _, bid_limit = avg_price_for_notional(bids, depth_notional)
    state = {
        "bid": compact_float(_latest_best_bid(stream, token_id, top_max_age_sec)),
        "ask": compact_float(_latest_best_ask(stream, token_id, top_max_age_sec)),
        "book_age_ms": compact_float((stream.get_latest_best_ask_age(token_id) or 0) * 1000, 0) if asks or bids else None,
        "ask_avg": ask_avg,
        "bid_avg": bid_avg,
        "ask_limit": ask_limit,
        "bid_limit": bid_limit,
        "stable_depth_usd": compact_float(sum(price * size for price, size in asks), 4),
        "bid_depth_ok": bid_ok,
    }
    if include_ask_safety:
        safety_notional = depth_notional * max(1.0, float(depth_safety_multiplier))
        _, ask_safety_ok, _, ask_safety_limit = avg_price_for_notional(asks, safety_notional)
        state["ask_safety_limit"] = ask_safety_limit
        state["ask_depth_ok"] = ask_ok and ask_safety_ok
    return state


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
