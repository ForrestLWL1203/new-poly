#!/usr/bin/env python3
"""Dry-run logger for the BTC 5m probability-edge strategy.

This script observes live market state and emits compact JSONL rows. It never
authenticates to CLOB and never posts orders.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import math
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"
CLOB_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_BTC_TRADE_WS = "wss://stream.binance.com:9443/ws/btcusdt@trade"
BTC_5M_PREFIX = "btc-updown-5m"
BTC_5M_STEP = 300
SECONDS_PER_YEAR = 31_536_000
K_RETRY_AGES_SEC = (20.0, 25.0, 30.0, 35.0)
K_RETRY_TIMEOUT_SEC = 35.0
POLYMARKET_EVENT_PREFIXES = (
    "https://polymarket.com/zh/event/",
    "https://polymarket.com/event/",
)

SKIP_REASONS = {
    "warmup",
    "final_no_entry",
    "closed_market",
    "bad_resolution_source",
    "missing_chainlink_price",
    "paper_proxy_only",
    "missing_k",
    "missing_k_timeout",
    "stale_price",
    "stale_book",
    "missing_book",
    "insufficient_depth",
    "edge_too_small",
    "vol_stress",
    "basis_too_wide",
    "one_sided_book",
    "no_trade_blackout",
}


class PriceState:
    def __init__(
        self,
        source: str = "missing",
        s_price: float | None = None,
        k_price: float | None = None,
        basis_bps: float | None = None,
        updated_at: float | None = None,
        binance_price: float | None = None,
        binance_updated_at: float | None = None,
    ) -> None:
        self.source = source
        self.s_price = s_price
        self.k_price = k_price
        self.basis_bps = basis_bps
        self.updated_at = updated_at
        self.binance_price = binance_price
        self.binance_updated_at = binance_updated_at


class WindowPriceState:
    def __init__(
        self,
        k_price: float | None = None,
        close_price: float | None = None,
        k_source: str = "missing",
        binance_open_price: float | None = None,
    ) -> None:
        self.k_price = k_price
        self.close_price = close_price
        self.k_source = k_source
        self.binance_open_price = binance_open_price


class KRetryState:
    def __init__(self) -> None:
        self.attempted_slots: set[float] = set()
        self.last_attempt_age: float | None = None
        self.timed_out = False

    def record_attempt(self, age_sec: float) -> None:
        eligible = [slot for slot in K_RETRY_AGES_SEC if slot <= age_sec and slot not in self.attempted_slots]
        if eligible:
            self.attempted_slots.add(max(eligible))
        self.last_attempt_age = age_sec
        if K_RETRY_TIMEOUT_SEC in self.attempted_slots:
            self.timed_out = True


class WindowLimitTracker:
    def __init__(self, limit: int | None) -> None:
        self.limit = limit
        self.seen: list[str] = []

    def observe(self, slug: str) -> bool:
        if self.limit is None:
            return False
        if slug not in self.seen:
            self.seen.append(slug)
        return len(self.seen) > self.limit


class TokenBookState:
    def __init__(self, token_id: str) -> None:
        self.token_id = token_id
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.bid_seen_at: dict[float, float] = {}
        self.ask_seen_at: dict[float, float] = {}
        self.received_at: float | None = None

    def update_snapshot(self, bids: list[tuple[float, float]], asks: list[tuple[float, float]], now: float) -> None:
        self._replace_side("bid", bids, now)
        self._replace_side("ask", asks, now)
        self.received_at = now

    def update_level(self, side: str, price: float, size: float, now: float) -> None:
        book, seen = self._side_maps(side)
        if size <= 0:
            book.pop(price, None)
            seen.pop(price, None)
        else:
            if price not in book:
                seen[price] = now
            book[price] = size
        self.received_at = now

    def levels(self, side: str, stable_secs: float = 0.0, now: float | None = None) -> list[tuple[float, float]]:
        book, seen = self._side_maps(side)
        rows: list[tuple[float, float]] = []
        for price, size in book.items():
            if size <= 0:
                continue
            if stable_secs > 0 and now is not None:
                first_seen = seen.get(price)
                if first_seen is None or now - first_seen < stable_secs:
                    continue
            rows.append((price, size))
        rows.sort(key=lambda item: item[0], reverse=(side == "bid"))
        return rows

    def best(self, side: str) -> float | None:
        rows = self.levels(side)
        return rows[0][0] if rows else None

    def age_ms(self, now: float) -> int | None:
        if self.received_at is None:
            return None
        return int(round((now - self.received_at) * 1000))

    def _replace_side(self, side: str, rows: list[tuple[float, float]], now: float) -> None:
        book, seen = self._side_maps(side)
        new_book = {price: size for price, size in rows if size > 0}
        new_seen: dict[float, float] = {}
        for price in new_book:
            new_seen[price] = seen.get(price, now)
        book.clear()
        book.update(new_book)
        seen.clear()
        seen.update(new_seen)

    def _side_maps(self, side: str) -> tuple[dict[float, float], dict[float, float]]:
        if side == "bid":
            return self.bids, self.bid_seen_at
        if side == "ask":
            return self.asks, self.ask_seen_at
        raise ValueError(f"unknown side: {side}")


def parse_tokens(raw: Any) -> list[str]:
    if isinstance(raw, str):
        return list(json.loads(raw))
    return list(raw or [])


def parse_time(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def is_chainlink_btc_resolution(*values: str | None) -> bool:
    text = " ".join(str(value or "").lower() for value in values)
    has_chainlink = "chainlink" in text or "chain.link" in text
    return has_chainlink and ("btc" in text or "bitcoin" in text) and "usd" in text


def _balanced_json_object_before_marker(text: str, marker_index: int) -> dict[str, Any] | None:
    start = text.rfind('{"dehydratedAt"', 0, marker_index)
    if start < 0:
        start = text.rfind("{", 0, marker_index)
    if start < 0:
        return None
    in_str = False
    esc = False
    depth = 0
    for idx, ch in enumerate(text[start:], start):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start:idx + 1])
                    except json.JSONDecodeError:
                        return None
    return None


def extract_crypto_prices_from_html(html: str, *, start_iso: str, end_iso: str) -> dict[str, float | None] | None:
    marker = (
        f'"queryKey":["crypto-prices","price","BTC","{start_iso}",'
        f'"fiveminute","{end_iso}"]'
    )
    marker_index = html.find(marker)
    if marker_index < 0:
        return None
    obj = _balanced_json_object_before_marker(html, marker_index)
    if obj is None:
        return None
    data = obj.get("state", {}).get("data")
    if not isinstance(data, dict):
        return None
    open_price = data.get("openPrice")
    close_price = data.get("closePrice")
    try:
        parsed_open = float(open_price) if open_price is not None else None
        parsed_close = float(close_price) if close_price is not None else None
    except (TypeError, ValueError):
        return None
    if parsed_open is None:
        return None
    return {"openPrice": parsed_open, "closePrice": parsed_close}


def binary_probs(
    *,
    s_price: float | None,
    k_price: float | None,
    sigma_eff: float | None,
    remaining_sec: float | None,
) -> tuple[float | None, float | None]:
    if s_price is None or k_price is None or sigma_eff is None or remaining_sec is None:
        return None, None
    if s_price <= 0 or k_price <= 0 or sigma_eff <= 0:
        return None, None
    if remaining_sec <= 0:
        up = 1.0 if s_price >= k_price else 0.0
        return up, 1.0 - up
    t_years = remaining_sec / SECONDS_PER_YEAR
    denom = sigma_eff * math.sqrt(t_years)
    if denom <= 0:
        return None, None
    d2 = (math.log(s_price / k_price) - 0.5 * sigma_eff * sigma_eff * t_years) / denom
    up = 0.5 * (1.0 + math.erf(d2 / math.sqrt(2.0)))
    return up, 1.0 - up


def phase_for_window(age_sec: float, remaining_sec: float) -> str:
    if remaining_sec <= 0:
        return "closed"
    if age_sec < 15:
        return "warmup"
    if age_sec < 120:
        return "early"
    if age_sec < 240:
        return "core"
    if age_sec < 270:
        return "late"
    return "no_entry"


def compact_float(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def avg_price_for_notional(levels: list[tuple[float, float]], target_notional: float) -> dict[str, Any]:
    if target_notional <= 0:
        return {"ok": False, "avg": None, "shares": 0.0, "notional": 0.0}
    shares = 0.0
    notional = 0.0
    for price, size in levels:
        if price <= 0 or size <= 0:
            continue
        remaining_notional = target_notional - notional
        if remaining_notional <= 0:
            break
        take_shares = min(size, remaining_notional / price)
        shares += take_shares
        notional += take_shares * price
        if notional >= target_notional - 1e-12:
            break
    avg = notional / shares if shares > 0 else None
    return {
        "ok": notional >= target_notional - 1e-9,
        "avg": compact_float(avg),
        "shares": compact_float(shares),
        "notional": compact_float(notional),
    }


def stable_depth_usd(levels: list[tuple[float, float]]) -> float:
    return sum(price * size for price, size in levels if price > 0 and size > 0)


def token_log_state(
    book: TokenBookState,
    *,
    prob: float | None,
    order_notional: float,
    stable_secs: float,
    now_mono: float,
) -> tuple[dict[str, Any], bool]:
    bid = book.best("bid")
    ask = book.best("ask")
    stable_asks = book.levels("ask", stable_secs=stable_secs, now=now_mono)
    stable_bids = book.levels("bid", stable_secs=stable_secs, now=now_mono)
    ask_avg = avg_price_for_notional(stable_asks, order_notional)
    bid_avg = avg_price_for_notional(stable_bids, order_notional)
    edge = prob - ask_avg["avg"] if prob is not None and ask_avg["avg"] is not None else None
    age_ms = book.age_ms(now_mono)
    state = {
        "bid": compact_float(bid),
        "ask": compact_float(ask),
        "book_age_ms": age_ms,
        "ask_avg": ask_avg["avg"],
        "bid_avg": bid_avg["avg"],
        "stable_depth_usd": compact_float(stable_depth_usd(stable_asks), 4),
        "edge": compact_float(edge),
    }
    return state, bool(ask_avg["ok"])


def clean_components(components: dict[str, float | None]) -> dict[str, float]:
    return {
        key: round(float(value), 6)
        for key, value in components.items()
        if value is not None and math.isfinite(float(value)) and abs(float(value)) > 1e-12
    }


def choose_skip_reason(
    *,
    phase: str,
    good_resolution: bool,
    price_state: PriceState,
    up_book: TokenBookState,
    down_book: TokenBookState,
    up_depth_ok: bool,
    down_depth_ok: bool,
    max_book_age_ms: int,
    now_mono: float,
    best_edge: float | None,
    required_edge: float,
    k_timed_out: bool = False,
) -> str | None:
    if not good_resolution:
        return "bad_resolution_source"
    if phase == "closed":
        return "closed_market"
    if phase == "warmup":
        return "warmup"
    if phase == "no_entry":
        return "final_no_entry"
    if price_state.k_price is None:
        return "missing_k_timeout" if k_timed_out else "missing_k"
    if price_state.source == "proxy_binance":
        return "paper_proxy_only"
    if price_state.source == "proxy_binance_basis_adjusted":
        return "paper_proxy_only"
    if price_state.source != "chainlink":
        return "missing_chainlink_price"
    if up_book.received_at is None or down_book.received_at is None:
        return "missing_book"
    ages = [age for age in (up_book.age_ms(now_mono), down_book.age_ms(now_mono)) if age is not None]
    if any(age > max_book_age_ms for age in ages):
        return "stale_book"
    if up_book.best("bid") is None or up_book.best("ask") is None or down_book.best("bid") is None or down_book.best("ask") is None:
        return "one_sided_book"
    if not up_depth_ok and not down_depth_ok:
        return "insufficient_depth"
    if best_edge is None or best_edge <= required_edge:
        return "edge_too_small"
    return None


def build_log_row(
    *,
    market: dict[str, Any],
    now: dt.datetime,
    order_notional: float,
    sigma_source: str,
    sigma_eff: float | None,
    price_state: PriceState,
    up_state: TokenBookState,
    down_state: TokenBookState,
    required_edge: float,
    edge_components: dict[str, float | None],
    max_book_age_ms: int = 1000,
    stable_depth_sec: float = 0.0,
    paired_buffer: float = 0.01,
) -> dict[str, Any]:
    start = market.get("start")
    end = market.get("end")
    if not isinstance(start, dt.datetime) or not isinstance(end, dt.datetime):
        raise ValueError("market start/end must be datetimes")
    age_sec = (now - start).total_seconds()
    remaining_sec = (end - now).total_seconds()
    phase = phase_for_window(age_sec, remaining_sec)
    good_resolution = is_chainlink_btc_resolution(
        market.get("resolution_source"),
        market.get("description"),
    )
    settlement_aligned = bool(good_resolution and price_state.source == "chainlink" and price_state.s_price is not None and price_state.k_price is not None)
    live_ready = settlement_aligned
    up_prob, down_prob = binary_probs(
        s_price=price_state.s_price,
        k_price=price_state.k_price,
        sigma_eff=sigma_eff,
        remaining_sec=remaining_sec,
    )
    now_mono = time.monotonic()
    up_log, up_depth_ok = token_log_state(
        up_state,
        prob=up_prob,
        order_notional=order_notional,
        stable_secs=stable_depth_sec,
        now_mono=now_mono,
    )
    down_log, down_depth_ok = token_log_state(
        down_state,
        prob=down_prob,
        order_notional=order_notional,
        stable_secs=stable_depth_sec,
        now_mono=now_mono,
    )
    up_edge = up_log["edge"]
    down_edge = down_log["edge"]
    best_edge = max([edge for edge in (up_edge, down_edge) if edge is not None], default=None)
    candidate_side = None
    if best_edge is not None and best_edge > required_edge:
        candidate_side = "up" if up_edge == best_edge else "down"
    skip_reason = choose_skip_reason(
        phase=phase,
        good_resolution=good_resolution,
        price_state=price_state,
        up_book=up_state,
        down_book=down_state,
        up_depth_ok=up_depth_ok,
        down_depth_ok=down_depth_ok,
        max_book_age_ms=max_book_age_ms,
        now_mono=now_mono,
        best_edge=best_edge,
        required_edge=required_edge,
        k_timed_out=bool(market.get("k_timed_out")),
    )
    decision = "candidate" if skip_reason is None and candidate_side is not None else "skip"
    if decision == "skip":
        candidate_side = None
    ask_sum = None
    if up_log["ask_avg"] is not None and down_log["ask_avg"] is not None:
        ask_sum = up_log["ask_avg"] + down_log["ask_avg"]
    bid_sum = None
    if up_log["bid_avg"] is not None and down_log["bid_avg"] is not None:
        bid_sum = up_log["bid_avg"] + down_log["bid_avg"]
    warnings: list[str] = []
    if price_state.source == "proxy_binance":
        warnings.append("binance_proxy_not_settlement_source")
    if price_state.source == "proxy_binance_basis_adjusted":
        warnings.append("basis_adjusted_binance_proxy_not_settlement_source")
    if not up_depth_ok or not down_depth_ok:
        warnings.append("depth_below_target_notional")
    row = {
        "ts": now.astimezone().isoformat(),
        "market_slug": market.get("slug"),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "age_sec": int(round(age_sec)),
        "remaining_sec": int(round(remaining_sec)),
        "phase": phase,
        "resolution_source": market.get("resolution_source"),
        "settlement_aligned": settlement_aligned,
        "live_ready": live_ready,
        "sigma_source": sigma_source,
        "sigma_eff": compact_float(sigma_eff),
        "price_source": price_state.source,
        "s_price": compact_float(price_state.s_price, 2),
        "k_price": compact_float(price_state.k_price, 2),
        "k_source": market.get("k_source"),
        "close_price": compact_float(market.get("close_price"), 2),
        "basis_bps": compact_float(price_state.basis_bps, 3),
        "up_prob": compact_float(up_prob),
        "down_prob": compact_float(down_prob),
        "required_edge": compact_float(required_edge),
        "edge_components": clean_components(edge_components),
        "order_notional": compact_float(order_notional, 2),
        "up": up_log,
        "down": down_log,
        "yes_no_sum": {
            "ask_sum": compact_float(ask_sum),
            "bid_sum": compact_float(bid_sum),
            "ask_arb": bool(ask_sum is not None and ask_sum < 1.0 - paired_buffer),
            "bid_lock": bool(bid_sum is not None and bid_sum > 1.0 + paired_buffer),
        },
        "decision": decision,
        "candidate_side": candidate_side,
        "skip_reason": skip_reason,
    }
    if warnings:
        row["warnings"] = warnings
    return row


def parse_book_levels(raw_levels: Any) -> list[tuple[float, float]]:
    parsed: list[tuple[float, float]] = []
    for item in raw_levels or []:
        try:
            price = float(item["price"])
            size = float(item.get("size", 0))
        except (KeyError, TypeError, ValueError):
            continue
        if price > 0 and size > 0:
            parsed.append((price, size))
    return parsed


def token_from_event(event: dict[str, Any]) -> str | None:
    for key in ("asset_id", "assetId", "token_id", "tokenId"):
        value = event.get(key)
        if value is not None:
            return str(value)
    return None


def apply_clob_event(event: dict[str, Any], books: dict[str, TokenBookState], now: float) -> None:
    event_type = event.get("event_type") or event.get("type")
    if event_type == "book":
        token = token_from_event(event)
        if token in books:
            books[token].update_snapshot(
                parse_book_levels(event.get("bids")),
                parse_book_levels(event.get("asks")),
                now,
            )
        return
    if event_type == "price_change":
        changes = event.get("price_changes") or event.get("changes") or []
        if isinstance(changes, dict):
            changes = [changes]
        for change in changes:
            token = token_from_event(change) or token_from_event(event)
            if token not in books:
                continue
            try:
                price = float(change["price"])
                size = float(change.get("size", 0))
            except (KeyError, TypeError, ValueError):
                continue
            raw_side = str(change.get("side") or event.get("side") or "").upper()
            side = "bid" if raw_side == "BUY" else "ask" if raw_side == "SELL" else ""
            if side:
                books[token].update_level(side, price, size, now)


def fetch_gamma_markets(slug: str) -> Any:
    url = GAMMA_MARKETS + "?" + urllib.parse.urlencode({"slug": slug})
    req = urllib.request.Request(url, headers={"User-Agent": "new-poly-dry-run/0.1"})
    with urllib.request.urlopen(req, timeout=8.0) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_polymarket_html(slug: str) -> str:
    last_error: Exception | None = None
    for prefix in POLYMARKET_EVENT_PREFIXES:
        try:
            req = urllib.request.Request(prefix + slug, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15.0) as resp:
                return resp.read().decode("utf-8", "ignore")
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"failed to fetch Polymarket event HTML: {last_error}")


def fetch_polymarket_html_candidates(slug: str) -> list[str]:
    htmls: list[str] = []
    last_error: Exception | None = None
    for prefix in POLYMARKET_EVENT_PREFIXES:
        try:
            req = urllib.request.Request(prefix + slug, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15.0) as resp:
                htmls.append(resp.read().decode("utf-8", "ignore"))
        except Exception as exc:
            last_error = exc
    if not htmls and last_error is not None:
        raise RuntimeError(f"failed to fetch Polymarket event HTML: {last_error}")
    return htmls


async def fetch_window_prices(market: dict[str, Any]) -> WindowPriceState:
    start = market["start"]
    end = market["end"]
    htmls = await asyncio.to_thread(fetch_polymarket_html_candidates, market["slug"])
    data = None
    for html in htmls:
        data = extract_crypto_prices_from_html(html, start_iso=iso_z(start), end_iso=iso_z(end))
        if data is not None and data.get("openPrice") is not None:
            break
    if data is None or data.get("openPrice") is None:
        return WindowPriceState()
    return WindowPriceState(
        k_price=data["openPrice"],
        close_price=data["closePrice"],
        k_source="polymarket_html_crypto_prices",
    )


def should_retry_k_price(retry_state: KRetryState, age_sec: float) -> bool:
    if retry_state.timed_out:
        return False
    if age_sec > K_RETRY_TIMEOUT_SEC:
        retry_state.timed_out = True
        return False
    return any(slot <= age_sec and slot not in retry_state.attempted_slots for slot in K_RETRY_AGES_SEC)


async def refresh_missing_window_prices(
    market: dict[str, Any],
    window_prices: WindowPriceState,
    *,
    retry_state: KRetryState,
    age_sec: float,
) -> tuple[WindowPriceState, KRetryState]:
    if window_prices.k_price is not None:
        return window_prices, retry_state
    if not should_retry_k_price(retry_state, age_sec):
        return window_prices, retry_state
    retry_state.record_attempt(age_sec)
    fetched = await fetch_window_prices(market)
    if fetched.binance_open_price is None:
        fetched.binance_open_price = window_prices.binance_open_price
    if fetched.k_price is None:
        return window_prices, retry_state
    return fetched, retry_state


def fetch_binance_open_price(start: dt.datetime) -> float | None:
    start_ms = int(start.timestamp() * 1000)
    url = "https://api.binance.com/api/v3/klines?" + urllib.parse.urlencode(
        {"symbol": "BTCUSDT", "interval": "1m", "startTime": start_ms, "limit": 1}
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "new-poly-dry-run/0.1"})
        with urllib.request.urlopen(req, timeout=8.0) as resp:
            rows = json.loads(resp.read().decode("utf-8"))
        if not rows:
            return None
        return float(rows[0][1])
    except Exception:
        return None


async def discover_btc_5m(max_windows: int = 8) -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc)
    base = int(now.timestamp()) // BTC_5M_STEP * BTC_5M_STEP
    for offset in range(-1, max_windows):
        epoch = base + offset * BTC_5M_STEP
        slug = f"{BTC_5M_PREFIX}-{epoch}"
        rows = await asyncio.to_thread(fetch_gamma_markets, slug)
        if not isinstance(rows, list):
            continue
        for market in rows:
            if market.get("slug") != slug or market.get("closed"):
                continue
            start = parse_time(market.get("eventStartTime"))
            end = parse_time(market.get("endDate"))
            if end is not None and end <= now:
                continue
            tokens = parse_tokens(market.get("clobTokenIds"))
            if start and end and len(tokens) >= 2:
                return {
                    "slug": slug,
                    "start": start,
                    "end": end,
                    "resolution_source": market.get("resolutionSource"),
                    "description": market.get("description"),
                    "up_token": tokens[0],
                    "down_token": tokens[1],
                }
    raise RuntimeError("no live/future BTC 5m market found")


async def binance_trade_task(price_state: PriceState, stop: asyncio.Event) -> None:
    try:
        import websockets
    except ModuleNotFoundError:
        return
    while not stop.is_set():
        try:
            async with websockets.connect(BINANCE_BTC_TRADE_WS, ping_interval=20, ping_timeout=10) as ws:
                while not stop.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    data = json.loads(raw)
                    price = float(data["p"])
                    price_state.binance_price = price
                    price_state.binance_updated_at = time.monotonic()
                    if price_state.source == "missing":
                        price_state.source = "proxy_binance"
                        price_state.s_price = price
        except asyncio.TimeoutError:
            continue
        except Exception:
            await asyncio.sleep(1.0)


async def clob_books_task(
    books: dict[str, TokenBookState],
    token_ids: list[str],
    stop: asyncio.Event,
    log_every_book_change: bool = False,
) -> None:
    try:
        import websockets
    except ModuleNotFoundError:
        return
    subscribe = {
        "type": "market",
        "assets_ids": token_ids,
        "operation": "subscribe",
        "custom_feature_enabled": True,
    }
    while not stop.is_set():
        try:
            async with websockets.connect(CLOB_MARKET_WS, ping_interval=None) as ws:
                await ws.send(json.dumps(subscribe))
                next_ping = time.monotonic() + 10.0
                while not stop.is_set():
                    timeout = max(0.1, next_ping - time.monotonic())
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        await ws.send("{}")
                        next_ping = time.monotonic() + 10.0
                        continue
                    payload = json.loads(raw)
                    events = payload if isinstance(payload, list) else [payload]
                    now = time.monotonic()
                    for event in events:
                        if isinstance(event, dict):
                            apply_clob_event(event, books, now)
                    if log_every_book_change:
                        pass
        except Exception:
            await asyncio.sleep(1.0)


class JsonlWriter:
    def __init__(self, path: Path | None) -> None:
        self.path = path
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = path.open("a", encoding="utf-8") if path else None

    def write(self, row: dict[str, Any]) -> None:
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
        print(line, flush=True)
        if self.handle is not None:
            self.handle.write(line + "\n")
            self.handle.flush()

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()


def error_row(message: str) -> dict[str, Any]:
    return {
        "ts": dt.datetime.now().astimezone().isoformat(),
        "decision": "skip",
        "skip_reason": "error",
        "error": message,
        "live_ready": False,
        "settlement_aligned": False,
    }


def basis_adjusted_price_state(
    shared: PriceState,
    *,
    k_price: float | None,
    binance_open_price: float | None,
) -> PriceState:
    if k_price is None:
        return PriceState(
            source="proxy_binance" if shared.binance_price is not None else "missing",
            s_price=shared.binance_price,
            k_price=None,
            binance_price=shared.binance_price,
            binance_updated_at=shared.binance_updated_at,
        )
    if shared.binance_price is not None and binance_open_price is not None:
        basis = binance_open_price - k_price
        adjusted = shared.binance_price - basis
        return PriceState(
            source="proxy_binance_basis_adjusted",
            s_price=adjusted,
            k_price=k_price,
            basis_bps=(basis / k_price) * 10_000.0,
            binance_price=shared.binance_price,
            binance_updated_at=shared.binance_updated_at,
        )
    return PriceState(
        source="missing",
        s_price=None,
        k_price=k_price,
        binance_price=shared.binance_price,
        binance_updated_at=shared.binance_updated_at,
    )


def build_price_state(args: argparse.Namespace, shared: PriceState) -> PriceState:
    state = PriceState(
        source=shared.source,
        s_price=shared.s_price,
        k_price=None,
        basis_bps=None,
        updated_at=shared.updated_at,
        binance_price=shared.binance_price,
        binance_updated_at=shared.binance_updated_at,
    )
    if args.chainlink_s_price is not None and args.chainlink_k_price is not None:
        state.source = "chainlink"
        state.s_price = args.chainlink_s_price
        state.k_price = args.chainlink_k_price
        state.updated_at = time.monotonic()
        if shared.binance_price:
            state.basis_bps = ((shared.binance_price - state.s_price) / state.s_price) * 10_000.0
    elif shared.binance_price is not None:
        state.source = "proxy_binance"
        state.s_price = shared.binance_price
    return state


def build_effective_price_state(
    args: argparse.Namespace,
    shared: PriceState,
    window_prices: WindowPriceState,
) -> PriceState:
    if args.chainlink_s_price is not None and args.chainlink_k_price is not None:
        return build_price_state(args, shared)
    return basis_adjusted_price_state(
        shared,
        k_price=window_prices.k_price,
        binance_open_price=window_prices.binance_open_price,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emit compact JSONL dry-run rows for BTC 5m probability edge.")
    parser.add_argument("--order-notional", type=float, default=5.0)
    parser.add_argument("--base-edge", type=float, default=0.07)
    parser.add_argument("--max-book-age-ms", type=int, default=1000)
    parser.add_argument("--max-binance-age-ms", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--jsonl", type=Path)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--log-every-book-change", action="store_true")
    parser.add_argument("--stable-depth-sec", type=float, default=2.0)
    parser.add_argument("--paired-buffer", type=float, default=0.01)
    parser.add_argument("--sigma-eff", type=float, default=None)
    parser.add_argument("--sigma-source", default="missing")
    parser.add_argument("--chainlink-s-price", type=float, default=None)
    parser.add_argument("--chainlink-k-price", type=float, default=None)
    parser.add_argument("--warmup-timeout-sec", type=float, default=8.0)
    parser.add_argument(
        "--windows",
        type=int,
        default=None,
        help="Stop after observing this many distinct BTC 5m windows. Omit to run forever.",
    )
    return parser


async def run(args: argparse.Namespace) -> int:
    writer = JsonlWriter(args.jsonl)
    stop = asyncio.Event()
    tasks: list[asyncio.Task[Any]] = []
    try:
        window_tracker = WindowLimitTracker(args.windows)
        market = await discover_btc_5m()
        window_prices = WindowPriceState()
        window_prices.binance_open_price = await asyncio.to_thread(fetch_binance_open_price, market["start"])
        k_retry_state = KRetryState()
        market["k_source"] = window_prices.k_source
        market["close_price"] = window_prices.close_price
        market["k_timed_out"] = k_retry_state.timed_out
        books = {
            market["up_token"]: TokenBookState(market["up_token"]),
            market["down_token"]: TokenBookState(market["down_token"]),
        }
        shared_price = PriceState()
        tasks = [
            asyncio.create_task(binance_trade_task(shared_price, stop)),
            asyncio.create_task(
                clob_books_task(
                    books,
                    [market["up_token"], market["down_token"]],
                    stop,
                    log_every_book_change=args.log_every_book_change,
                )
            ),
        ]
        first = True
        deadline = time.monotonic() + max(0.0, args.warmup_timeout_sec)
        while True:
            if window_tracker.observe(str(market["slug"])):
                return 0
            if first:
                while time.monotonic() < deadline:
                    if shared_price.binance_price is not None and all(book.received_at is not None for book in books.values()):
                        break
                    await asyncio.sleep(0.1)
                first = False
            now_for_row = dt.datetime.now(dt.timezone.utc)
            age_sec = (now_for_row - market["start"]).total_seconds()
            window_prices, k_retry_state = await refresh_missing_window_prices(
                market,
                window_prices,
                retry_state=k_retry_state,
                age_sec=age_sec,
            )
            market["k_source"] = window_prices.k_source
            market["close_price"] = window_prices.close_price
            market["k_timed_out"] = k_retry_state.timed_out
            price_state = build_effective_price_state(args, shared_price, window_prices)
            edge_components = {"base": args.base_edge}
            row = build_log_row(
                market=market,
                now=now_for_row,
                order_notional=args.order_notional,
                sigma_source=args.sigma_source if args.sigma_eff is not None else "missing",
                sigma_eff=args.sigma_eff,
                price_state=price_state,
                up_state=books[market["up_token"]],
                down_state=books[market["down_token"]],
                required_edge=args.base_edge,
                edge_components=edge_components,
                max_book_age_ms=args.max_book_age_ms,
                stable_depth_sec=args.stable_depth_sec,
                paired_buffer=args.paired_buffer,
            )
            if args.verbose:
                row["tokens"] = {"up": market["up_token"], "down": market["down_token"]}
            writer.write(row)
            if args.once:
                return 0
            await asyncio.sleep(args.interval_sec)
            now_utc = dt.datetime.now(dt.timezone.utc)
            if now_utc >= market["end"]:
                market = await discover_btc_5m()
                window_prices = WindowPriceState()
                window_prices.binance_open_price = await asyncio.to_thread(fetch_binance_open_price, market["start"])
                k_retry_state = KRetryState()
                market["k_source"] = window_prices.k_source
                market["close_price"] = window_prices.close_price
                market["k_timed_out"] = k_retry_state.timed_out
                books = {
                    market["up_token"]: TokenBookState(market["up_token"]),
                    market["down_token"]: TokenBookState(market["down_token"]),
                }
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                stop = asyncio.Event()
                tasks = [
                    asyncio.create_task(binance_trade_task(shared_price, stop)),
                    asyncio.create_task(
                        clob_books_task(
                            books,
                            [market["up_token"], market["down_token"]],
                            stop,
                            log_every_book_change=args.log_every_book_change,
                        )
                    ),
                ]
                deadline = time.monotonic() + max(0.0, args.warmup_timeout_sec)
                first = True
    except Exception as exc:
        writer.write(error_row(str(exc)))
        return 1
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        writer.close()


def main() -> int:
    args = build_arg_parser().parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
