"""Paper and live FAK execution gateways."""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field, replace
from typing import Any, Awaitable, Callable, Optional

from new_poly import config
from new_poly.market.stream import PriceStream
from .clob_client import get_client, get_order_options, get_token_balance
from .fak_quotes import buffer_buy_price_hint, get_tick_size

BUY = "BUY"
SELL = "SELL"


@dataclass(frozen=True)
class ExecutionConfig:
    paper_latency_sec: float = 0.0
    depth_notional: float = 5.0
    depth_safety_multiplier: float = 1.0
    max_book_age_sec: float = 1.0
    retry_count: int = 1
    retry_interval_sec: float = 0.0
    buy_price_buffer_ticks: float = 2.0
    buy_retry_price_buffer_ticks: float = 4.0
    sell_price_buffer_ticks: float = 5.0
    sell_retry_price_buffer_ticks: float = 6.0
    batch_exit_enabled: bool = False
    batch_exit_min_shares: float = 20.0
    batch_exit_min_notional_usd: float = 5.0
    batch_exit_slices: tuple[float, ...] = (0.4, 0.3, 1.0)
    batch_exit_extra_buffer_ticks: tuple[float, ...] = (0.0, 3.0, 6.0)

    def normalization_warnings(self) -> tuple[str, ...]:
        warnings: list[str] = []
        if self.buy_retry_price_buffer_ticks < self.buy_price_buffer_ticks:
            warnings.append("buy_retry_price_buffer_ticks_clamped_to_buy_price_buffer_ticks")
        if self.sell_retry_price_buffer_ticks < self.sell_price_buffer_ticks:
            warnings.append("sell_retry_price_buffer_ticks_clamped_to_sell_price_buffer_ticks")
        return tuple(warnings)

    def normalized(self) -> "ExecutionConfig":
        return replace(
            self,
            buy_retry_price_buffer_ticks=max(self.buy_price_buffer_ticks, self.buy_retry_price_buffer_ticks),
            sell_retry_price_buffer_ticks=max(self.sell_price_buffer_ticks, self.sell_retry_price_buffer_ticks),
        )


@dataclass(frozen=True)
class ExecutionResult:
    success: bool
    order_id: str | None = None
    filled_size: float = 0.0
    avg_price: float = 0.0
    message: str = ""
    mode: str = "paper"
    latency_ms: int | None = None
    attempt: int = 1
    total_latency_ms: int | None = None
    timing: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BuyRetryParams:
    max_price: float | None
    best_ask: float | None
    price_hint_base: float | None


@dataclass(frozen=True)
class SellRetryParams:
    min_price: float | None
    exit_reason: str | None


BuyRetryRefresh = Callable[[int], Awaitable[Optional[BuyRetryParams]]]
SellRetryRefresh = Callable[[int], Awaitable[Optional[SellRetryParams]]]


def _with_attempt(result: ExecutionResult, *, attempt: int, total_latency_ms: int) -> ExecutionResult:
    return replace(result, attempt=attempt, total_latency_ms=total_latency_ms)


def _retry_skipped(result: ExecutionResult, *, attempt: int, total_latency_ms: int) -> ExecutionResult:
    message = result.message or "order no fill"
    return replace(
        result,
        success=False,
        message=f"{message}; retry skipped: signal no longer valid",
        attempt=attempt,
        total_latency_ms=total_latency_ms,
    )


def _ms_since(start: float) -> int:
    return round((time.monotonic() - start) * 1000)


def _avg_buy_fill(levels: list[tuple[float, float]], amount_usd: float, max_price: float | None) -> tuple[float, float] | None:
    notional = 0.0
    shares = 0.0
    for price, size in levels:
        if max_price is not None and price > max_price:
            break
        take = min(size, max(0.0, amount_usd - notional) / price)
        notional += take * price
        shares += take
        if notional >= amount_usd - 1e-9:
            return shares, notional / shares
    return None


def _avg_sell_fill(levels: list[tuple[float, float]], shares: float, min_price: float | None) -> tuple[float, float] | None:
    sold = 0.0
    received = 0.0
    for price, size in levels:
        if min_price is not None and price < min_price:
            break
        take = min(size, shares - sold)
        sold += take
        received += take * price
        if sold >= shares - 1e-9:
            return sold, received / sold
    return None


def _avg_sell_fill_partial(levels: list[tuple[float, float]], shares: float, min_price: float | None) -> tuple[float, float, list[tuple[float, float]]] | None:
    sold = 0.0
    received = 0.0
    remaining_levels: list[tuple[float, float]] = []
    for price, size in levels:
        if min_price is not None and price < min_price:
            remaining_levels.append((price, size))
            continue
        remaining_to_sell = shares - sold
        if remaining_to_sell <= 1e-9:
            remaining_levels.append((price, size))
            continue
        take = min(size, remaining_to_sell)
        sold += take
        received += take * price
        leftover = size - take
        if leftover > 1e-9:
            remaining_levels.append((price, leftover))
    if sold <= 1e-9:
        return None
    return sold, received / sold, remaining_levels


def _sell_aggression_ticks(
    exit_reason: str | None,
    attempt: int,
    *,
    sell_price_buffer_ticks: float,
    sell_retry_price_buffer_ticks: float,
) -> float:
    if exit_reason == "final_force_exit":
        # Keep this emergency ladder fixed: the final seconds prioritize
        # reducing expiry exposure over profile-level price preservation.
        return 5.0 if attempt == 0 else 10.0
    if exit_reason in {
        "logic_decay_exit",
        "risk_exit",
        "market_overprice_exit",
        "market_disagrees_exit",
        "polymarket_divergence_exit",
        "defensive_take_profit",
        "profit_protection_exit",
    }:
        return sell_price_buffer_ticks if attempt == 0 else sell_retry_price_buffer_ticks
    return 0.0


def _sell_price_hint(
    token_id: str,
    min_price: float | None,
    exit_reason: str | None,
    attempt: int,
    *,
    sell_price_buffer_ticks: float = 5.0,
    sell_retry_price_buffer_ticks: float = 6.0,
    tick_size: float | None = None,
) -> float | None:
    if min_price is None:
        return None
    tick = float(tick_size) if tick_size is not None else get_tick_size(token_id)
    if tick <= 0:
        tick = 0.01
    buffered = min_price - _sell_aggression_ticks(
        exit_reason,
        attempt,
        sell_price_buffer_ticks=sell_price_buffer_ticks,
        sell_retry_price_buffer_ticks=sell_retry_price_buffer_ticks,
    ) * tick
    floor = tick
    rounded = round(max(floor, buffered), 6)
    return min(1.0, rounded)


def _sell_price_hint_with_extra(
    token_id: str,
    min_price: float | None,
    exit_reason: str | None,
    attempt: int,
    *,
    extra_buffer_ticks: float,
    sell_price_buffer_ticks: float = 5.0,
    sell_retry_price_buffer_ticks: float = 6.0,
    tick_size: float | None = None,
) -> float | None:
    if min_price is None:
        return None
    tick = float(tick_size) if tick_size is not None else get_tick_size(token_id)
    if tick <= 0:
        tick = 0.01
    base_ticks = _sell_aggression_ticks(
        exit_reason,
        attempt,
        sell_price_buffer_ticks=sell_price_buffer_ticks,
        sell_retry_price_buffer_ticks=sell_retry_price_buffer_ticks,
    )
    buffered = min_price - (base_ticks + max(0.0, extra_buffer_ticks)) * tick
    return round(max(tick, min(1.0, buffered)), 6)


def _batch_exit_parts(shares: float, slices: tuple[float, ...]) -> list[float]:
    if shares <= 0:
        return []
    clean = [value for value in slices if value > 0]
    if not clean:
        return [shares]
    remaining = shares
    parts: list[float] = []
    for index, value in enumerate(clean):
        is_last = index == len(clean) - 1
        if is_last or value >= 1.0:
            part = remaining
        else:
            part = min(remaining, shares * value)
        if part > 1e-9:
            parts.append(part)
            remaining -= part
        if remaining <= 1e-9:
            break
    if remaining > 1e-9:
        parts.append(remaining)
    return parts


def _should_batch_exit(shares: float, min_price: float | None, cfg: ExecutionConfig) -> bool:
    if not cfg.batch_exit_enabled:
        return False
    if shares >= cfg.batch_exit_min_shares:
        return True
    if min_price is not None and shares * min_price >= cfg.batch_exit_min_notional_usd:
        return True
    return False


class PaperExecutionGateway:
    def __init__(
        self,
        *,
        stream: PriceStream,
        config: ExecutionConfig,
        before_fill: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.stream = stream
        self.config = config
        self.before_fill = before_fill

    async def _delay(self) -> int:
        start = time.monotonic()
        if self.before_fill is not None:
            await self.before_fill()
        if self.config.paper_latency_sec > 0:
            await asyncio.sleep(self.config.paper_latency_sec)
        return _ms_since(start)

    async def _retry_wait(self) -> int:
        start = time.monotonic()
        if self.before_fill is not None:
            await self.before_fill()
        if self.config.retry_interval_sec > 0:
            await asyncio.sleep(self.config.retry_interval_sec)
        return _ms_since(start)

    def _paper_timing(self, *, start: float, attempts: int, sleep_ms: int, retry_wait_ms: int, retry_refresh_ms: int, book_read_ms: int) -> dict[str, Any]:
        return {
            "paper_configured_latency_ms": round(max(0.0, self.config.paper_latency_sec) * 1000),
            "paper_actual_sleep_ms": sleep_ms,
            "retry_configured_wait_ms": round(max(0.0, self.config.retry_interval_sec) * 1000),
            "retry_actual_wait_ms": retry_wait_ms,
            "retry_refresh_ms": retry_refresh_ms,
            "book_read_ms": book_read_ms,
            "attempts": attempts,
            "total_latency_ms": _ms_since(start),
        }

    def _paper_batch_sell_fill(
        self,
        token_id: str,
        levels: list[tuple[float, float]],
        shares: float,
        min_price: float | None,
        exit_reason: str | None,
        attempt: int,
    ) -> tuple[float, float] | None:
        sold_total = 0.0
        received_total = 0.0
        remaining_levels = list(levels)
        parts = _batch_exit_parts(shares, self.config.batch_exit_slices)
        extra_ticks = self.config.batch_exit_extra_buffer_ticks
        for index, part in enumerate(parts):
            extra = extra_ticks[index] if index < len(extra_ticks) else extra_ticks[-1] if extra_ticks else 0.0
            effective_min = _sell_price_hint_with_extra(
                token_id,
                min_price,
                exit_reason,
                attempt,
                extra_buffer_ticks=extra,
                sell_price_buffer_ticks=self.config.sell_price_buffer_ticks,
                sell_retry_price_buffer_ticks=self.config.sell_retry_price_buffer_ticks,
                tick_size=0.01,
            )
            fill = _avg_sell_fill_partial(remaining_levels, part, effective_min)
            if fill is None:
                continue
            sold, avg_price, remaining_levels = fill
            sold_total += sold
            received_total += sold * avg_price
        if sold_total <= 1e-9:
            return None
        return sold_total, received_total / sold_total

    async def buy(
        self,
        token_id: str,
        amount_usd: float,
        max_price: float | None = None,
        best_ask: float | None = None,
        price_hint_base: float | None = None,
        retry_refresh: BuyRetryRefresh | None = None,
    ) -> ExecutionResult:
        start = time.monotonic()
        sleep_ms = await self._delay()
        retry_wait_ms = 0
        retry_refresh_ms = 0
        book_read_ms = 0
        for attempt in range(self.config.retry_count + 1):
            if attempt > 0:
                retry_wait_ms += await self._retry_wait()
                if retry_refresh is not None:
                    refresh_start = time.monotonic()
                    refreshed = await retry_refresh(attempt)
                    retry_refresh_ms += _ms_since(refresh_start)
                    if refreshed is None:
                        total_latency_ms = _ms_since(start)
                        return _retry_skipped(
                            ExecutionResult(
                                False,
                                message="paper buy no fill",
                                mode="paper",
                                timing=self._paper_timing(
                                    start=start,
                                    attempts=attempt,
                                    sleep_ms=sleep_ms,
                                    retry_wait_ms=retry_wait_ms,
                                    retry_refresh_ms=retry_refresh_ms,
                                    book_read_ms=book_read_ms,
                                ),
                            ),
                            attempt=attempt,
                            total_latency_ms=total_latency_ms,
                        )
                    max_price = refreshed.max_price
                    best_ask = refreshed.best_ask
                    price_hint_base = refreshed.price_hint_base
            book_start = time.monotonic()
            levels = self.stream.get_latest_ask_levels_with_size(token_id, max_age_sec=self.config.max_book_age_sec)
            book_read_ms += _ms_since(book_start)
            fill = _avg_buy_fill(levels, amount_usd, max_price)
            total_latency_ms = _ms_since(start)
            if fill is not None:
                shares, avg_price = fill
                return ExecutionResult(True, order_id="paper-buy", filled_size=shares, avg_price=avg_price, message="paper buy filled", latency_ms=total_latency_ms, attempt=attempt + 1, total_latency_ms=total_latency_ms, timing=self._paper_timing(start=start, attempts=attempt + 1, sleep_ms=sleep_ms, retry_wait_ms=retry_wait_ms, retry_refresh_ms=retry_refresh_ms, book_read_ms=book_read_ms))
        return ExecutionResult(False, message="paper no fill: insufficient ask depth", latency_ms=total_latency_ms, attempt=self.config.retry_count + 1, total_latency_ms=total_latency_ms, timing=self._paper_timing(start=start, attempts=self.config.retry_count + 1, sleep_ms=sleep_ms, retry_wait_ms=retry_wait_ms, retry_refresh_ms=retry_refresh_ms, book_read_ms=book_read_ms))

    async def sell(
        self,
        token_id: str,
        shares: float,
        min_price: float | None = None,
        exit_reason: str | None = None,
        retry_refresh: SellRetryRefresh | None = None,
    ) -> ExecutionResult:
        start = time.monotonic()
        sleep_ms = await self._delay()
        retry_wait_ms = 0
        retry_refresh_ms = 0
        book_read_ms = 0
        for attempt in range(self.config.retry_count + 1):
            if attempt > 0:
                retry_wait_ms += await self._retry_wait()
                if retry_refresh is not None:
                    refresh_start = time.monotonic()
                    refreshed = await retry_refresh(attempt)
                    retry_refresh_ms += _ms_since(refresh_start)
                    if refreshed is None:
                        total_latency_ms = _ms_since(start)
                        return _retry_skipped(
                            ExecutionResult(
                                False,
                                message="paper sell no fill",
                                mode="paper",
                                timing=self._paper_timing(
                                    start=start,
                                    attempts=attempt,
                                    sleep_ms=sleep_ms,
                                    retry_wait_ms=retry_wait_ms,
                                    retry_refresh_ms=retry_refresh_ms,
                                    book_read_ms=book_read_ms,
                                ),
                            ),
                            attempt=attempt,
                            total_latency_ms=total_latency_ms,
                        )
                    min_price = refreshed.min_price
                    exit_reason = refreshed.exit_reason
            book_start = time.monotonic()
            levels = self.stream.get_latest_bid_levels_with_size(token_id, max_age_sec=self.config.max_book_age_sec)
            book_read_ms += _ms_since(book_start)
            effective_min = _sell_price_hint(
                token_id,
                min_price,
                exit_reason,
                attempt,
                sell_price_buffer_ticks=self.config.sell_price_buffer_ticks,
                sell_retry_price_buffer_ticks=self.config.sell_retry_price_buffer_ticks,
                tick_size=0.01,
            )
            if _should_batch_exit(shares, min_price, self.config):
                fill = self._paper_batch_sell_fill(token_id, levels, shares, min_price, exit_reason, attempt)
            else:
                fill = _avg_sell_fill(levels, shares, effective_min)
            total_latency_ms = _ms_since(start)
            if fill is not None:
                sold, avg_price = fill
                return ExecutionResult(True, order_id="paper-sell", filled_size=sold, avg_price=avg_price, message="paper sell filled", latency_ms=total_latency_ms, attempt=attempt + 1, total_latency_ms=total_latency_ms, timing=self._paper_timing(start=start, attempts=attempt + 1, sleep_ms=sleep_ms, retry_wait_ms=retry_wait_ms, retry_refresh_ms=retry_refresh_ms, book_read_ms=book_read_ms))
        return ExecutionResult(False, message="paper no fill: insufficient bid depth", latency_ms=total_latency_ms, attempt=self.config.retry_count + 1, total_latency_ms=total_latency_ms, timing=self._paper_timing(start=start, attempts=self.config.retry_count + 1, sleep_ms=sleep_ms, retry_wait_ms=retry_wait_ms, retry_refresh_ms=retry_refresh_ms, book_read_ms=book_read_ms))


def _safe_float(value) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _derive_fill(side: str, amount: float, taking: float, making: float, fallback_price: float) -> tuple[float, float]:
    if side == BUY:
        filled = taking
        price = making / taking if making > 0 and taking > 0 else fallback_price
        if filled <= 0 and price > 0:
            filled = amount / price
        return filled, price
    filled = making if making > 0 else amount
    price = taking / making if taking > 0 and making > 0 else fallback_price
    return filled, price


def _is_matched_response(resp: dict) -> bool:
    status = str(resp.get("status", "")).upper()
    return bool(resp.get("success")) and status == "MATCHED"


def _is_fak_no_match_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "no orders found to match" in text and "fak" in text


def _order_id_from_error(exc: Exception) -> str | None:
    match = re.search(r"['\"]orderID['\"]\s*:\s*['\"]([^'\"]+)['\"]", str(exc))
    return match.group(1) if match else None


class LiveFakExecutionGateway:
    def __init__(
        self,
        *,
        live_risk_ack: bool,
        retry_count: int = 1,
        retry_interval_sec: float = 0.0,
        buy_price_buffer_ticks: float = 2.0,
        buy_retry_price_buffer_ticks: float = 4.0,
        sell_price_buffer_ticks: float = 5.0,
        sell_retry_price_buffer_ticks: float = 6.0,
        batch_exit_enabled: bool = False,
        batch_exit_min_shares: float = 20.0,
        batch_exit_min_notional_usd: float = 5.0,
        batch_exit_slices: tuple[float, ...] = (0.4, 0.3, 1.0),
        batch_exit_extra_buffer_ticks: tuple[float, ...] = (0.0, 3.0, 6.0),
    ) -> None:
        if not live_risk_ack:
            raise ValueError("live mode requires --i-understand-live-risk")
        self.retry_count = max(0, int(retry_count))
        self.retry_interval_sec = max(0.0, float(retry_interval_sec))
        self.buy_price_buffer_ticks = max(0.0, float(buy_price_buffer_ticks))
        self.buy_retry_price_buffer_ticks = max(self.buy_price_buffer_ticks, float(buy_retry_price_buffer_ticks))
        self.sell_price_buffer_ticks = max(0.0, float(sell_price_buffer_ticks))
        self.sell_retry_price_buffer_ticks = max(self.sell_price_buffer_ticks, float(sell_retry_price_buffer_ticks))
        self.batch_config = ExecutionConfig(
            batch_exit_enabled=bool(batch_exit_enabled),
            batch_exit_min_shares=max(0.0, float(batch_exit_min_shares)),
            batch_exit_min_notional_usd=max(0.0, float(batch_exit_min_notional_usd)),
            batch_exit_slices=tuple(float(value) for value in batch_exit_slices),
            batch_exit_extra_buffer_ticks=tuple(float(value) for value in batch_exit_extra_buffer_ticks),
            sell_price_buffer_ticks=self.sell_price_buffer_ticks,
            sell_retry_price_buffer_ticks=self.sell_retry_price_buffer_ticks,
        )

    async def buy(
        self,
        token_id: str,
        amount_usd: float,
        max_price: float | None = None,
        best_ask: float | None = None,
        price_hint_base: float | None = None,
        retry_refresh: BuyRetryRefresh | None = None,
    ) -> ExecutionResult:
        base_price = price_hint_base if price_hint_base is not None else best_ask
        last = ExecutionResult(False, message="live buy not attempted", mode="live")
        start = time.monotonic()
        for attempt in range(self.retry_count + 1):
            buffer_ticks = self.buy_price_buffer_ticks if attempt == 0 else self.buy_retry_price_buffer_ticks
            price_hint = buffer_buy_price_hint(
                token_id,
                base_price,
                buffer_ticks=buffer_ticks,
                max_price=max_price,
            )
            last = await asyncio.to_thread(self._post, token_id, amount_usd, BUY, price_hint or max_price)
            if last.success or attempt >= self.retry_count:
                return _with_attempt(last, attempt=attempt + 1, total_latency_ms=round((time.monotonic() - start) * 1000))
            if self.retry_interval_sec > 0:
                await asyncio.sleep(self.retry_interval_sec)
            if retry_refresh is not None:
                refreshed = await retry_refresh(attempt + 1)
                if refreshed is None:
                    return _retry_skipped(
                        last,
                        attempt=attempt + 1,
                        total_latency_ms=round((time.monotonic() - start) * 1000),
                    )
                max_price = refreshed.max_price
                best_ask = refreshed.best_ask
                base_price = refreshed.price_hint_base if refreshed.price_hint_base is not None else refreshed.best_ask
        return last

    async def sell(
        self,
        token_id: str,
        shares: float,
        min_price: float | None = None,
        exit_reason: str | None = None,
        retry_refresh: SellRetryRefresh | None = None,
    ) -> ExecutionResult:
        balance = await asyncio.to_thread(get_token_balance, token_id, safe=True)
        amount = min(shares, balance or 0.0)
        if amount <= 0:
            return ExecutionResult(False, message="live no sellable balance", mode="live", attempt=0, total_latency_ms=0)
        last = ExecutionResult(False, message="live sell not attempted", mode="live")
        start = time.monotonic()
        for attempt in range(self.retry_count + 1):
            price_hint = _sell_price_hint(
                token_id,
                min_price,
                exit_reason,
                attempt,
                sell_price_buffer_ticks=self.sell_price_buffer_ticks,
                sell_retry_price_buffer_ticks=self.sell_retry_price_buffer_ticks,
            )
            if _should_batch_exit(amount, min_price, self.batch_config):
                last = await asyncio.to_thread(self._post_batch_sell, token_id, amount, min_price, exit_reason, attempt)
            else:
                last = await asyncio.to_thread(self._post, token_id, amount, SELL, price_hint or min_price)
            if last.success or attempt >= self.retry_count:
                return _with_attempt(last, attempt=attempt + 1, total_latency_ms=round((time.monotonic() - start) * 1000))
            if self.retry_interval_sec > 0:
                await asyncio.sleep(self.retry_interval_sec)
            if retry_refresh is not None:
                refreshed = await retry_refresh(attempt + 1)
                if refreshed is None:
                    return _retry_skipped(
                        last,
                        attempt=attempt + 1,
                        total_latency_ms=round((time.monotonic() - start) * 1000),
                    )
                min_price = refreshed.min_price
                exit_reason = refreshed.exit_reason
            balance = await asyncio.to_thread(get_token_balance, token_id, safe=True)
            amount = min(shares, balance or 0.0)
            if amount <= 0:
                return _with_attempt(last, attempt=attempt + 1, total_latency_ms=round((time.monotonic() - start) * 1000))
        return last

    def _post_batch_sell(self, token_id: str, shares: float, min_price: float | None, exit_reason: str | None, attempt: int) -> ExecutionResult:
        from py_clob_client_v2 import MarketOrderArgs, OrderType
        from py_clob_client_v2.order_builder.constants import SELL as SDK_SELL

        start = time.monotonic()
        client = get_client()
        post_orders = getattr(client, "post_orders", None) or getattr(client, "postOrders", None)
        if post_orders is None:
            return self._post(token_id, shares, SELL, _sell_price_hint(
                token_id,
                min_price,
                exit_reason,
                attempt,
                sell_price_buffer_ticks=self.sell_price_buffer_ticks,
                sell_retry_price_buffer_ticks=self.sell_retry_price_buffer_ticks,
            ) or min_price)
        parts = _batch_exit_parts(shares, self.batch_config.batch_exit_slices)
        extra_ticks = self.batch_config.batch_exit_extra_buffer_ticks
        batch = []
        for index, part in enumerate(parts[:15]):
            extra = extra_ticks[index] if index < len(extra_ticks) else extra_ticks[-1] if extra_ticks else 0.0
            price_hint = _sell_price_hint_with_extra(
                token_id,
                min_price,
                exit_reason,
                attempt,
                extra_buffer_ticks=extra,
                sell_price_buffer_ticks=self.sell_price_buffer_ticks,
                sell_retry_price_buffer_ticks=self.sell_retry_price_buffer_ticks,
            )
            args = MarketOrderArgs(token_id=token_id, amount=part, side=SDK_SELL, order_type=OrderType.FAK, price=price_hint or min_price or 0)
            signed = client.create_market_order(args, options=get_order_options(token_id))
            batch.append({"order": signed, "orderType": OrderType.FAK})
        try:
            responses = post_orders(batch)
        except Exception as exc:
            latency_ms = round((time.monotonic() - start) * 1000)
            if _is_fak_no_match_error(exc):
                return ExecutionResult(False, message="live no fill: batch FAK no match", mode="live", latency_ms=latency_ms, total_latency_ms=latency_ms)
            raise
        latency_ms = round((time.monotonic() - start) * 1000)
        if not isinstance(responses, list):
            responses = [responses]
        filled_total = 0.0
        received_total = 0.0
        order_ids: list[str] = []
        matched_count = 0
        for response, part in zip(responses, parts):
            if not isinstance(response, dict):
                continue
            order_id = response.get("orderID") or response.get("orderId") or response.get("id")
            if order_id:
                order_ids.append(str(order_id))
            matched = _is_matched_response(response)
            filled = _safe_float(response.get("sizeFilled", response.get("filledSize", 0)))
            avg_price = _safe_float(response.get("avgPrice", response.get("price", 0)))
            if matched and (filled <= 0 or avg_price <= 0):
                filled, avg_price = _derive_fill(SELL, part, _safe_float(response.get("takingAmount")), _safe_float(response.get("makingAmount")), min_price or avg_price)
            if matched and filled > 0 and avg_price > 0:
                matched_count += 1
                filled_total += filled
                received_total += filled * avg_price
        if filled_total <= 1e-9:
            return ExecutionResult(False, order_id=",".join(order_ids) or None, message="live no fill: batch FAK no match", mode="live", latency_ms=latency_ms, total_latency_ms=latency_ms, timing={"batch_orders": len(batch), "matched_orders": matched_count})
        return ExecutionResult(True, order_id=",".join(order_ids) or None, filled_size=filled_total, avg_price=received_total / filled_total, message="batch matched", mode="live", latency_ms=latency_ms, total_latency_ms=latency_ms, timing={"batch_orders": len(batch), "matched_orders": matched_count})

    def _post(self, token_id: str, amount: float, side: str, price_hint: float | None) -> ExecutionResult:
        from py_clob_client_v2 import MarketOrderArgs, OrderType
        from py_clob_client_v2.order_builder.constants import BUY as SDK_BUY, SELL as SDK_SELL

        start = time.monotonic()
        client = get_client()
        sdk_side = SDK_BUY if side == BUY else SDK_SELL
        args = MarketOrderArgs(token_id=token_id, amount=amount, side=sdk_side, order_type=OrderType.FAK, price=price_hint or 0)
        signed = client.create_market_order(args, options=get_order_options(token_id))
        try:
            resp = client.post_order(signed, OrderType.FAK)
        except Exception as exc:
            latency_ms = round((time.monotonic() - start) * 1000)
            if _is_fak_no_match_error(exc):
                return ExecutionResult(
                    success=False,
                    order_id=_order_id_from_error(exc),
                    message="live no fill: FAK no match",
                    mode="live",
                    latency_ms=latency_ms,
                    total_latency_ms=latency_ms,
                )
            raise
        latency_ms = round((time.monotonic() - start) * 1000)
        order_id = resp.get("orderID") or resp.get("orderId") or resp.get("id")
        filled = _safe_float(resp.get("sizeFilled", resp.get("filledSize", 0)))
        avg_price = _safe_float(resp.get("avgPrice", resp.get("price", 0)))
        matched = _is_matched_response(resp)
        if matched and (filled <= 0 or avg_price <= 0):
            filled, avg_price = _derive_fill(side, amount, _safe_float(resp.get("takingAmount")), _safe_float(resp.get("makingAmount")), price_hint or avg_price)
        return ExecutionResult(
            success=matched,
            order_id=str(order_id) if order_id else None,
            filled_size=filled,
            avg_price=avg_price,
            message=str(resp.get("status", "")),
            mode="live",
            latency_ms=latency_ms,
            total_latency_ms=latency_ms,
        )
