"""Paper and live FAK execution gateways."""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field, replace
from typing import Any, Awaitable, Callable, Optional

from new_poly import config
from new_poly.market.stream import PriceStream
from .clob_client import get_client, get_order_options, get_token_balance, reset_clob_http_client
from .fak_quotes import buffer_buy_price_hint, get_tick_size

BUY = "BUY"
SELL = "SELL"
RECONCILE_BALANCE_POLL_TIMEOUT_SEC = 1.5
RECONCILE_BALANCE_POLL_INTERVAL_SEC = 0.25
UNKNOWN_BUY_RECONCILE_DELAYS_SEC = (3.0, 6.0, 9.0)


@dataclass(frozen=True)
class ExecutionConfig:
    paper_latency_sec: float = 0.0
    depth_notional: float = 5.0
    max_book_age_sec: float = 1.0
    retry_count: int = 1
    retry_interval_sec: float = 0.0
    buy_price_buffer_ticks: float = 2.0
    buy_retry_price_buffer_ticks: float = 4.0
    buy_dynamic_buffer_enabled: bool = True
    buy_dynamic_buffer_attempt1_max_ticks: float = 5.0
    buy_dynamic_buffer_attempt2_max_ticks: float = 8.0
    sell_price_buffer_ticks: float = 5.0
    sell_retry_price_buffer_ticks: float = 8.0
    sell_dynamic_buffer_enabled: bool = True
    sell_profit_exit_buffer_ticks: float = 5.0
    sell_profit_exit_retry_buffer_ticks: float = 8.0
    sell_risk_exit_buffer_ticks: float = 8.0
    sell_risk_exit_retry_buffer_ticks: float = 12.0
    sell_force_exit_buffer_ticks: float = 10.0
    sell_force_exit_retry_buffer_ticks: float = 15.0
    batch_exit_enabled: bool = False
    batch_exit_min_shares: float = 20.0
    batch_exit_min_notional_usd: float = 5.0
    batch_exit_slices: tuple[float, ...] = (0.4, 0.3, 1.0)
    batch_exit_extra_buffer_ticks: tuple[float, ...] = (0.0, 3.0, 6.0)
    live_min_sell_shares: float = 0.01
    live_min_sell_notional_usd: float = 0.0

    def normalization_warnings(self) -> tuple[str, ...]:
        warnings: list[str] = []
        if self.buy_retry_price_buffer_ticks < self.buy_price_buffer_ticks:
            warnings.append("buy_retry_price_buffer_ticks_clamped_to_buy_price_buffer_ticks")
        if self.sell_retry_price_buffer_ticks < self.sell_price_buffer_ticks:
            warnings.append("sell_retry_price_buffer_ticks_clamped_to_sell_price_buffer_ticks")
        if self.sell_profit_exit_retry_buffer_ticks < self.sell_profit_exit_buffer_ticks:
            warnings.append("sell_profit_exit_retry_buffer_ticks_clamped_to_sell_profit_exit_buffer_ticks")
        if self.sell_risk_exit_retry_buffer_ticks < self.sell_risk_exit_buffer_ticks:
            warnings.append("sell_risk_exit_retry_buffer_ticks_clamped_to_sell_risk_exit_buffer_ticks")
        if self.sell_force_exit_retry_buffer_ticks < self.sell_force_exit_buffer_ticks:
            warnings.append("sell_force_exit_retry_buffer_ticks_clamped_to_sell_force_exit_buffer_ticks")
        return tuple(warnings)

    def normalized(self) -> "ExecutionConfig":
        return replace(
            self,
            buy_retry_price_buffer_ticks=max(self.buy_price_buffer_ticks, self.buy_retry_price_buffer_ticks),
            sell_retry_price_buffer_ticks=max(self.sell_price_buffer_ticks, self.sell_retry_price_buffer_ticks),
            sell_profit_exit_retry_buffer_ticks=max(self.sell_profit_exit_buffer_ticks, self.sell_profit_exit_retry_buffer_ticks),
            sell_risk_exit_retry_buffer_ticks=max(self.sell_risk_exit_buffer_ticks, self.sell_risk_exit_retry_buffer_ticks),
            sell_force_exit_retry_buffer_ticks=max(self.sell_force_exit_buffer_ticks, self.sell_force_exit_retry_buffer_ticks),
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
    fatal_stop_reason: str | None = None


@dataclass(frozen=True)
class SellRetryParams:
    min_price: float | None
    exit_reason: str | None


@dataclass(frozen=True)
class BuyRetryParams:
    best_ask: float | None
    price_hint_base: float | None
    max_price: float | None


SellRetryRefresh = Callable[[int], Awaitable[Optional[SellRetryParams]]]
BuyRetryRefresh = Callable[[int], Awaitable[Optional[BuyRetryParams]]]


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


def _timed_token_balance(token_id: str) -> tuple[float | None, int]:
    start = time.monotonic()
    balance = get_token_balance(token_id, safe=True)
    return balance, _ms_since(start)


def _poll_token_balance(
    token_id: str,
    is_ready: Callable[[float], bool],
    *,
    timeout_sec: float | None = None,
    interval_sec: float | None = None,
) -> tuple[float | None, dict[str, Any]]:
    if timeout_sec is None:
        timeout_sec = RECONCILE_BALANCE_POLL_TIMEOUT_SEC
    if interval_sec is None:
        interval_sec = RECONCILE_BALANCE_POLL_INTERVAL_SEC
    start = time.monotonic()
    checks = 0
    query_ms = 0
    last_balance: float | None = None
    while True:
        balance, balance_ms = _timed_token_balance(token_id)
        checks += 1
        query_ms += balance_ms
        if balance is not None:
            last_balance = balance
            if is_ready(balance):
                break
        if time.monotonic() - start >= max(0.0, timeout_sec):
            break
        time.sleep(max(0.0, interval_sec))
    return last_balance, {
        "reconcile_balance_ms": query_ms,
        "reconcile_balance_poll_ms": _ms_since(start),
        "reconcile_balance_checks": checks,
    }


def _delayed_token_balance_checks(
    token_id: str,
    is_ready: Callable[[float], bool],
    *,
    delays_sec: tuple[float, ...],
    sent_at_epoch_ms: int | None,
) -> tuple[float | None, dict[str, Any]]:
    start = time.monotonic()
    now_epoch_ms = int(time.time() * 1000)
    if sent_at_epoch_ms is not None and abs(now_epoch_ms - sent_at_epoch_ms) <= 60_000:
        elapsed_from_send = max(0.0, (now_epoch_ms - sent_at_epoch_ms) / 1000.0)
    else:
        elapsed_from_send = 0.0
    checks = 0
    query_ms = 0
    last_balance: float | None = None
    balances: list[float | None] = []
    delays: list[float] = []
    for delay in delays_sec:
        wait_sec = max(0.0, float(delay) - elapsed_from_send - (time.monotonic() - start))
        time.sleep(wait_sec)
        balance, balance_ms = _timed_token_balance(token_id)
        checks += 1
        query_ms += balance_ms
        last_balance = balance if balance is not None else last_balance
        balances.append(round(balance, 6) if balance is not None else None)
        delays.append(float(delay))
        if balance is not None and is_ready(balance):
            break
    return last_balance, {
        "reconcile_balance_ms": query_ms,
        "reconcile_balance_poll_ms": _ms_since(start),
        "reconcile_balance_checks": checks,
        "reconcile_balance_delays_sec": delays,
        "reconcile_balance_values": balances,
    }


def _dynamic_buy_price_hint(
    token_id: str,
    best_ask: float | None,
    max_price: float | None,
    *,
    attempt: int,
    enabled: bool,
    fallback_buffer_ticks: float,
    attempt1_max_ticks: float,
    attempt2_max_ticks: float,
) -> float | None:
    if best_ask is None:
        return None
    if not enabled or max_price is None:
        return buffer_buy_price_hint(token_id, best_ask, buffer_ticks=fallback_buffer_ticks, max_price=max_price)
    tick = get_tick_size(token_id)
    if tick <= 0:
        tick = 0.01
    fair_room = max(0.0, max_price - best_ask)
    if fair_room <= 0:
        return buffer_buy_price_hint(token_id, best_ask, buffer_ticks=fallback_buffer_ticks, max_price=max_price)
    max_ticks = attempt1_max_ticks if attempt == 0 else attempt2_max_ticks
    desired = best_ask + max(0.0, max_ticks) * tick
    rounded = round(max(0.0, min(1.0, desired, max_price)), 6)
    return rounded


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


PROFIT_SELL_EXIT_REASONS = frozenset({
})

RISK_SELL_EXIT_REASONS = frozenset({
    "risk_exit",
    "late_ev_exit",
    "extreme_loss_exit",
})


def sell_aggression_ticks(
    exit_reason: str | None,
    attempt: int,
    *,
    sell_dynamic_buffer_enabled: bool = True,
    sell_price_buffer_ticks: float,
    sell_retry_price_buffer_ticks: float,
    sell_profit_exit_buffer_ticks: float = 5.0,
    sell_profit_exit_retry_buffer_ticks: float = 8.0,
    sell_risk_exit_buffer_ticks: float = 8.0,
    sell_risk_exit_retry_buffer_ticks: float = 12.0,
    sell_force_exit_buffer_ticks: float = 10.0,
    sell_force_exit_retry_buffer_ticks: float = 15.0,
) -> float:
    if not sell_dynamic_buffer_enabled:
        if exit_reason == "final_force_exit":
            return 5.0 if attempt == 0 else 10.0
        if exit_reason in PROFIT_SELL_EXIT_REASONS or exit_reason in RISK_SELL_EXIT_REASONS:
            return sell_price_buffer_ticks if attempt == 0 else sell_retry_price_buffer_ticks
        return 0.0
    if exit_reason == "final_force_exit":
        return sell_force_exit_buffer_ticks if attempt == 0 else sell_force_exit_retry_buffer_ticks
    if exit_reason in RISK_SELL_EXIT_REASONS:
        return sell_risk_exit_buffer_ticks if attempt == 0 else sell_risk_exit_retry_buffer_ticks
    if exit_reason in PROFIT_SELL_EXIT_REASONS:
        return sell_profit_exit_buffer_ticks if attempt == 0 else sell_profit_exit_retry_buffer_ticks
    return 0.0


def _sell_aggression_ticks(
    exit_reason: str | None,
    attempt: int,
    **kwargs,
) -> float:
    return sell_aggression_ticks(exit_reason, attempt, **kwargs)


def _sell_price_hint(
    token_id: str,
    min_price: float | None,
    exit_reason: str | None,
    attempt: int,
    *,
    sell_dynamic_buffer_enabled: bool = True,
    sell_price_buffer_ticks: float = 5.0,
    sell_retry_price_buffer_ticks: float = 8.0,
    sell_profit_exit_buffer_ticks: float = 5.0,
    sell_profit_exit_retry_buffer_ticks: float = 8.0,
    sell_risk_exit_buffer_ticks: float = 8.0,
    sell_risk_exit_retry_buffer_ticks: float = 12.0,
    sell_force_exit_buffer_ticks: float = 10.0,
    sell_force_exit_retry_buffer_ticks: float = 15.0,
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
        sell_dynamic_buffer_enabled=sell_dynamic_buffer_enabled,
        sell_price_buffer_ticks=sell_price_buffer_ticks,
        sell_retry_price_buffer_ticks=sell_retry_price_buffer_ticks,
        sell_profit_exit_buffer_ticks=sell_profit_exit_buffer_ticks,
        sell_profit_exit_retry_buffer_ticks=sell_profit_exit_retry_buffer_ticks,
        sell_risk_exit_buffer_ticks=sell_risk_exit_buffer_ticks,
        sell_risk_exit_retry_buffer_ticks=sell_risk_exit_retry_buffer_ticks,
        sell_force_exit_buffer_ticks=sell_force_exit_buffer_ticks,
        sell_force_exit_retry_buffer_ticks=sell_force_exit_retry_buffer_ticks,
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
    sell_dynamic_buffer_enabled: bool = True,
    sell_price_buffer_ticks: float = 5.0,
    sell_retry_price_buffer_ticks: float = 8.0,
    sell_profit_exit_buffer_ticks: float = 5.0,
    sell_profit_exit_retry_buffer_ticks: float = 8.0,
    sell_risk_exit_buffer_ticks: float = 8.0,
    sell_risk_exit_retry_buffer_ticks: float = 12.0,
    sell_force_exit_buffer_ticks: float = 10.0,
    sell_force_exit_retry_buffer_ticks: float = 15.0,
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
        sell_dynamic_buffer_enabled=sell_dynamic_buffer_enabled,
        sell_price_buffer_ticks=sell_price_buffer_ticks,
        sell_retry_price_buffer_ticks=sell_retry_price_buffer_ticks,
        sell_profit_exit_buffer_ticks=sell_profit_exit_buffer_ticks,
        sell_profit_exit_retry_buffer_ticks=sell_profit_exit_retry_buffer_ticks,
        sell_risk_exit_buffer_ticks=sell_risk_exit_buffer_ticks,
        sell_risk_exit_retry_buffer_ticks=sell_risk_exit_retry_buffer_ticks,
        sell_force_exit_buffer_ticks=sell_force_exit_buffer_ticks,
        sell_force_exit_retry_buffer_ticks=sell_force_exit_retry_buffer_ticks,
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
                sell_dynamic_buffer_enabled=self.config.sell_dynamic_buffer_enabled,
                sell_price_buffer_ticks=self.config.sell_price_buffer_ticks,
                sell_retry_price_buffer_ticks=self.config.sell_retry_price_buffer_ticks,
                sell_profit_exit_buffer_ticks=self.config.sell_profit_exit_buffer_ticks,
                sell_profit_exit_retry_buffer_ticks=self.config.sell_profit_exit_retry_buffer_ticks,
                sell_risk_exit_buffer_ticks=self.config.sell_risk_exit_buffer_ticks,
                sell_risk_exit_retry_buffer_ticks=self.config.sell_risk_exit_retry_buffer_ticks,
                sell_force_exit_buffer_ticks=self.config.sell_force_exit_buffer_ticks,
                sell_force_exit_retry_buffer_ticks=self.config.sell_force_exit_retry_buffer_ticks,
                # Paper uses BTC 5m's observed 0.01 tick for deterministic
                # replay; live mode still asks CLOB for the token tick size.
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
                            ExecutionResult(False, message="paper no fill: retry refresh unavailable", mode="paper"),
                            attempt=attempt,
                            total_latency_ms=total_latency_ms,
                        )
                    max_price = refreshed.max_price
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
            depth_max_age = None if exit_reason == "final_force_exit" else self.config.max_book_age_sec
            levels = self.stream.get_latest_bid_levels_with_size(token_id, max_age_sec=depth_max_age)
            book_read_ms += _ms_since(book_start)
            effective_min = _sell_price_hint(
                token_id,
                min_price,
                exit_reason,
                attempt,
                sell_dynamic_buffer_enabled=self.config.sell_dynamic_buffer_enabled,
                sell_price_buffer_ticks=self.config.sell_price_buffer_ticks,
                sell_retry_price_buffer_ticks=self.config.sell_retry_price_buffer_ticks,
                sell_profit_exit_buffer_ticks=self.config.sell_profit_exit_buffer_ticks,
                sell_profit_exit_retry_buffer_ticks=self.config.sell_profit_exit_retry_buffer_ticks,
                sell_risk_exit_buffer_ticks=self.config.sell_risk_exit_buffer_ticks,
                sell_risk_exit_retry_buffer_ticks=self.config.sell_risk_exit_retry_buffer_ticks,
                sell_force_exit_buffer_ticks=self.config.sell_force_exit_buffer_ticks,
                sell_force_exit_retry_buffer_ticks=self.config.sell_force_exit_retry_buffer_ticks,
                # Paper uses BTC 5m's observed 0.01 tick for deterministic
                # replay; live mode still asks CLOB for the token tick size.
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


def _trade_timestamp_ms(trade: dict[str, Any]) -> int | None:
    for key in ("timestamp", "match_time", "matchTime", "created_at", "createdAt", "last_update", "lastUpdate"):
        raw = trade.get(key)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        return int(value if value > 10_000_000_000 else value * 1000)
    return None


def _trade_asset_id(trade: dict[str, Any]) -> str:
    for key in ("asset_id", "assetId", "token_id", "tokenId", "asset"):
        value = trade.get(key)
        if value is not None:
            return str(value)
    return ""


def _trade_side(trade: dict[str, Any]) -> str:
    for key in ("side", "taker_side", "takerSide"):
        value = trade.get(key)
        if value is not None:
            return str(value).upper()
    return ""


def _trade_size(trade: dict[str, Any]) -> float:
    return _safe_float(trade.get("size", trade.get("amount", trade.get("filledSize", trade.get("filled_size", 0)))))


def _trade_price(trade: dict[str, Any]) -> float:
    return _safe_float(trade.get("price", trade.get("avgPrice", trade.get("avg_price", 0))))


def _recent_trade_fill(token_id: str, *, side: str, sent_at_epoch_ms: int | None, max_size: float) -> tuple[float, float, int]:
    if sent_at_epoch_ms is None or max_size <= 0:
        return 0.0, 0.0, 0
    try:
        from py_clob_client_v2 import TradeParams

        trades = get_client().get_trades(
            TradeParams(asset_id=token_id, after=max(0, int(sent_at_epoch_ms / 1000) - 10)),
            only_first_page=True,
        )
    except Exception:
        return 0.0, 0.0, 0
    if not isinstance(trades, list):
        return 0.0, 0.0, 0
    start_ms = sent_at_epoch_ms - 10_000
    filled = 0.0
    proceeds = 0.0
    count = 0
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        asset_id = _trade_asset_id(trade)
        if asset_id and asset_id != token_id:
            continue
        trade_side = _trade_side(trade)
        if trade_side and trade_side != side:
            continue
        ts_ms = _trade_timestamp_ms(trade)
        if ts_ms is not None and ts_ms < start_ms:
            continue
        size = _trade_size(trade)
        price = _trade_price(trade)
        if size <= 0 or price <= 0:
            continue
        take = min(size, max(0.0, max_size - filled))
        if take <= 0:
            break
        filled += take
        proceeds += take * price
        count += 1
    return filled, proceeds / filled if filled > 0 else 0.0, count


def _min_adopt_buy_shares(amount_usd: float) -> float:
    # Binary tokens are priced below 1 USDC, so a real BUY of N USDC should
    # produce more than N shares. Use a small cushion so rounding/dust does not
    # block adopting a confirmed-but-late balance after an unknown POST result.
    return max(1e-6, float(amount_usd) * 0.9)


def _buy_balance_price(amount_usd: float, shares: float, fallback_price: float | None) -> float:
    if shares > 0 and amount_usd > 0:
        return max(0.0, min(1.0, amount_usd / shares))
    return max(0.0, min(1.0, fallback_price or 0.0))


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


def _is_execution_rejected_error(exc: Exception) -> bool:
    return "could not run the execution" in str(exc).lower()


def _is_invalid_amount_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "invalid amounts" in text and "maker and taker amount" in text


def _is_insufficient_balance_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "not enough balance" in text or "not enough balance / allowance" in text


def _is_live_request_exception(exc: Exception) -> bool:
    text = str(exc).lower()
    if "request exception" in text:
        return True
    if "status_code=425" in text or "service not ready" in text:
        return True
    if "readtimeout" in text or "read operation timed out" in text or "timed out" in text:
        return True
    context = getattr(exc, "__context__", None) or getattr(exc, "__cause__", None)
    if context is not None and context is not exc:
        return _is_live_request_exception(context)
    return False


def _is_sell_execution_unknown(result: ExecutionResult) -> bool:
    text = (result.message or "").lower()
    return "request exception" in text or "sell balance unavailable" in text


def _is_buy_execution_unknown(result: ExecutionResult) -> bool:
    text = (result.message or "").lower()
    return "request exception" in text


def _order_id_from_error(exc: Exception) -> str | None:
    match = re.search(r"['\"]orderID['\"]\s*:\s*['\"]([^'\"]+)['\"]", str(exc))
    return match.group(1) if match else None


def _live_dust_sell_result(
    *,
    shares: float,
    min_price: float | None,
    min_sell_shares: float,
    min_sell_notional_usd: float,
) -> ExecutionResult | None:
    notional = shares * min_price if min_price is not None else None
    if min_sell_shares > 0 and shares < min_sell_shares:
        return ExecutionResult(
            False,
            message="live dust sell skipped: shares below minimum",
            mode="live",
            attempt=0,
            total_latency_ms=0,
            timing={
                "dust_shares": shares,
                "min_live_sell_shares": min_sell_shares,
                "dust_notional_usd": notional,
                "min_live_sell_notional_usd": min_sell_notional_usd,
            },
        )
    if min_sell_notional_usd > 0 and notional is not None and notional < min_sell_notional_usd:
        return ExecutionResult(
            False,
            message="live dust sell skipped: notional below minimum",
            mode="live",
            attempt=0,
            total_latency_ms=0,
            timing={
                "dust_shares": shares,
                "min_live_sell_shares": min_sell_shares,
                "dust_notional_usd": notional,
                "min_live_sell_notional_usd": min_sell_notional_usd,
            },
        )
    return None


class LiveFakExecutionGateway:
    def __init__(
        self,
        *,
        live_risk_ack: bool,
        retry_count: int = 1,
        retry_interval_sec: float = 0.0,
        buy_price_buffer_ticks: float = 2.0,
        buy_retry_price_buffer_ticks: float = 4.0,
        buy_dynamic_buffer_enabled: bool = True,
        buy_dynamic_buffer_attempt1_max_ticks: float = 5.0,
        buy_dynamic_buffer_attempt2_max_ticks: float = 8.0,
        sell_price_buffer_ticks: float = 5.0,
        sell_retry_price_buffer_ticks: float = 8.0,
        sell_dynamic_buffer_enabled: bool = True,
        sell_profit_exit_buffer_ticks: float = 5.0,
        sell_profit_exit_retry_buffer_ticks: float = 8.0,
        sell_risk_exit_buffer_ticks: float = 8.0,
        sell_risk_exit_retry_buffer_ticks: float = 12.0,
        sell_force_exit_buffer_ticks: float = 10.0,
        sell_force_exit_retry_buffer_ticks: float = 15.0,
        batch_exit_enabled: bool = False,
        batch_exit_min_shares: float = 20.0,
        batch_exit_min_notional_usd: float = 5.0,
        batch_exit_slices: tuple[float, ...] = (0.4, 0.3, 1.0),
        batch_exit_extra_buffer_ticks: tuple[float, ...] = (0.0, 3.0, 6.0),
        live_min_sell_shares: float = 0.01,
        live_min_sell_notional_usd: float = 0.0,
    ) -> None:
        if not live_risk_ack:
            raise ValueError("live mode requires --i-understand-live-risk")
        self.retry_count = max(0, int(retry_count))
        self.retry_interval_sec = max(0.0, float(retry_interval_sec))
        self.buy_price_buffer_ticks = max(0.0, float(buy_price_buffer_ticks))
        self.buy_retry_price_buffer_ticks = max(self.buy_price_buffer_ticks, float(buy_retry_price_buffer_ticks))
        self.buy_dynamic_buffer_enabled = bool(buy_dynamic_buffer_enabled)
        self.buy_dynamic_buffer_attempt1_max_ticks = max(0.0, float(buy_dynamic_buffer_attempt1_max_ticks))
        self.buy_dynamic_buffer_attempt2_max_ticks = max(0.0, float(buy_dynamic_buffer_attempt2_max_ticks))
        self.sell_price_buffer_ticks = max(0.0, float(sell_price_buffer_ticks))
        self.sell_retry_price_buffer_ticks = max(self.sell_price_buffer_ticks, float(sell_retry_price_buffer_ticks))
        self.sell_dynamic_buffer_enabled = bool(sell_dynamic_buffer_enabled)
        self.sell_profit_exit_buffer_ticks = max(0.0, float(sell_profit_exit_buffer_ticks))
        self.sell_profit_exit_retry_buffer_ticks = max(self.sell_profit_exit_buffer_ticks, float(sell_profit_exit_retry_buffer_ticks))
        self.sell_risk_exit_buffer_ticks = max(0.0, float(sell_risk_exit_buffer_ticks))
        self.sell_risk_exit_retry_buffer_ticks = max(self.sell_risk_exit_buffer_ticks, float(sell_risk_exit_retry_buffer_ticks))
        self.sell_force_exit_buffer_ticks = max(0.0, float(sell_force_exit_buffer_ticks))
        self.sell_force_exit_retry_buffer_ticks = max(self.sell_force_exit_buffer_ticks, float(sell_force_exit_retry_buffer_ticks))
        self.live_min_sell_shares = max(0.0, float(live_min_sell_shares))
        self.live_min_sell_notional_usd = max(0.0, float(live_min_sell_notional_usd))
        self.batch_config = ExecutionConfig(
            buy_dynamic_buffer_enabled=self.buy_dynamic_buffer_enabled,
            buy_dynamic_buffer_attempt1_max_ticks=self.buy_dynamic_buffer_attempt1_max_ticks,
            buy_dynamic_buffer_attempt2_max_ticks=self.buy_dynamic_buffer_attempt2_max_ticks,
            batch_exit_enabled=bool(batch_exit_enabled),
            batch_exit_min_shares=max(0.0, float(batch_exit_min_shares)),
            batch_exit_min_notional_usd=max(0.0, float(batch_exit_min_notional_usd)),
            batch_exit_slices=tuple(float(value) for value in batch_exit_slices),
            batch_exit_extra_buffer_ticks=tuple(float(value) for value in batch_exit_extra_buffer_ticks),
            sell_price_buffer_ticks=self.sell_price_buffer_ticks,
            sell_retry_price_buffer_ticks=self.sell_retry_price_buffer_ticks,
            sell_dynamic_buffer_enabled=self.sell_dynamic_buffer_enabled,
            sell_profit_exit_buffer_ticks=self.sell_profit_exit_buffer_ticks,
            sell_profit_exit_retry_buffer_ticks=self.sell_profit_exit_retry_buffer_ticks,
            sell_risk_exit_buffer_ticks=self.sell_risk_exit_buffer_ticks,
            sell_risk_exit_retry_buffer_ticks=self.sell_risk_exit_retry_buffer_ticks,
            sell_force_exit_buffer_ticks=self.sell_force_exit_buffer_ticks,
            sell_force_exit_retry_buffer_ticks=self.sell_force_exit_retry_buffer_ticks,
            live_min_sell_shares=self.live_min_sell_shares,
            live_min_sell_notional_usd=self.live_min_sell_notional_usd,
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
        starting_balance, starting_balance_ms = await asyncio.to_thread(_timed_token_balance, token_id)
        if starting_balance is not None and starting_balance >= _min_adopt_buy_shares(amount_usd):
            fallback_price = max_price or base_price
            return ExecutionResult(
                True,
                filled_size=starting_balance,
                avg_price=_buy_balance_price(amount_usd, starting_balance, fallback_price),
                message="live buy adopted existing token balance",
                mode="live",
                attempt=0,
                total_latency_ms=0,
                timing={
                    "reconciliation": "existing_balance_before_post",
                    "pre_balance_ms": starting_balance_ms,
                    "balance": round(starting_balance, 6),
                    "min_adopt_buy_shares": round(_min_adopt_buy_shares(amount_usd), 6),
                    "fallback_price": round(_buy_balance_price(amount_usd, starting_balance, fallback_price), 6),
                },
            )
        last = ExecutionResult(False, message="live buy not attempted", mode="live")
        start = time.monotonic()
        for attempt in range(self.retry_count + 1):
            if attempt > 0 and retry_refresh is not None:
                refreshed = await retry_refresh(attempt)
                if refreshed is None:
                    return _retry_skipped(
                        last,
                        attempt=attempt,
                        total_latency_ms=round((time.monotonic() - start) * 1000),
                    )
                best_ask = refreshed.best_ask
                price_hint_base = refreshed.price_hint_base
                max_price = refreshed.max_price
                base_price = price_hint_base if price_hint_base is not None else best_ask
            if starting_balance is not None:
                before_balance = starting_balance
                before_balance_ms = starting_balance_ms
            else:
                before_balance, before_balance_ms = await asyncio.to_thread(_timed_token_balance, token_id)
            buffer_ticks = self.buy_price_buffer_ticks if attempt == 0 else self.buy_retry_price_buffer_ticks
            price_hint = _dynamic_buy_price_hint(
                token_id,
                base_price,
                max_price,
                attempt=attempt,
                enabled=self.buy_dynamic_buffer_enabled,
                fallback_buffer_ticks=buffer_ticks,
                attempt1_max_ticks=self.buy_dynamic_buffer_attempt1_max_ticks,
                attempt2_max_ticks=self.buy_dynamic_buffer_attempt2_max_ticks,
            )
            last = await asyncio.to_thread(self._post, token_id, amount_usd, BUY, price_hint or max_price)
            last = replace(last, timing={**last.timing, "pre_balance_ms": before_balance_ms})
            if last.success:
                last = await asyncio.to_thread(
                    self._reconcile_successful_buy,
                    token_id,
                    before_balance,
                    amount_usd,
                    max_price,
                    price_hint,
                    last,
                )
            if not last.success and _is_buy_execution_unknown(last):
                reconciled = await asyncio.to_thread(
                    self._reconcile_unknown_buy,
                    token_id,
                    before_balance,
                    amount_usd,
                    max_price,
                    price_hint,
                    last,
                )
                if reconciled.success:
                    return _with_attempt(
                        reconciled,
                        attempt=attempt + 1,
                        total_latency_ms=round((time.monotonic() - start) * 1000),
                    )
                return _with_attempt(
                    reconciled,
                    attempt=attempt + 1,
                    total_latency_ms=round((time.monotonic() - start) * 1000),
                )
            if last.success or attempt >= self.retry_count:
                return _with_attempt(last, attempt=attempt + 1, total_latency_ms=round((time.monotonic() - start) * 1000))
            if self.retry_interval_sec > 0:
                await asyncio.sleep(self.retry_interval_sec)
        return last

    def _reconcile_successful_buy(
        self,
        token_id: str,
        before_balance: float | None,
        amount_usd: float,
        max_price: float | None,
        price_hint: float | None,
        last: ExecutionResult,
    ) -> ExecutionResult:
        after_balance, balance_ms = _timed_token_balance(token_id)
        if before_balance is None or after_balance is None:
            return replace(
                last,
                timing={**last.timing, "success_reconciliation": "balance_unavailable", "reconcile_balance_ms": balance_ms},
            )
        bought_by_balance = max(0.0, after_balance - before_balance)
        min_size = _min_adopt_buy_shares(amount_usd)
        if bought_by_balance < max(1e-9, min_size):
            return replace(
                last,
                timing={
                    **last.timing,
                    "success_reconciliation": "no_balance_increase",
                    "reconcile_balance_ms": balance_ms,
                    "balance_before": round(before_balance, 6),
                    "balance_after": round(after_balance, 6),
                    "min_adopt_buy_shares": round(min_size, 6),
                },
            )
        price = last.avg_price if last.avg_price > 0 else _buy_balance_price(amount_usd, bought_by_balance, max_price or price_hint)
        timing = {
            **last.timing,
            "reconciliation": "balance_increase_after_success",
            "reconcile_balance_ms": balance_ms,
            "balance_before": round(before_balance, 6),
            "balance_after": round(after_balance, 6),
            "bought_by_balance": round(bought_by_balance, 6),
            "response_fill_size": round(last.filled_size, 6),
            "fallback_price": round(price, 6),
        }
        if abs(bought_by_balance - last.filled_size) <= 1e-6 and last.avg_price > 0:
            return replace(last, timing=timing)
        return replace(
            last,
            filled_size=bought_by_balance,
            avg_price=price,
            message="live buy reconciled after successful POST response",
            timing=timing,
        )

    def _reconcile_unknown_buy(
        self,
        token_id: str,
        before_balance: float | None,
        amount_usd: float,
        max_price: float | None,
        price_hint: float | None,
        last: ExecutionResult,
    ) -> ExecutionResult:
        min_size = _min_adopt_buy_shares(amount_usd)

        def ready(balance: float) -> bool:
            if before_balance is None:
                return balance >= min_size
            bought = max(0.0, balance - before_balance)
            return bought >= min_size or balance >= min_size

        sent_at = last.timing.get("sent_at_epoch_ms") if isinstance(last.timing, dict) else None
        after_balance, balance_timing = _delayed_token_balance_checks(
            token_id,
            ready,
            delays_sec=UNKNOWN_BUY_RECONCILE_DELAYS_SEC,
            sent_at_epoch_ms=int(sent_at) if sent_at else None,
        )
        if after_balance is None:
            return replace(
                last,
                message=f"{last.message}; reconciliation no balance after delayed checks",
                timing={
                    **last.timing,
                    "reconciliation": "unknown_buy_no_balance_after_delayed_checks",
                    **balance_timing,
                    "balance_before": round(before_balance, 6) if before_balance is not None else None,
                    "balance_after": None,
                    "min_adopt_buy_shares": round(min_size, 6),
                },
            )
        bought_by_balance = max(0.0, after_balance - before_balance) if before_balance is not None else 0.0
        if bought_by_balance < max(1e-9, min_size) and after_balance < min_size:
            return replace(
                last,
                message=f"{last.message}; reconciliation no balance after delayed checks",
                timing={
                    **last.timing,
                    "reconciliation": "unknown_buy_no_balance_after_delayed_checks",
                    **balance_timing,
                    "balance_before": round(before_balance, 6) if before_balance is not None else None,
                    "balance_after": round(after_balance, 6),
                    "min_adopt_buy_shares": round(min_size, 6),
                },
            )
        reconciled_by = "balance_increase" if bought_by_balance >= min_size else "existing_balance"
        size_from_balance = bought_by_balance if bought_by_balance >= min_size else after_balance
        trade_start = time.monotonic()
        trade_size, trade_price, trade_count = _recent_trade_fill(token_id, side=BUY, sent_at_epoch_ms=int(sent_at) if sent_at else None, max_size=size_from_balance)
        trade_lookup_ms = _ms_since(trade_start)
        bought = trade_size if trade_size > 0 else size_from_balance
        price = trade_price if trade_price > 0 else _buy_balance_price(amount_usd, bought, max_price or price_hint)
        return replace(
            last,
            success=True,
            filled_size=bought,
            avg_price=price,
            message="live buy reconciled after unknown POST result",
            timing={
                **last.timing,
                "reconciliation": reconciled_by,
                **balance_timing,
                "reconcile_trade_lookup_ms": trade_lookup_ms,
                "balance_before": round(before_balance, 6) if before_balance is not None else None,
                "balance_after": round(after_balance, 6),
                "bought_by_balance": round(bought_by_balance, 6),
                "adopted_balance": round(size_from_balance, 6),
                "min_adopt_buy_shares": round(min_size, 6),
                "trade_fill_size": round(trade_size, 6),
                "trade_fill_price": round(trade_price, 6),
                "trade_count": trade_count,
                "fallback_price": round(price, 6),
            },
        )

    async def sell(
        self,
        token_id: str,
        shares: float,
        min_price: float | None = None,
        exit_reason: str | None = None,
        retry_refresh: SellRetryRefresh | None = None,
    ) -> ExecutionResult:
        amount = max(0.0, shares)
        if amount <= 0:
            return ExecutionResult(
                False,
                message="live no sellable balance",
                mode="live",
                attempt=0,
                total_latency_ms=0,
            )
        dust = _live_dust_sell_result(
            shares=amount,
            min_price=min_price,
            min_sell_shares=self.live_min_sell_shares,
            min_sell_notional_usd=self.live_min_sell_notional_usd,
        )
        if dust is not None:
            return dust
        last = ExecutionResult(False, message="live sell not attempted", mode="live")
        start = time.monotonic()
        for attempt in range(self.retry_count + 1):
            if attempt == 0 and retry_refresh is not None:
                refreshed = await retry_refresh(0)
                if refreshed is not None:
                    min_price = refreshed.min_price
                    exit_reason = refreshed.exit_reason
            before_balance = amount
            price_hint = _sell_price_hint(
                token_id,
                min_price,
                exit_reason,
                attempt,
                sell_dynamic_buffer_enabled=self.sell_dynamic_buffer_enabled,
                sell_price_buffer_ticks=self.sell_price_buffer_ticks,
                sell_retry_price_buffer_ticks=self.sell_retry_price_buffer_ticks,
                sell_profit_exit_buffer_ticks=self.sell_profit_exit_buffer_ticks,
                sell_profit_exit_retry_buffer_ticks=self.sell_profit_exit_retry_buffer_ticks,
                sell_risk_exit_buffer_ticks=self.sell_risk_exit_buffer_ticks,
                sell_risk_exit_retry_buffer_ticks=self.sell_risk_exit_retry_buffer_ticks,
                sell_force_exit_buffer_ticks=self.sell_force_exit_buffer_ticks,
                sell_force_exit_retry_buffer_ticks=self.sell_force_exit_retry_buffer_ticks,
            )
            if _should_batch_exit(amount, min_price, self.batch_config):
                last = await asyncio.to_thread(self._post_batch_sell, token_id, amount, min_price, exit_reason, attempt)
            else:
                last = await asyncio.to_thread(self._post, token_id, amount, SELL, price_hint or min_price)
            if last.success:
                last = await asyncio.to_thread(
                    self._reconcile_successful_sell,
                    token_id,
                    before_balance,
                    amount,
                    min_price,
                    price_hint,
                    last,
                )
            if not last.success and _is_sell_execution_unknown(last):
                reconciled = await asyncio.to_thread(
                    self._reconcile_unknown_sell,
                    token_id,
                    before_balance,
                    amount,
                    min_price,
                    price_hint,
                    last,
                )
                if reconciled.success:
                    return _with_attempt(
                        reconciled,
                        attempt=attempt + 1,
                        total_latency_ms=round((time.monotonic() - start) * 1000),
                    )
                last = reconciled
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
            balance, retry_balance_ms = await asyncio.to_thread(_timed_token_balance, token_id)
            last = replace(last, timing={**last.timing, "retry_balance_ms": retry_balance_ms})
            amount = min(shares, balance or 0.0)
            if amount <= 0:
                return _with_attempt(last, attempt=attempt + 1, total_latency_ms=round((time.monotonic() - start) * 1000))
            dust = _live_dust_sell_result(
                shares=amount,
                min_price=min_price,
                min_sell_shares=self.live_min_sell_shares,
                min_sell_notional_usd=self.live_min_sell_notional_usd,
            )
            if dust is not None:
                return _with_attempt(dust, attempt=attempt + 1, total_latency_ms=round((time.monotonic() - start) * 1000))
        return last

    def _reconcile_successful_sell(
        self,
        token_id: str,
        before_balance: float,
        attempted_amount: float,
        min_price: float | None,
        price_hint: float | None,
        last: ExecutionResult,
    ) -> ExecutionResult:
        after_balance, balance_ms = _timed_token_balance(token_id)
        if after_balance is None:
            return replace(
                last,
                timing={**last.timing, "success_reconciliation": "balance_unavailable", "reconcile_balance_ms": balance_ms},
            )
        sold_by_balance = max(0.0, before_balance - after_balance)
        min_reconcile_size = max(
            1e-9,
            min(self.live_min_sell_shares, attempted_amount) if self.live_min_sell_shares > 0 else 1e-9,
        )
        if last.filled_size > 0 and last.avg_price > 0 and sold_by_balance <= last.filled_size + 1e-6:
            return replace(
                last,
                timing={
                    **last.timing,
                    "success_reconciliation": "balance_lagged_response_trusted",
                    "reconcile_balance_ms": balance_ms,
                    "balance_before": round(before_balance, 6),
                    "balance_after": round(after_balance, 6),
                    "balance_decrease": round(sold_by_balance, 6),
                    "response_fill_size": round(last.filled_size, 6),
                    "response_avg_price": round(last.avg_price, 6),
                },
            )
        if sold_by_balance < min_reconcile_size:
            return replace(
                last,
                timing={
                    **last.timing,
                    "success_reconciliation": "no_balance_decrease",
                    "reconcile_balance_ms": balance_ms,
                    "balance_before": round(before_balance, 6),
                    "balance_after": round(after_balance, 6),
                },
            )
        sold = min(sold_by_balance, attempted_amount)
        price = last.avg_price if last.avg_price > 0 else max(0.0, price_hint or min_price or 0.0)
        timing = {
            **last.timing,
            "reconciliation": "balance_decrease_after_success",
            "reconcile_balance_ms": balance_ms,
            "balance_before": round(before_balance, 6),
            "balance_after": round(after_balance, 6),
            "sold_by_balance": round(sold_by_balance, 6),
            "response_fill_size": round(last.filled_size, 6),
            "fallback_price": round(price, 6),
        }
        if abs(sold - last.filled_size) <= 1e-6 and last.avg_price > 0:
            return replace(last, timing=timing)
        return replace(
            last,
            filled_size=sold,
            avg_price=price,
            message="live sell reconciled after successful POST response",
            timing=timing,
        )

    def _reconcile_unknown_sell(
        self,
        token_id: str,
        before_balance: float,
        attempted_amount: float,
        min_price: float | None,
        price_hint: float | None,
        last: ExecutionResult,
    ) -> ExecutionResult:
        min_reconcile_size = max(
            1e-9,
            min(self.live_min_sell_shares, attempted_amount) if self.live_min_sell_shares > 0 else 1e-9,
        )

        def ready(balance: float) -> bool:
            return max(0.0, before_balance - balance) >= min_reconcile_size

        after_balance, balance_timing = _poll_token_balance(token_id, ready)
        if after_balance is None:
            return replace(
                last,
                message=f"{last.message}; reconciliation balance unavailable",
                timing={**last.timing, "reconciliation": "balance_unavailable", **balance_timing},
            )
        sold_by_balance = max(0.0, before_balance - after_balance)
        if sold_by_balance < min_reconcile_size:
            return replace(
                last,
                message=f"{last.message}; reconciliation no balance decrease",
                timing={
                    **last.timing,
                    "reconciliation": "no_balance_decrease",
                    **balance_timing,
                    "balance_before": round(before_balance, 6),
                    "balance_after": round(after_balance, 6),
                },
            )
        sent_at = last.timing.get("sent_at_epoch_ms") if isinstance(last.timing, dict) else None
        trade_start = time.monotonic()
        trade_size, trade_price, trade_count = _recent_trade_fill(token_id, side=SELL, sent_at_epoch_ms=int(sent_at) if sent_at else None, max_size=sold_by_balance)
        trade_lookup_ms = _ms_since(trade_start)
        sold = trade_size if trade_size > 0 else sold_by_balance
        price = trade_price if trade_price > 0 else max(0.0, price_hint or min_price or 0.0)
        return replace(
            last,
            success=True,
            filled_size=sold,
            avg_price=price,
            message="live sell reconciled after unknown POST result",
            timing={
                **last.timing,
                "reconciliation": "balance_decrease",
                **balance_timing,
                "reconcile_trade_lookup_ms": trade_lookup_ms,
                "balance_before": round(before_balance, 6),
                "balance_after": round(after_balance, 6),
                "sold_by_balance": round(sold_by_balance, 6),
                "trade_fill_size": round(trade_size, 6),
                "trade_fill_price": round(trade_price, 6),
                "trade_count": trade_count,
                "fallback_price": round(max(0.0, price_hint or min_price or 0.0), 6),
            },
        )

    def _post_batch_sell(self, token_id: str, shares: float, min_price: float | None, exit_reason: str | None, attempt: int) -> ExecutionResult:
        from py_clob_client_v2 import MarketOrderArgs, OrderType
        from py_clob_client_v2.order_builder.constants import SELL as SDK_SELL

        start = time.monotonic()
        client_start = time.monotonic()
        client = get_client()
        get_client_ms = _ms_since(client_start)
        post_orders = getattr(client, "post_orders", None) or getattr(client, "postOrders", None)
        if post_orders is None:
            return self._post(token_id, shares, SELL, _sell_price_hint(
                token_id,
                min_price,
                exit_reason,
                attempt,
                sell_dynamic_buffer_enabled=self.sell_dynamic_buffer_enabled,
                sell_price_buffer_ticks=self.sell_price_buffer_ticks,
                sell_retry_price_buffer_ticks=self.sell_retry_price_buffer_ticks,
                sell_profit_exit_buffer_ticks=self.sell_profit_exit_buffer_ticks,
                sell_profit_exit_retry_buffer_ticks=self.sell_profit_exit_retry_buffer_ticks,
                sell_risk_exit_buffer_ticks=self.sell_risk_exit_buffer_ticks,
                sell_risk_exit_retry_buffer_ticks=self.sell_risk_exit_retry_buffer_ticks,
                sell_force_exit_buffer_ticks=self.sell_force_exit_buffer_ticks,
                sell_force_exit_retry_buffer_ticks=self.sell_force_exit_retry_buffer_ticks,
            ) or min_price)
        parts = _batch_exit_parts(shares, self.batch_config.batch_exit_slices)
        extra_ticks = self.batch_config.batch_exit_extra_buffer_ticks
        batch = []
        create_orders_ms = 0
        order_options_ms = 0
        for index, part in enumerate(parts[:15]):
            extra = extra_ticks[index] if index < len(extra_ticks) else extra_ticks[-1] if extra_ticks else 0.0
            price_hint = _sell_price_hint_with_extra(
                token_id,
                min_price,
                exit_reason,
                attempt,
                extra_buffer_ticks=extra,
                sell_dynamic_buffer_enabled=self.sell_dynamic_buffer_enabled,
                sell_price_buffer_ticks=self.sell_price_buffer_ticks,
                sell_retry_price_buffer_ticks=self.sell_retry_price_buffer_ticks,
                sell_profit_exit_buffer_ticks=self.sell_profit_exit_buffer_ticks,
                sell_profit_exit_retry_buffer_ticks=self.sell_profit_exit_retry_buffer_ticks,
                sell_risk_exit_buffer_ticks=self.sell_risk_exit_buffer_ticks,
                sell_risk_exit_retry_buffer_ticks=self.sell_risk_exit_retry_buffer_ticks,
                sell_force_exit_buffer_ticks=self.sell_force_exit_buffer_ticks,
                sell_force_exit_retry_buffer_ticks=self.sell_force_exit_retry_buffer_ticks,
            )
            args = MarketOrderArgs(token_id=token_id, amount=part, side=SDK_SELL, order_type=OrderType.FAK, price=price_hint or min_price or 0)
            options_start = time.monotonic()
            order_options = get_order_options(token_id)
            order_options_ms += _ms_since(options_start)
            create_start = time.monotonic()
            signed = client.create_market_order(args, options=order_options)
            create_orders_ms += _ms_since(create_start)
            batch.append({"order": signed, "orderType": OrderType.FAK})
        wall_start = time.time()
        post_start = time.monotonic()
        sent_at_epoch_ms = round(time.time() * 1000)
        try:
            responses = post_orders(batch)
            batch_post_orders_ms = _ms_since(post_start)
        except Exception as exc:
            response_at_epoch_ms = round(time.time() * 1000)
            batch_post_orders_ms = _ms_since(post_start)
            latency_ms = round((time.monotonic() - start) * 1000)
            timing = {
                "get_client_ms": get_client_ms,
                "batch_create_orders_ms": create_orders_ms,
                "batch_order_options_ms": order_options_ms,
                "batch_post_orders_ms": batch_post_orders_ms,
                "batch_orders": len(batch),
                "sent_at_epoch_ms": sent_at_epoch_ms,
                "response_at_epoch_ms": response_at_epoch_ms,
                "wall_latency_ms": response_at_epoch_ms - round(wall_start * 1000),
            }
            if _is_fak_no_match_error(exc):
                return ExecutionResult(False, message="live no fill: batch FAK no match", mode="live", latency_ms=latency_ms, total_latency_ms=latency_ms, timing=timing)
            if _is_execution_rejected_error(exc):
                return ExecutionResult(False, message="live no fill: batch execution rejected", mode="live", latency_ms=latency_ms, total_latency_ms=latency_ms, timing=timing)
            if _is_invalid_amount_error(exc):
                return ExecutionResult(False, message="live invalid amount", mode="live", latency_ms=latency_ms, total_latency_ms=latency_ms, timing=timing)
            if _is_insufficient_balance_error(exc):
                return ExecutionResult(False, message="live sell balance unavailable", mode="live", latency_ms=latency_ms, total_latency_ms=latency_ms, timing=timing)
            if _is_live_request_exception(exc):
                reset_clob_http_client()
                return ExecutionResult(False, message="live order request exception", mode="live", latency_ms=latency_ms, total_latency_ms=latency_ms, timing=timing)
            raise
        response_at_epoch_ms = round(time.time() * 1000)
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
        timing = {
            "get_client_ms": get_client_ms,
            "batch_create_orders_ms": create_orders_ms,
            "batch_order_options_ms": order_options_ms,
            "batch_post_orders_ms": batch_post_orders_ms,
            "batch_orders": len(batch),
            "matched_orders": matched_count,
            "sent_at_epoch_ms": sent_at_epoch_ms,
            "response_at_epoch_ms": response_at_epoch_ms,
            "wall_latency_ms": response_at_epoch_ms - round(wall_start * 1000),
        }
        if filled_total <= 1e-9:
            return ExecutionResult(False, order_id=",".join(order_ids) or None, message="live no fill: batch FAK no match", mode="live", latency_ms=latency_ms, total_latency_ms=latency_ms, timing=timing)
        return ExecutionResult(True, order_id=",".join(order_ids) or None, filled_size=filled_total, avg_price=received_total / filled_total, message="batch matched", mode="live", latency_ms=latency_ms, total_latency_ms=latency_ms, timing=timing)

    def _post(self, token_id: str, amount: float, side: str, price_hint: float | None) -> ExecutionResult:
        from py_clob_client_v2 import MarketOrderArgs, OrderType
        from py_clob_client_v2.order_builder.constants import BUY as SDK_BUY, SELL as SDK_SELL

        start = time.monotonic()
        wall_start = time.time()
        client_start = time.monotonic()
        client = get_client()
        get_client_ms = _ms_since(client_start)
        sdk_side = SDK_BUY if side == BUY else SDK_SELL
        args_start = time.monotonic()
        args = MarketOrderArgs(token_id=token_id, amount=amount, side=sdk_side, order_type=OrderType.FAK, price=price_hint or 0)
        build_args_ms = _ms_since(args_start)
        options_start = time.monotonic()
        order_options = get_order_options(token_id)
        order_options_ms = _ms_since(options_start)
        create_start = time.monotonic()
        signed = client.create_market_order(args, options=order_options)
        create_order_ms = _ms_since(create_start)
        post_start = time.monotonic()
        sent_at_epoch_ms = round(time.time() * 1000)
        try:
            resp = client.post_order(signed, OrderType.FAK)
        except Exception as exc:
            response_at_epoch_ms = round(time.time() * 1000)
            post_order_ms = _ms_since(post_start)
            latency_ms = round((time.monotonic() - start) * 1000)
            timing = {
                "get_client_ms": get_client_ms,
                "build_order_args_ms": build_args_ms,
                "order_options_ms": order_options_ms,
                "create_order_ms": create_order_ms,
                "post_order_ms": post_order_ms,
                "sent_at_epoch_ms": sent_at_epoch_ms,
                "response_at_epoch_ms": response_at_epoch_ms,
                "wall_latency_ms": response_at_epoch_ms - round(wall_start * 1000),
            }
            if _is_fak_no_match_error(exc):
                return ExecutionResult(
                    success=False,
                    order_id=_order_id_from_error(exc),
                    message="live no fill: FAK no match",
                    mode="live",
                    latency_ms=latency_ms,
                    total_latency_ms=latency_ms,
                    timing=timing,
                )
            if _is_execution_rejected_error(exc):
                return ExecutionResult(
                    success=False,
                    order_id=_order_id_from_error(exc),
                    message="live no fill: execution rejected",
                    mode="live",
                    latency_ms=latency_ms,
                    total_latency_ms=latency_ms,
                    timing=timing,
                )
            if _is_invalid_amount_error(exc):
                return ExecutionResult(
                    success=False,
                    order_id=_order_id_from_error(exc),
                    message="live invalid amount",
                    mode="live",
                    latency_ms=latency_ms,
                    total_latency_ms=latency_ms,
                    timing=timing,
                )
            if _is_insufficient_balance_error(exc):
                fatal_stop_reason = "live_insufficient_cash_balance" if side == BUY else None
                message = "live insufficient cash balance" if side == BUY else "live sell balance unavailable"
                return ExecutionResult(
                    success=False,
                    order_id=_order_id_from_error(exc),
                    message=message,
                    mode="live",
                    latency_ms=latency_ms,
                    total_latency_ms=latency_ms,
                    timing=timing,
                    fatal_stop_reason=fatal_stop_reason,
                )
            if _is_live_request_exception(exc):
                reset_clob_http_client()
                return ExecutionResult(
                    success=False,
                    order_id=_order_id_from_error(exc),
                    message="live order request exception",
                    mode="live",
                    latency_ms=latency_ms,
                    total_latency_ms=latency_ms,
                    timing=timing,
                )
            raise
        response_at_epoch_ms = round(time.time() * 1000)
        post_order_ms = _ms_since(post_start)
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
            timing={
                "get_client_ms": get_client_ms,
                "build_order_args_ms": build_args_ms,
                "order_options_ms": order_options_ms,
                "create_order_ms": create_order_ms,
                "post_order_ms": post_order_ms,
                "sent_at_epoch_ms": sent_at_epoch_ms,
                "response_at_epoch_ms": response_at_epoch_ms,
                "wall_latency_ms": response_at_epoch_ms - round(wall_start * 1000),
            },
        )
