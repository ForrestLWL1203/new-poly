"""Paper and live FAK execution gateways."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from new_poly import config
from new_poly.market.stream import PriceStream
from .clob_client import get_client, get_order_options, get_token_balance
from .fak_quotes import buffer_buy_price_hint, buffer_sell_price_hint

BUY = "BUY"
SELL = "SELL"


@dataclass(frozen=True)
class ExecutionConfig:
    paper_latency_sec: float = 0.4
    depth_notional: float = 5.0
    max_book_age_sec: float = 1.0
    retry_count: int = 1
    retry_interval_sec: float = 0.2


@dataclass(frozen=True)
class ExecutionResult:
    success: bool
    order_id: str | None = None
    filled_size: float = 0.0
    avg_price: float = 0.0
    message: str = ""
    mode: str = "paper"
    latency_ms: int | None = None


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

    async def _delay(self) -> None:
        if self.before_fill is not None:
            await self.before_fill()
        if self.config.paper_latency_sec > 0:
            await asyncio.sleep(self.config.paper_latency_sec)

    async def buy(
        self,
        token_id: str,
        amount_usd: float,
        max_price: float | None = None,
        best_ask: float | None = None,
        price_hint_base: float | None = None,
    ) -> ExecutionResult:
        start = time.monotonic()
        await self._delay()
        levels = self.stream.get_latest_ask_levels_with_size(token_id, max_age_sec=self.config.max_book_age_sec)
        fill = _avg_buy_fill(levels, amount_usd, max_price)
        latency_ms = round((time.monotonic() - start) * 1000)
        if fill is None:
            return ExecutionResult(False, message="paper no fill: insufficient ask depth", latency_ms=latency_ms)
        shares, avg_price = fill
        return ExecutionResult(True, order_id="paper-buy", filled_size=shares, avg_price=avg_price, message="paper buy filled", latency_ms=latency_ms)

    async def sell(self, token_id: str, shares: float, min_price: float | None = None) -> ExecutionResult:
        start = time.monotonic()
        await self._delay()
        levels = self.stream.get_latest_bid_levels_with_size(token_id, max_age_sec=self.config.max_book_age_sec)
        fill = _avg_sell_fill(levels, shares, min_price)
        latency_ms = round((time.monotonic() - start) * 1000)
        if fill is None:
            return ExecutionResult(False, message="paper no fill: insufficient bid depth", latency_ms=latency_ms)
        sold, avg_price = fill
        return ExecutionResult(True, order_id="paper-sell", filled_size=sold, avg_price=avg_price, message="paper sell filled", latency_ms=latency_ms)


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


class LiveFakExecutionGateway:
    def __init__(self, *, live_risk_ack: bool) -> None:
        if not live_risk_ack:
            raise ValueError("live mode requires --i-understand-live-risk")

    async def buy(
        self,
        token_id: str,
        amount_usd: float,
        max_price: float | None = None,
        best_ask: float | None = None,
        price_hint_base: float | None = None,
    ) -> ExecutionResult:
        base_price = price_hint_base if price_hint_base is not None else best_ask
        price_hint = buffer_buy_price_hint(
            token_id,
            base_price,
            buffer_ticks=config.PRICE_HINT_BUFFER_TICKS,
            max_price=max_price,
        )
        return await asyncio.to_thread(self._post, token_id, amount_usd, BUY, price_hint or max_price)

    async def sell(self, token_id: str, shares: float, min_price: float | None = None) -> ExecutionResult:
        balance = await asyncio.to_thread(get_token_balance, token_id, safe=True)
        amount = min(shares, balance or 0.0)
        if amount <= 0:
            return ExecutionResult(False, message="live no sellable balance", mode="live")
        price_hint = buffer_sell_price_hint(token_id, min_price, min_price=min_price)
        return await asyncio.to_thread(self._post, token_id, amount, SELL, price_hint or min_price)

    def _post(self, token_id: str, amount: float, side: str, price_hint: float | None) -> ExecutionResult:
        from py_clob_client_v2 import MarketOrderArgs, OrderType
        from py_clob_client_v2.order_builder.constants import BUY as SDK_BUY, SELL as SDK_SELL

        start = time.monotonic()
        client = get_client()
        sdk_side = SDK_BUY if side == BUY else SDK_SELL
        args = MarketOrderArgs(token_id=token_id, amount=amount, side=sdk_side, order_type=OrderType.FAK, price=price_hint or 0)
        signed = client.create_market_order(args, options=get_order_options(token_id))
        resp = client.post_order(signed, OrderType.FAK)
        latency_ms = round((time.monotonic() - start) * 1000)
        order_id = resp.get("orderID") or resp.get("orderId") or resp.get("id")
        filled = _safe_float(resp.get("sizeFilled", resp.get("filledSize", 0)))
        avg_price = _safe_float(resp.get("avgPrice", resp.get("price", 0)))
        if resp.get("success") and str(resp.get("status", "")).upper() == "MATCHED" and (filled <= 0 or avg_price <= 0):
            filled, avg_price = _derive_fill(side, amount, _safe_float(resp.get("takingAmount")), _safe_float(resp.get("makingAmount")), price_hint or avg_price)
        return ExecutionResult(
            success=filled > 0,
            order_id=str(order_id) if order_id else None,
            filled_size=filled,
            avg_price=avg_price,
            message=str(resp.get("status", "")),
            mode="live",
            latency_ms=latency_ms,
        )
