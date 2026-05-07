"""Lifecycle helpers for the BTC 5m probability-edge bot."""

from __future__ import annotations

import asyncio
import datetime as dt
import time

from new_poly.bot_loop import DvolRuntime, FeedContext
from new_poly.bot_runtime import (
    BotConfig,
    DvolRefreshState,
    JsonlLogger,
    RuntimeOptions,
    _noop_price_update,
    _warmup_warning_row,
    fetch_valid_dvol_with_retries,
)
from new_poly.market.binance import BinancePriceFeed
from new_poly.market.coinbase import CoinbaseBtcPriceFeed
from new_poly.market.polymarket_live import PolymarketChainlinkBtcPriceFeed
from new_poly.market.stream import PriceStream
from new_poly.trading.execution import LiveFakExecutionGateway, PaperExecutionGateway


def create_feeds(cfg: BotConfig) -> FeedContext:
    polymarket_feed = (
        PolymarketChainlinkBtcPriceFeed(max_history_sec=15.0, stale_reconnect_sec=cfg.polymarket_stale_reconnect_sec)
        if cfg.polymarket_price_enabled
        else None
    )
    return FeedContext(
        binance=None,
        coinbase=None,
        polymarket=polymarket_feed,
        stream=PriceStream(on_price=_noop_price_update),
    )


def create_gateway(*, options: RuntimeOptions, cfg: BotConfig, feeds: FeedContext):
    if options.mode == "live":
        return LiveFakExecutionGateway(
            live_risk_ack=options.live_risk_ack,
            retry_count=cfg.execution.retry_count,
            retry_interval_sec=cfg.execution.retry_interval_sec,
            buy_price_buffer_ticks=cfg.execution.buy_price_buffer_ticks,
            buy_retry_price_buffer_ticks=cfg.execution.buy_retry_price_buffer_ticks,
            buy_dynamic_buffer_enabled=cfg.execution.buy_dynamic_buffer_enabled,
            buy_dynamic_buffer_attempt1_room_frac=cfg.execution.buy_dynamic_buffer_attempt1_room_frac,
            buy_dynamic_buffer_attempt2_room_frac=cfg.execution.buy_dynamic_buffer_attempt2_room_frac,
            buy_dynamic_buffer_attempt1_max_ticks=cfg.execution.buy_dynamic_buffer_attempt1_max_ticks,
            buy_dynamic_buffer_attempt2_max_ticks=cfg.execution.buy_dynamic_buffer_attempt2_max_ticks,
            buy_dynamic_buffer_min_reserved_edge=cfg.execution.buy_dynamic_buffer_min_reserved_edge,
            buy_dynamic_buffer_reserved_room_frac=cfg.execution.buy_dynamic_buffer_reserved_room_frac,
            sell_price_buffer_ticks=cfg.execution.sell_price_buffer_ticks,
            sell_retry_price_buffer_ticks=cfg.execution.sell_retry_price_buffer_ticks,
            batch_exit_enabled=cfg.execution.batch_exit_enabled,
            batch_exit_min_shares=cfg.execution.batch_exit_min_shares,
            batch_exit_min_notional_usd=cfg.execution.batch_exit_min_notional_usd,
            batch_exit_slices=cfg.execution.batch_exit_slices,
            batch_exit_extra_buffer_ticks=cfg.execution.batch_exit_extra_buffer_ticks,
            live_min_sell_shares=cfg.execution.live_min_sell_shares,
            live_min_sell_notional_usd=cfg.execution.live_min_sell_notional_usd,
        )
    return PaperExecutionGateway(stream=feeds.stream, config=cfg.execution)


async def startup_dvol_runtime(*, cfg: BotConfig, options: RuntimeOptions, logger: JsonlLogger) -> DvolRuntime | None:
    dvol = DvolRuntime(
        state=DvolRefreshState(),
        refresh_task=None,
        refresh_market_slug=None,
        next_refresh=time.monotonic() + cfg.dvol_refresh_sec,
    )
    startup_dvol = await fetch_valid_dvol_with_retries(
        retry_interval_sec=cfg.dvol_retry_interval_sec,
        max_retries=cfg.dvol_retry_attempts,
        on_retry=lambda attempt, snapshot, error: logger.write({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "event": "dvol_retry",
            "mode": options.mode,
            "phase": "startup",
            "attempt": attempt,
            "max_retries": cfg.dvol_retry_attempts,
            "retry_interval_sec": cfg.dvol_retry_interval_sec,
            "snapshot": snapshot.to_json() if snapshot is not None else None,
            "error": error,
        }),
    )
    if startup_dvol is None:
        logger.write({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "event": "dvol_startup_failed",
            "mode": options.mode,
            "max_retries": cfg.dvol_retry_attempts,
            "action": "stop",
        })
        return None
    dvol.state.apply_refresh_result(startup_dvol)
    logger.write({
        "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        "event": "dvol_ready",
        "mode": options.mode,
        "phase": "startup",
        "volatility": startup_dvol.to_json(),
    })
    return dvol


async def start_market_feeds(*, feeds: FeedContext, cfg: BotConfig, options: RuntimeOptions, logger: JsonlLogger, window) -> None:
    feeds.binance = BinancePriceFeed("btcusdt")
    await feeds.binance.start()
    if feeds.polymarket is not None:
        await feeds.polymarket.start()
    if cfg.coinbase_enabled and feeds.coinbase is None:
        feeds.coinbase = CoinbaseBtcPriceFeed()
        await feeds.coinbase.start()
    await feeds.stream.connect([window.up_token, window.down_token])


async def warmup_binance(*, feeds: FeedContext, cfg: BotConfig, options: RuntimeOptions, logger: JsonlLogger, market_slug: str) -> None:
    warmup_deadline = asyncio.get_running_loop().time() + max(0.0, cfg.warmup_timeout_sec)
    while asyncio.get_running_loop().time() < warmup_deadline:
        if feeds.binance is not None and feeds.binance.latest_price is not None:
            break
        await asyncio.sleep(0.1)
    if feeds.binance is None or feeds.binance.latest_price is None:
        logger.write(_warmup_warning_row(
            now=dt.datetime.now(dt.timezone.utc),
            mode=options.mode,
            market_slug=market_slug,
            unhealthy_log_after_sec=cfg.polymarket_unhealthy_log_after_sec,
        ))


async def close_runtime(*, feeds: FeedContext, dvol_task: asyncio.Task | None, logger: JsonlLogger) -> None:
    if dvol_task is not None:
        dvol_task.cancel()
        await asyncio.gather(dvol_task, return_exceptions=True)
    closers = [feeds.stream.close()]
    if feeds.binance is not None:
        closers.append(feeds.binance.stop())
    if feeds.polymarket is not None:
        closers.append(feeds.polymarket.stop())
    if feeds.coinbase is not None:
        closers.append(feeds.coinbase.stop())
    for closer in closers:
        try:
            await asyncio.wait_for(closer, timeout=5.0)
        except Exception:
            pass
    logger.close()
