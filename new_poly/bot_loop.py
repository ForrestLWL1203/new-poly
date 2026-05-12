"""Loop helpers for the BTC 5m probability-edge bot."""

from __future__ import annotations

import asyncio
import datetime as dt
import time
from dataclasses import dataclass
from typing import Any, Callable

from new_poly.bot_log_schema import _compact
from new_poly.bot_runtime import (
    BotConfig,
    DvolRefreshState,
    JsonlLogger,
    RuntimeOptions,
    WindowPrices,
    _polymarket_reference_recovered_row,
    _polymarket_reference_unhealthy_row,
    choose_settlement,
    effective_price,
    fetch_valid_dvol_with_retries,
    find_following_window,
    is_dvol_stale,
    make_volatility_fetcher,
    refresh_binance_open,
    refresh_coinbase_open,
    refresh_k_price,
    refresh_polymarket_open,
    volatility_refresh_interval_sec,
)
from new_poly.market.binance import BinancePriceFeed
from new_poly.market.coinbase import CoinbaseBtcPriceFeed
from new_poly.market.prob_edge_data import fetch_crypto_price_api
from new_poly.market.polymarket_live import PolymarketChainlinkBtcPriceFeed
from new_poly.market.series import MarketSeries
from new_poly.market.stream import PriceStream
from new_poly.strategy.dynamic_params import DynamicDecision, DynamicState
from new_poly.strategy.dynamic_params import save_dynamic_state
from new_poly.strategy.state import StrategyState
from new_poly.trading.clob_client import prefetch_order_params

@dataclass
class FeedContext:
    binance: BinancePriceFeed | None
    coinbase: CoinbaseBtcPriceFeed | None
    polymarket: PolymarketChainlinkBtcPriceFeed | None
    stream: PriceStream


@dataclass
class WindowContext:
    window: Any
    prices: WindowPrices


@dataclass
class DvolRuntime:
    state: DvolRefreshState
    refresh_task: asyncio.Task | None
    refresh_market_slug: str | None
    next_refresh: float


@dataclass
class WindowCloseResult:
    window: Any
    prices: WindowPrices
    cfg: BotConfig
    dynamic_state: DynamicState | None
    dynamic_task: asyncio.Task[tuple[DynamicDecision, DynamicState]] | None
    should_stop: bool


@dataclass
class PendingWindowSettlement:
    window: Any
    cfg: BotConfig
    options: RuntimeOptions
    due_at: dt.datetime


@dataclass
class LoopRuntime:
    completed_windows: int = 0
    seen_repetitive_skips: set[tuple[str, str]] | None = None
    polymarket_reference_warning_logged: bool = False
    polymarket_unhealthy_since: float | None = None
    pending_window_settlement: PendingWindowSettlement | None = None
    post_exit_observation_market_slug: str | None = None
    post_exit_observation_last_age_sec: float | None = None

    def __post_init__(self) -> None:
        if self.seen_repetitive_skips is None:
            self.seen_repetitive_skips = set()

    def reset_post_exit_observation(self) -> None:
        self.post_exit_observation_market_slug = None
        self.post_exit_observation_last_age_sec = None


async def _prefetch_live_order_params(*, token_side: str, token_id: str, market_slug: str, logger: JsonlLogger) -> dict[str, Any]:
    result = await asyncio.to_thread(prefetch_order_params, token_id, raise_on_error=False)
    if not result.get("ok"):
        logger.write({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "event": "clob_prefetch_failed",
            "market_slug": market_slug,
            "token_side": token_side,
            **result,
            "action": "continue_without_prefetch",
        })
    return result


async def _drain_dynamic_task(
    *,
    dynamic_task: asyncio.Task[tuple[DynamicDecision, DynamicState]] | None,
    dynamic_state: DynamicState | None,
    logger: JsonlLogger,
    options: RuntimeOptions,
    window_slug: str,
) -> tuple[asyncio.Task[tuple[DynamicDecision, DynamicState]] | None, DynamicState | None]:
    if dynamic_task is None or not dynamic_task.done():
        return dynamic_task, dynamic_state
    try:
        decision, dynamic_state = dynamic_task.result()
        if dynamic_state is not None:
            save_dynamic_state(options.dynamic_state, dynamic_state)
        logger.write(decision.to_log_row(
            mode=options.mode,
            window_id=window_slug,
            failed_health_checks=dynamic_state.failed_health_checks if dynamic_state is not None else 0,
        ))
    except Exception as exc:
        logger.write({
            "ts": dt.datetime.now().astimezone().isoformat(),
            "event": "dynamic_error",
            "mode": options.mode,
            "market_slug": window_slug,
            "error_type": type(exc).__name__,
            "message": str(exc),
            "action": "keep_current",
        })
    return None, dynamic_state


async def _refresh_window_inputs(
    *,
    feeds: FeedContext,
    window: Any,
    prices: WindowPrices,
    cfg: BotConfig,
    logger: JsonlLogger,
    options: RuntimeOptions,
    loop: LoopRuntime,
    age_sec: float,
) -> None:
    await refresh_k_price(window, prices, age_sec)
    if feeds.polymarket is not None:
        await refresh_polymarket_open(feeds.polymarket, window, prices, age_sec)
        pm_age = feeds.polymarket.latest_age_sec()
        pm_healthy = (
            feeds.polymarket.latest_price is not None
            and (pm_age is None or pm_age <= cfg.max_polymarket_price_age_sec)
        )
        if pm_healthy:
            if loop.polymarket_reference_warning_logged:
                logger.write(_polymarket_reference_recovered_row(
                    now=dt.datetime.now(dt.timezone.utc),
                    mode=options.mode,
                    market_slug=window.slug,
                ))
                loop.polymarket_reference_warning_logged = False
            loop.polymarket_unhealthy_since = None
        elif loop.polymarket_unhealthy_since is None:
            loop.polymarket_unhealthy_since = time.monotonic()
        if (
            not loop.polymarket_reference_warning_logged
            and loop.polymarket_unhealthy_since is not None
            and time.monotonic() - loop.polymarket_unhealthy_since >= cfg.polymarket_unhealthy_log_after_sec
        ):
            unhealthy_for_sec = time.monotonic() - loop.polymarket_unhealthy_since
            loop.polymarket_reference_warning_logged = True
            logger.write(_polymarket_reference_unhealthy_row(
                now=dt.datetime.now(dt.timezone.utc),
                mode=options.mode,
                market_slug=window.slug,
                unhealthy_for_sec=unhealthy_for_sec,
                coinbase_started=cfg.coinbase_enabled,
            ))
    if feeds.binance is not None:
        await refresh_binance_open(feeds.binance, window, prices, age_sec)
    if feeds.coinbase is not None:
        await refresh_coinbase_open(feeds.coinbase, window, prices, age_sec)


async def _advance_dvol_refresh(
    *,
    dvol: DvolRuntime,
    cfg: BotConfig,
    logger: JsonlLogger,
    options: RuntimeOptions,
    window_slug: str,
) -> tuple[float | None, bool]:
    if dvol.refresh_task is not None and dvol.refresh_task.done():
        try:
            refreshed = dvol.refresh_task.result()
        except Exception:
            refreshed = None
        was_stale = is_dvol_stale(dvol.state.current, now_wall=time.time(), max_age_sec=cfg.max_dvol_age_sec)
        failed_before = dvol.state.failed_refreshes
        if dvol.state.apply_refresh_result(refreshed):
            if was_stale or failed_before > 0:
                logger.write({
                    "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "event": "volatility_recovered",
                    "mode": options.mode,
                    "market_slug": dvol.refresh_market_slug or window_slug,
                    "volatility": dvol.state.current.to_json() if dvol.state.current is not None else None,
                })
        else:
            logger.write({
                "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                "event": "volatility_refresh_failed",
                "mode": options.mode,
                "market_slug": dvol.refresh_market_slug or window_slug,
                "failed_refreshes": dvol.state.failed_refreshes,
                "last_error": dvol.state.last_error,
                "kept_previous": dvol.state.current.to_json() if dvol.state.current is not None else None,
            })
        dvol.refresh_task = None
        dvol.refresh_market_slug = None
        dvol.next_refresh = time.monotonic() + volatility_refresh_interval_sec(cfg)
    if dvol.refresh_task is None and time.monotonic() >= dvol.next_refresh:
        refresh_market_slug = window_slug
        dvol.refresh_market_slug = refresh_market_slug
        dvol.refresh_task = asyncio.create_task(fetch_valid_dvol_with_retries(
            fetcher=make_volatility_fetcher(cfg),
            retry_interval_sec=cfg.dvol_retry_interval_sec,
            max_retries=cfg.dvol_retry_attempts,
            on_retry=lambda attempt, snapshot, error: logger.write({
                "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                "event": "volatility_retry",
                "mode": options.mode,
                "market_slug": refresh_market_slug,
                "phase": "refresh",
                "volatility_source": cfg.volatility_source,
                "attempt": attempt,
                "max_retries": cfg.dvol_retry_attempts,
                "retry_interval_sec": cfg.dvol_retry_interval_sec,
                "snapshot": snapshot.to_json() if snapshot is not None else None,
                "error": error,
            }),
        ))
    dvol_stale = is_dvol_stale(dvol.state.current, now_wall=time.time(), max_age_sec=cfg.max_dvol_age_sec)
    sigma_eff = None if dvol_stale or dvol.state.current is None else dvol.state.current.sigma
    return sigma_eff, dvol_stale


async def _settle_open_position_if_needed(
    *,
    window: Any,
    prices: WindowPrices,
    cfg: BotConfig,
    options: RuntimeOptions,
    feeds: FeedContext,
    state: StrategyState,
    logger: JsonlLogger,
) -> None:
    if not state.has_position or state.open_position is None:
        return
    settlement_price = effective_price(
        feeds.binance,
        feeds.coinbase,
        prices,
        coinbase_enabled=cfg.coinbase_enabled,
        polymarket_feed=feeds.polymarket,
        polymarket_enabled=cfg.polymarket_price_enabled,
    ).effective
    settlement = await _crypto_close_settlement(window, cfg)
    if settlement["winning_side"] is None:
        close_settlement = settlement
        settlement = choose_settlement(prices, settlement_price, boundary_usd=cfg.settlement_boundary_usd)
        settlement["settlement_open_price"] = close_settlement.get("settlement_open_price")
        settlement["settlement_close_price"] = close_settlement.get("settlement_close_price")
        settlement["settlement_completed"] = close_settlement.get("settlement_completed")
        settlement["settlement_cached"] = close_settlement.get("settlement_cached")
    settled_position = state.open_position
    if settlement["winning_side"] is not None:
        pnl = state.record_settlement(settlement["winning_side"])
    else:
        pnl = state.record_exit(settled_position.entry_avg_price, "unsettled_missing_price")
    row = {
        "ts": dt.datetime.now().astimezone().isoformat(),
        "mode": options.mode,
        "event": "settlement",
        "market_slug": window.slug,
        **settlement,
        "settlement_price": _compact(settlement.get("settlement_price"), 2),
        "settlement_open_price": _compact(settlement.get("settlement_open_price"), 2),
        "settlement_close_price": _compact(settlement.get("settlement_close_price"), 2),
        "settlement_proxy_price": _compact(settlement_price, 2),
        "k_price": _compact(prices.k_price, 2),
        "position": settled_position.__dict__,
        "settlement_pnl": _compact(pnl, 4),
        "realized_pnl": _compact(state.realized_pnl, 4),
    }
    risk_event = state.apply_closed_trade_risk(
        pnl,
        loss_limit=cfg.risk.consecutive_loss_limit,
        pause_windows=cfg.risk.loss_pause_windows,
    )
    if risk_event is not None:
        row["risk_event"] = risk_event
    logger.write(row)


async def _crypto_close_settlement(window: Any, cfg: BotConfig) -> dict[str, Any]:
    close_data = await asyncio.to_thread(fetch_crypto_price_api, window)
    close_price = close_data.get("closePrice") if close_data is not None else None
    open_price = close_data.get("openPrice") if close_data is not None else None
    base = {
        "settlement_open_price": open_price,
        "settlement_close_price": close_price,
        "settlement_completed": bool(close_data.get("completed")) if close_data is not None else None,
        "settlement_cached": bool(close_data.get("cached")) if close_data is not None else None,
    }
    if close_price is None or open_price is None:
        return {
            **base,
            "winning_side": None,
            "settlement_source": "polymarket_crypto_price_api_missing_close",
            "settlement_price": close_price,
            "settlement_uncertain": True,
        }
    return {
        **base,
        "winning_side": "up" if close_price >= open_price else "down",
        "settlement_source": "polymarket_crypto_price_api_close",
        "settlement_price": close_price,
        "settlement_uncertain": abs(close_price - open_price) < cfg.settlement_boundary_usd,
    }


async def _write_window_settlement_row(
    *,
    window: Any,
    cfg: BotConfig,
    options: RuntimeOptions,
    logger: JsonlLogger,
) -> None:
    settlement = await _crypto_close_settlement(window, cfg)
    logger.write({
        "ts": dt.datetime.now().astimezone().isoformat(),
        "mode": options.mode,
        "event": "window_settlement",
        "market_slug": window.slug,
        **settlement,
        "settlement_price": _compact(settlement.get("settlement_price"), 2),
        "settlement_open_price": _compact(settlement.get("settlement_open_price"), 2),
        "settlement_close_price": _compact(settlement.get("settlement_close_price"), 2),
    })


async def _write_pending_window_settlement_if_due(
    *,
    loop: LoopRuntime,
    logger: JsonlLogger,
    now: dt.datetime | None = None,
) -> None:
    pending = loop.pending_window_settlement
    if pending is None:
        return
    now = now or dt.datetime.now(dt.timezone.utc)
    if now < pending.due_at:
        return
    loop.pending_window_settlement = None
    await _write_window_settlement_row(
        window=pending.window,
        cfg=pending.cfg,
        options=pending.options,
        logger=logger,
    )


def _prune_logs_after_window_if_needed(
    *,
    loop: LoopRuntime,
    logger: JsonlLogger,
    options: RuntimeOptions,
) -> None:
    if loop.completed_windows <= 0 or loop.completed_windows % options.log_prune_every_windows != 0:
        return
    removed_log_rows = logger.prune()
    if not removed_log_rows:
        return
    logger.write({
        "ts": dt.datetime.now().astimezone().isoformat(),
        "event": "log_retention",
        "mode": options.mode,
        "retention_hours": options.log_retention_hours,
        "prune_every_windows": options.log_prune_every_windows,
        "removed_rows": removed_log_rows,
    })


async def _switch_to_next_window(
    *,
    window: Any,
    series: MarketSeries,
    feeds: FeedContext,
    state: StrategyState,
    loop: LoopRuntime,
    options: RuntimeOptions,
    logger: JsonlLogger,
    next_window: Any | None = None,
) -> tuple[Any, WindowPrices]:
    if next_window is None:
        next_window = find_following_window(window, series)
    prices = WindowPrices()
    state.reset_for_market(next_window.slug)
    loop.seen_repetitive_skips.clear()
    loop.reset_post_exit_observation()
    await asyncio.wait_for(feeds.stream.switch_tokens([next_window.up_token, next_window.down_token]), timeout=8.0)
    logger.write({
        "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        "event": "window_selected",
        "mode": options.mode,
        "market_slug": next_window.slug,
        "window_start": next_window.start_time.isoformat(),
        "window_end": next_window.end_time.isoformat(),
        "completed_windows": loop.completed_windows,
    })
    if options.mode == "live":
        await _prefetch_live_order_params(
            token_side="up",
            token_id=next_window.up_token,
            market_slug=next_window.slug,
            logger=logger,
        )
        await _prefetch_live_order_params(
            token_side="down",
            token_id=next_window.down_token,
            market_slug=next_window.slug,
            logger=logger,
        )
    return next_window, prices


async def _handle_window_close(
    *,
    window: Any,
    prices: WindowPrices,
    cfg: BotConfig,
    options: RuntimeOptions,
    feeds: FeedContext,
    state: StrategyState,
    loop: LoopRuntime,
    logger: JsonlLogger,
    series: MarketSeries,
    dynamic_state: DynamicState | None,
    dynamic_task: asyncio.Task[tuple[DynamicDecision, DynamicState]] | None,
    trigger_dynamic_analysis: Callable[[int, str, float, BotConfig], asyncio.Task[tuple[DynamicDecision, DynamicState]] | None] | None = None,
    apply_pending_dynamic_profile: Callable[[str, BotConfig], tuple[BotConfig, DynamicState | None]] | None = None,
) -> WindowCloseResult:
    if state.has_position:
        await _settle_open_position_if_needed(
            window=window,
            prices=prices,
            cfg=cfg,
            options=options,
            feeds=feeds,
            state=state,
            logger=logger,
        )
    if prices.k_price is not None:
        loop.completed_windows += 1
    pause_event = state.advance_loss_pause_after_window(window.slug)
    if pause_event is not None:
        logger.write({
            "ts": dt.datetime.now().astimezone().isoformat(),
            "mode": options.mode,
            **pause_event,
        })
    _prune_logs_after_window_if_needed(loop=loop, logger=logger, options=options)
    if trigger_dynamic_analysis is not None:
        dynamic_task = trigger_dynamic_analysis(loop.completed_windows, window.slug, state.drawdown, cfg)
    if options.windows is not None and loop.completed_windows >= options.windows:
        if not state.has_position:
            loop.pending_window_settlement = None
        return WindowCloseResult(window, prices, cfg, dynamic_state, dynamic_task, True)

    next_window = find_following_window(window, series)
    if not state.has_position and prices.k_price is not None:
        loop.pending_window_settlement = PendingWindowSettlement(
            window=window,
            cfg=cfg,
            options=options,
            due_at=next_window.start_time + dt.timedelta(seconds=30),
        )
    if apply_pending_dynamic_profile is not None:
        cfg, dynamic_state = apply_pending_dynamic_profile(next_window.slug, cfg)

    window, prices = await _switch_to_next_window(
        window=window,
        series=series,
        feeds=feeds,
        state=state,
        loop=loop,
        options=options,
        logger=logger,
        next_window=next_window,
    )
    return WindowCloseResult(window, prices, cfg, dynamic_state, dynamic_task, False)
