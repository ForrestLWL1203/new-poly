"""Runtime runner for the BTC 5m poly-source bot."""

from __future__ import annotations

import asyncio
import datetime as dt
import traceback
from dataclasses import dataclass, replace
from typing import Any

from new_poly.bot_execution_flow import handle_flat_tick, handle_open_position_tick
from new_poly.bot_logging import build_tick_row, compact_high_frequency_row, write_tick_row
from new_poly.bot_loop import (
    DvolRuntime,
    FeedContext,
    LoopRuntime,
    WindowContext,
    _advance_dvol_refresh,
    _handle_window_close,
    _prefetch_live_order_params,
    _refresh_window_inputs,
    _write_pending_window_settlement_if_due,
)
from new_poly.bot_lifecycle import create_feeds, create_gateway, close_runtime, start_market_feeds
from new_poly.bot_runtime import (
    JsonlLogger,
    RuntimeOptions,
    WindowPrices,
    _config_log_row,
    _price_analysis,
    _reference_meta,
    _snapshot,
    find_initial_window,
)
from new_poly.market.market import MarketWindow
from new_poly.market.series import MarketSeries
from new_poly.strategy.types import MarketSnapshot, StrategyDecision
from new_poly.strategy.state import StrategyState
from new_poly.trading.execution import LiveFakExecutionGateway, PaperExecutionGateway

Gateway = LiveFakExecutionGateway | PaperExecutionGateway
PENDING_EXECUTION_WINDOW_CLOSE_TIMEOUT_SEC = 15.0


@dataclass(slots=True)
class StartupContext:
    feeds: FeedContext
    gateway: Gateway


@dataclass(slots=True)
class StartedContext:
    feeds: FeedContext
    gateway: Gateway
    window: MarketWindow
    prices: WindowPrices


@dataclass(slots=True)
class TickContext:
    sigma_eff: float | None
    dvol_stale: bool
    snap: MarketSnapshot
    meta: dict[str, Any]
    price_analysis: dict[str, Any]
    reference_meta: dict[str, Any]
    row: dict[str, Any]


class BotRunner:
    """Thin orchestration wrapper around the strategy loop.

    The runner owns process state, while strategy decisions and execution remain
    in library modules. Keeping this class intentionally small makes later
    lifecycle refactors safer without changing trading behavior.
    """

    def __init__(self, options: RuntimeOptions) -> None:
        self.options = options
        self.cfg = options.config
        self.logger = JsonlLogger(options.jsonl, retention_hours=options.log_retention_hours)
        self.series = MarketSeries.from_known("btc-updown-5m")
        self.startup_context: StartupContext | None = None
        self.context: StartedContext | None = None
        self.dvol: DvolRuntime | None = None
        self.state = StrategyState()
        self.loop = LoopRuntime()

    @property
    def active(self) -> StartedContext:
        if self.context is None:
            raise RuntimeError("bot runner is not started")
        return self.context

    @property
    def startup(self) -> StartupContext:
        if self.startup_context is None:
            raise RuntimeError("bot runner startup context is not initialized")
        return self.startup_context

    async def run(self) -> int:
        try:
            feeds = create_feeds(self.cfg)
            gateway = create_gateway(options=self.options, cfg=self.cfg, feeds=feeds)
            self.startup_context = StartupContext(feeds=feeds, gateway=gateway)
            if not await self.start():
                return 1
            return await self.run_loop()
        except Exception as exc:
            self.logger.write({
                "ts": dt.datetime.now().astimezone().isoformat(),
                "event": "error",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            })
            return 1
        finally:
            await self.close()

    async def start(self) -> bool:
        self.logger.write(_config_log_row(self.options))
        await self.start_first_window()
        return True

    async def start_first_window(self) -> None:
        self.set_window_context(WindowContext(window=find_initial_window(self.series), prices=WindowPrices()))
        self.logger.write({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "event": "window_selected",
            "mode": self.options.mode,
            "market_slug": self.active.window.slug,
            "window_start": self.active.window.start_time.isoformat(),
            "window_end": self.active.window.end_time.isoformat(),
        })
        await start_market_feeds(
            feeds=self.active.feeds,
            cfg=self.cfg,
            options=self.options,
            logger=self.logger,
            window=self.active.window,
        )
        self.logger.write({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "event": "market_feeds_started",
            "mode": self.options.mode,
            "market_slug": self.active.window.slug,
            "binance": self.active.feeds.binance is not None,
            "polymarket_reference": self.active.feeds.polymarket is not None,
            "coinbase": self.active.feeds.coinbase is not None,
            "clob_stream": True,
        })
        if self.options.mode == "live":
            for token_side, token_id in (("up", self.active.window.up_token), ("down", self.active.window.down_token)):
                self.logger.write({
                    "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "event": "clob_prefetch_started",
                    "mode": self.options.mode,
                    "market_slug": self.active.window.slug,
                    "token_side": token_side,
                })
                prefetch_result = await _prefetch_live_order_params(
                    token_side=token_side,
                    token_id=token_id,
                    market_slug=self.active.window.slug,
                    logger=self.logger,
                )
                if prefetch_result.get("ok"):
                    self.logger.write({
                        "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "event": "clob_prefetch_ready",
                        "mode": self.options.mode,
                        "market_slug": self.active.window.slug,
                        "token_side": token_side,
                    })

    def set_window_context(self, active: WindowContext) -> None:
        if self.context is None:
            self.context = StartedContext(
                feeds=self.startup.feeds,
                gateway=self.startup.gateway,
                window=active.window,
                prices=active.prices,
            )
        else:
            self.context.window = active.window
            self.context.prices = active.prices
        self.state.reset_for_market(self.active.window.slug)
        self.loop.reset_post_exit_observation()

    async def run_loop(self) -> int:
        while True:
            should_stop = await self.run_tick()
            if should_stop:
                return 0
            await asyncio.sleep(self.cfg.interval_sec)
            if dt.datetime.now(dt.timezone.utc) >= self.active.window.end_time:
                should_stop = await self.roll_window()
                if should_stop:
                    return 0

    async def run_tick(self) -> bool:
        await _write_pending_window_settlement_if_due(loop=self.loop, logger=self.logger)
        tick = await self.prepare_tick_context()
        decision = await self.handle_strategy_tick(
            row=tick.row,
            snap=tick.snap,
            sigma_eff=tick.sigma_eff,
            price_analysis=tick.price_analysis,
        )
        self.write_tick_context(tick, decision)
        self.write_post_exit_observation_if_due(tick)
        return self.options.once or self.state.fatal_stop_reason is not None

    async def prepare_tick_context(self) -> TickContext:
        await self.refresh_window_inputs()
        sigma_eff, dvol_stale = await self.advance_dvol()
        snap, meta = self.build_snapshot(sigma_eff)
        price_analysis = _price_analysis(meta, strategy_mode=self.cfg.strategy_mode)
        reference_meta = _reference_meta(meta, strategy_mode=self.cfg.strategy_mode)
        row = build_tick_row(
            meta,
            options=self.options,
            dvol=self.dvol,
            state=self.state,
            sigma_eff=sigma_eff,
            dvol_stale=dvol_stale,
        )
        return TickContext(
            sigma_eff=sigma_eff,
            dvol_stale=dvol_stale,
            snap=snap,
            meta=meta,
            price_analysis=price_analysis,
            reference_meta=reference_meta,
            row=row,
        )

    def write_tick_context(self, tick: TickContext, decision: StrategyDecision) -> None:
        write_tick_row(
            logger=self.logger,
            loop=self.loop,
            options=self.options,
            state=self.state,
            row=tick.row,
            reference_meta=tick.reference_meta,
            decision=decision,
        )

    def write_post_exit_observation_if_due(self, tick: TickContext) -> None:
        if not self.options.post_exit_observation_enabled:
            return
        if self.state.has_position or self.state.last_exit_age_sec is None:
            return
        market_slug = tick.snap.market_slug
        if self.state.current_market_slug != market_slug:
            return
        interval = max(1.0, float(self.options.post_exit_observation_interval_sec))
        age_sec = float(tick.snap.age_sec)
        remaining_sec = float(tick.snap.remaining_sec)
        if remaining_sec <= 0 or age_sec < float(self.state.last_exit_age_sec) + interval:
            return
        if self.loop.post_exit_observation_market_slug != market_slug:
            self.loop.post_exit_observation_market_slug = market_slug
            self.loop.post_exit_observation_last_age_sec = None
        last_age = self.loop.post_exit_observation_last_age_sec
        if last_age is not None and age_sec < last_age + interval:
            return

        row = {key: value for key, value in tick.row.items() if key != "_clob_ws"}
        row["event"] = "post_exit_observation"
        row["decision"] = {"action": "observe", "reason": "post_exit_observation"}
        row["last_exit_reason"] = self.state.last_exit_reason
        row["last_exit_side"] = self.state.last_exit_side
        row["last_exit_age_sec"] = self.state.last_exit_age_sec
        row["observation_interval_sec"] = interval
        if tick.reference_meta:
            row["reference"] = tick.reference_meta
        if tick.price_analysis:
            row["analysis"] = tick.price_analysis
        self.logger.write(compact_high_frequency_row(row, options=self.options))
        self.loop.post_exit_observation_last_age_sec = age_sec

    async def refresh_window_inputs(self) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        age_sec = (now - self.active.window.start_time).total_seconds()
        await _refresh_window_inputs(
            feeds=self.active.feeds,
            window=self.active.window,
            prices=self.active.prices,
            cfg=self.cfg,
            logger=self.logger,
            options=self.options,
            loop=self.loop,
            age_sec=age_sec,
        )

    async def advance_dvol(self) -> tuple[float | None, bool]:
        if self.dvol is None:
            return None, False
        return await _advance_dvol_refresh(
            dvol=self.dvol,
            cfg=self.cfg,
            logger=self.logger,
            options=self.options,
            window_slug=self.active.window.slug,
        )

    def build_snapshot(self, sigma_eff: float | None) -> tuple[MarketSnapshot, dict[str, Any]]:
        return _snapshot(
            self.active.window,
            self.active.prices,
            self.active.feeds.binance,
            self.active.feeds.coinbase,
            self.active.feeds.polymarket,
            self.active.feeds.stream,
            self.cfg,
            sigma_eff,
        )

    async def handle_strategy_tick(
        self,
        *,
        row: dict[str, Any],
        snap: MarketSnapshot,
        sigma_eff: float | None,
        price_analysis: dict[str, Any],
    ) -> StrategyDecision:
        if self.state.has_position and self.state.open_position is not None:
            return await handle_open_position_tick(
                row=row,
                snap=snap,
                window=self.active.window,
                prices=self.active.prices,
                feeds=self.active.feeds,
                cfg=self.cfg,
                options=self.options,
                gateway=self.active.gateway,
                state=self.state,
                sigma_eff=sigma_eff,
                price_analysis=price_analysis,
                logger=self.logger,
            )
        return await handle_flat_tick(
            row=row,
            snap=snap,
            window=self.active.window,
            prices=self.active.prices,
            feeds=self.active.feeds,
            cfg=self.cfg,
            options=self.options,
            gateway=self.active.gateway,
            state=self.state,
            sigma_eff=sigma_eff,
            price_analysis=price_analysis,
            logger=self.logger,
        )

    async def roll_window(self) -> bool:
        if self.state.pending_execution_task is not None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(self.state.pending_execution_task),
                    timeout=PENDING_EXECUTION_WINDOW_CLOSE_TIMEOUT_SEC,
                )
            except asyncio.TimeoutError:
                self.logger.write({
                    "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "event": "order_reconcile_timeout",
                    "mode": self.options.mode,
                    "market_slug": self.active.window.slug,
                    "pending_execution": self.state.pending_execution,
                    "action": "continue_window_close",
                })
        result = await _handle_window_close(
            window=self.active.window,
            prices=self.active.prices,
            cfg=self.cfg,
            options=self.options,
            feeds=self.active.feeds,
            state=self.state,
            loop=self.loop,
            logger=self.logger,
            series=self.series,
        )
        self.cfg = result.cfg
        self.set_window_context(WindowContext(window=result.window, prices=result.prices))
        return result.should_stop

    async def close(self) -> None:
        close_context = self.context or self.startup_context
        if close_context is None:
            self.logger.close()
            return
        await close_runtime(
            feeds=close_context.feeds,
            dvol_task=self.dvol.refresh_task if self.dvol is not None else None,
            logger=self.logger,
        )


async def run_bot(options: RuntimeOptions) -> int:
    return await BotRunner(options).run()
