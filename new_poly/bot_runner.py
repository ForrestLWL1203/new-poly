"""Runtime runner for the BTC 5m probability-edge bot."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass, replace

from new_poly.bot_dynamic import DynamicParamController
from new_poly.bot_loop import (
    DvolRuntime,
    FeedContext,
    LoopRuntime,
    WindowContext,
    _advance_dvol_refresh,
    _handle_flat_tick,
    _handle_open_position_tick,
    _handle_window_close,
    _refresh_window_inputs,
)
from new_poly.bot_lifecycle import create_feeds, create_gateway, close_runtime, start_market_feeds, warmup_binance
from new_poly.bot_lifecycle import startup_dvol_runtime
from new_poly.bot_runtime import (
    JsonlLogger,
    RuntimeOptions,
    WindowPrices,
    _bot_config_with_edge,
    _compact,
    _config_log_row,
    _position_log,
    _price_analysis,
    _reference_meta,
    _runtime_log_meta,
    _should_attach_reference_meta,
    _should_write_row,
    _snapshot,
    find_initial_window,
)
from new_poly.market.market import MarketWindow
from new_poly.market.series import MarketSeries
from new_poly.strategy.state import StrategyState
from new_poly.trading.execution import LiveFakExecutionGateway, PaperExecutionGateway

Gateway = LiveFakExecutionGateway | PaperExecutionGateway


@dataclass
class StartupContext:
    feeds: FeedContext
    gateway: Gateway


@dataclass
class StartedContext:
    feeds: FeedContext
    gateway: Gateway
    window: MarketWindow
    prices: WindowPrices


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
        self.dynamic = DynamicParamController()
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
            self.options, self.cfg = self.dynamic.load(options=self.options, bot_config=self.cfg)
            feeds = create_feeds(self.cfg)
            gateway = create_gateway(options=self.options, cfg=self.cfg, feeds=feeds)
            self.startup_context = StartupContext(feeds=feeds, gateway=gateway)
            if not await self.start():
                return 1
            return await self.run_loop()
        except Exception as exc:
            self.logger.write({"ts": dt.datetime.now().astimezone().isoformat(), "event": "error", "error": str(exc)})
            return 1
        finally:
            await self.close()

    async def start(self) -> bool:
        if self.options.analysis_logs:
            self.logger.write(_config_log_row(self.options))
        self.dynamic.write_startup_error(logger=self.logger, options=self.options)
        self.dvol = await startup_dvol_runtime(cfg=self.cfg, options=self.options, logger=self.logger)
        if self.dvol is None:
            return False
        await self.start_first_window()
        return True

    async def start_first_window(self) -> None:
        self.set_window_context(WindowContext(window=find_initial_window(self.series), prices=WindowPrices()))
        await start_market_feeds(feeds=self.active.feeds, cfg=self.cfg, options=self.options, window=self.active.window)
        await warmup_binance(
            feeds=self.active.feeds,
            cfg=self.cfg,
            options=self.options,
            logger=self.logger,
            market_slug=self.active.window.slug,
        )

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
        await self.drain_dynamic_task()
        await self.refresh_window_inputs()
        sigma_eff, dvol_stale = await self.advance_dvol()
        snap, meta = self.build_snapshot(sigma_eff)
        price_analysis = _price_analysis(meta)
        reference_meta = _reference_meta(meta)
        row = self.build_tick_row(meta, sigma_eff=sigma_eff, dvol_stale=dvol_stale)
        decision = await self.handle_strategy_tick(
            row=row,
            snap=snap,
            sigma_eff=sigma_eff,
            price_analysis=price_analysis,
        )
        self.write_tick_row(row=row, reference_meta=reference_meta, decision=decision)
        return self.options.once

    async def drain_dynamic_task(self) -> None:
        await self.dynamic.drain(
            logger=self.logger,
            options=self.options,
            window_slug=self.active.window.slug,
        )

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

    async def advance_dvol(self):
        return await _advance_dvol_refresh(
            dvol=self.dvol,
            cfg=self.cfg,
            logger=self.logger,
            options=self.options,
            window_slug=self.active.window.slug,
        )

    def build_snapshot(self, sigma_eff):
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

    def build_tick_row(self, meta, *, sigma_eff, dvol_stale):
        return {
            **_runtime_log_meta(meta),
            "mode": self.options.mode,
            "event": "tick",
            "sigma_source": self.dvol.state.current.source if self.dvol.state.current is not None else "missing",
            "sigma_eff": _compact(sigma_eff),
            "volatility_stale": dvol_stale,
            "position": _position_log(self.state.open_position, compact=True),
            "realized_pnl": _compact(self.state.realized_pnl, 4),
        }

    async def handle_strategy_tick(self, *, row, snap, sigma_eff, price_analysis):
        if self.state.has_position and self.state.open_position is not None:
            return await _handle_open_position_tick(
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
            )
        return await _handle_flat_tick(
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
        )

    def write_tick_row(self, *, row, reference_meta, decision) -> None:
        if _should_attach_reference_meta(
            reference_meta,
            analysis_logs=self.options.analysis_logs,
            has_position=self.state.has_position,
            decision=decision,
        ):
            row["reference"] = reference_meta
        if _should_write_row(row, self.loop.seen_repetitive_skips):
            self.logger.write(row)

    async def roll_window(self) -> bool:
        (
            window,
            prices,
            self.cfg,
            dynamic_state,
            dynamic_task,
            should_stop,
        ) = await _handle_window_close(
            window=self.active.window,
            prices=self.active.prices,
            cfg=self.cfg,
            options=self.options,
            feeds=self.active.feeds,
            state=self.state,
            loop=self.loop,
            logger=self.logger,
            series=self.series,
            dynamic_cfg=self.dynamic.cfg,
            dynamic_state=self.dynamic.state,
            dynamic_task=self.dynamic.task,
        )
        self.dynamic.update_after_window_close(state=dynamic_state, task=dynamic_task)
        self.set_window_context(WindowContext(window=window, prices=prices))
        return should_stop

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
