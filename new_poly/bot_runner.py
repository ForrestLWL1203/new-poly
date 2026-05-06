"""Runtime runner for the BTC 5m probability-edge bot."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import replace

from new_poly.bot_loop import (
    DvolRuntime,
    LoopRuntime,
    WindowContext,
    _advance_dvol_refresh,
    _drain_dynamic_task,
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
from new_poly.market.series import MarketSeries
from new_poly.strategy.dynamic_params import (
    DynamicConfig,
    DynamicDecision,
    DynamicState,
    load_dynamic_config,
    load_dynamic_state,
    save_dynamic_state,
)
from new_poly.strategy.state import StrategyState


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
        self.dynamic_cfg: DynamicConfig | None = None
        self.dynamic_state: DynamicState | None = None
        self.dynamic_task: asyncio.Task[tuple[DynamicDecision, DynamicState]] | None = None
        self.dynamic_start_error: str | None = None
        self.series = MarketSeries.from_known("btc-updown-5m")
        self.feeds = None
        self.gateway = None
        self.dvol: DvolRuntime | None = None
        self.state = StrategyState()
        self.loop = LoopRuntime()
        self.window = None
        self.prices: WindowPrices | None = None

    def load_dynamic_params(self) -> None:
        if not self.options.dynamic_params:
            return
        try:
            self.dynamic_cfg = load_dynamic_config(self.options.dynamic_config)
            self.dynamic_state = load_dynamic_state(
                self.options.dynamic_state,
                default_profile=self.dynamic_cfg.active_profile,
            )
            if self.dynamic_state.active_profile not in self.dynamic_cfg.profile_names():
                self.dynamic_state = replace(
                    self.dynamic_state,
                    active_profile=self.dynamic_cfg.active_profile,
                    pending_profile=None,
                )
            self.cfg = _bot_config_with_edge(
                self.cfg,
                self.dynamic_cfg.profile(self.dynamic_state.active_profile).apply_to(self.cfg.edge),
            )
            self.options = replace(self.options, config=self.cfg)
            save_dynamic_state(self.options.dynamic_state, self.dynamic_state)
        except Exception as exc:
            self.dynamic_cfg = None
            self.dynamic_state = None
            self.dynamic_start_error = str(exc)

    async def run(self) -> int:
        try:
            self.load_dynamic_params()
            self.feeds = create_feeds(self.cfg)
            self.gateway = create_gateway(options=self.options, cfg=self.cfg, feeds=self.feeds)
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
        if self.dynamic_start_error is not None:
            self.logger.write({
                "ts": dt.datetime.now().astimezone().isoformat(),
                "event": "dynamic_error",
                "mode": self.options.mode,
                "error_type": "startup",
                "message": self.dynamic_start_error,
                "action": "keep_current",
            })
        self.dvol = await startup_dvol_runtime(cfg=self.cfg, options=self.options, logger=self.logger)
        if self.dvol is None:
            return False
        active = WindowContext(window=find_initial_window(self.series), prices=WindowPrices())
        self.window = active.window
        self.prices = active.prices
        self.state.reset_for_market(self.window.slug)
        await start_market_feeds(feeds=self.feeds, cfg=self.cfg, options=self.options, window=self.window)
        await warmup_binance(
            feeds=self.feeds,
            cfg=self.cfg,
            options=self.options,
            logger=self.logger,
            market_slug=self.window.slug,
        )
        return True

    async def run_loop(self) -> int:
        while True:
            should_stop = await self.run_tick()
            if should_stop:
                return 0
            await asyncio.sleep(self.cfg.interval_sec)
            if dt.datetime.now(dt.timezone.utc) >= self.window.end_time:
                should_stop = await self.close_window()
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
        self.dynamic_task, self.dynamic_state = await _drain_dynamic_task(
            dynamic_task=self.dynamic_task,
            dynamic_state=self.dynamic_state,
            logger=self.logger,
            options=self.options,
            window_slug=self.window.slug,
        )

    async def refresh_window_inputs(self) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        age_sec = (now - self.window.start_time).total_seconds()
        await _refresh_window_inputs(
            feeds=self.feeds,
            window=self.window,
            prices=self.prices,
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
            window_slug=self.window.slug,
        )

    def build_snapshot(self, sigma_eff):
        return _snapshot(
            self.window,
            self.prices,
            self.feeds.binance,
            self.feeds.coinbase,
            self.feeds.polymarket,
            self.feeds.stream,
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
                window=self.window,
                prices=self.prices,
                feeds=self.feeds,
                cfg=self.cfg,
                options=self.options,
                gateway=self.gateway,
                state=self.state,
                sigma_eff=sigma_eff,
                price_analysis=price_analysis,
            )
        return await _handle_flat_tick(
            row=row,
            snap=snap,
            window=self.window,
            prices=self.prices,
            feeds=self.feeds,
            cfg=self.cfg,
            options=self.options,
            gateway=self.gateway,
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

    async def close_window(self) -> bool:
        (
            self.window,
            self.prices,
            self.cfg,
            self.dynamic_state,
            self.dynamic_task,
            should_stop,
        ) = await _handle_window_close(
            window=self.window,
            prices=self.prices,
            cfg=self.cfg,
            options=self.options,
            feeds=self.feeds,
            state=self.state,
            loop=self.loop,
            logger=self.logger,
            series=self.series,
            dynamic_cfg=self.dynamic_cfg,
            dynamic_state=self.dynamic_state,
            dynamic_task=self.dynamic_task,
        )
        return should_stop

    async def close(self) -> None:
        if self.feeds is None:
            self.logger.close()
            return
        await close_runtime(
            feeds=self.feeds,
            dvol_task=self.dvol.refresh_task if self.dvol is not None else None,
            logger=self.logger,
        )


async def run_bot(options: RuntimeOptions) -> int:
    return await BotRunner(options).run()
