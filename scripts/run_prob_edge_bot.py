#!/usr/bin/env python3
"""Run the BTC 5m probability-edge strategy bot."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import sys
from dataclasses import replace
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

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
    build_arg_parser,
    build_runtime_options,
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


async def run(options: RuntimeOptions) -> int:
    cfg = options.config
    logger = JsonlLogger(options.jsonl, retention_hours=options.log_retention_hours)
    dynamic_cfg: DynamicConfig | None = None
    dynamic_state: DynamicState | None = None
    dynamic_task: asyncio.Task[tuple[DynamicDecision, DynamicState]] | None = None
    dynamic_start_error: str | None = None
    if options.dynamic_params:
        try:
            dynamic_cfg = load_dynamic_config(options.dynamic_config)
            dynamic_state = load_dynamic_state(options.dynamic_state, default_profile=dynamic_cfg.active_profile)
            if dynamic_state.active_profile not in dynamic_cfg.profile_names():
                dynamic_state = replace(dynamic_state, active_profile=dynamic_cfg.active_profile, pending_profile=None)
            cfg = _bot_config_with_edge(cfg, dynamic_cfg.profile(dynamic_state.active_profile).apply_to(cfg.edge))
            options = replace(options, config=cfg)
            save_dynamic_state(options.dynamic_state, dynamic_state)
        except Exception as exc:
            dynamic_cfg = None
            dynamic_state = None
            dynamic_start_error = str(exc)
    series = MarketSeries.from_known("btc-updown-5m")
    feeds = create_feeds(cfg)
    dvol: DvolRuntime | None = None
    state = StrategyState()
    loop = LoopRuntime()

    gateway = create_gateway(options=options, cfg=cfg, feeds=feeds)

    try:
        if options.analysis_logs:
            logger.write(_config_log_row(options))
        if dynamic_start_error is not None:
            logger.write({
                "ts": dt.datetime.now().astimezone().isoformat(),
                "event": "dynamic_error",
                "mode": options.mode,
                "error_type": "startup",
                "message": dynamic_start_error,
                "action": "keep_current",
            })
        dvol = await startup_dvol_runtime(cfg=cfg, options=options, logger=logger)
        if dvol is None:
            return 1
        active = WindowContext(window=find_initial_window(series), prices=WindowPrices())
        window = active.window
        prices = active.prices
        state.reset_for_market(window.slug)
        await start_market_feeds(feeds=feeds, cfg=cfg, options=options, window=window)
        await warmup_binance(feeds=feeds, cfg=cfg, options=options, logger=logger, market_slug=window.slug)

        while True:
            dynamic_task, dynamic_state = await _drain_dynamic_task(
                dynamic_task=dynamic_task,
                dynamic_state=dynamic_state,
                logger=logger,
                options=options,
                window_slug=window.slug,
            )
            now = dt.datetime.now(dt.timezone.utc)
            age_sec = (now - window.start_time).total_seconds()
            await _refresh_window_inputs(
                feeds=feeds,
                window=window,
                prices=prices,
                cfg=cfg,
                logger=logger,
                options=options,
                loop=loop,
                age_sec=age_sec,
            )
            sigma_eff, dvol_stale = await _advance_dvol_refresh(
                dvol=dvol,
                cfg=cfg,
                logger=logger,
                options=options,
                window_slug=window.slug,
            )
            snap, meta = _snapshot(
                window,
                prices,
                feeds.binance,
                feeds.coinbase,
                feeds.polymarket,
                feeds.stream,
                cfg,
                sigma_eff,
            )
            price_analysis = _price_analysis(meta)
            reference_meta = _reference_meta(meta)

            row: dict[str, Any] = {
                **_runtime_log_meta(meta),
                "mode": options.mode,
                "event": "tick",
                "sigma_source": dvol.state.current.source if dvol.state.current is not None else "missing",
                "sigma_eff": _compact(sigma_eff),
                "volatility_stale": dvol_stale,
                "position": _position_log(state.open_position, compact=True),
                "realized_pnl": _compact(state.realized_pnl, 4),
            }
            if state.has_position and state.open_position is not None:
                decision = await _handle_open_position_tick(
                    row=row,
                    snap=snap,
                    window=window,
                    prices=prices,
                    feeds=feeds,
                    cfg=cfg,
                    options=options,
                    gateway=gateway,
                    state=state,
                    sigma_eff=sigma_eff,
                    price_analysis=price_analysis,
                )
            else:
                decision = await _handle_flat_tick(
                    row=row,
                    snap=snap,
                    window=window,
                    prices=prices,
                    feeds=feeds,
                    cfg=cfg,
                    options=options,
                    gateway=gateway,
                    state=state,
                    sigma_eff=sigma_eff,
                    price_analysis=price_analysis,
                )

            if _should_attach_reference_meta(
                reference_meta,
                analysis_logs=options.analysis_logs,
                has_position=state.has_position,
                decision=decision,
            ):
                row["reference"] = reference_meta
            if _should_write_row(row, loop.seen_repetitive_skips):
                logger.write(row)
            if options.once:
                return 0
            await asyncio.sleep(cfg.interval_sec)
            if dt.datetime.now(dt.timezone.utc) >= window.end_time:
                window, prices, cfg, dynamic_state, dynamic_task, should_stop = await _handle_window_close(
                    window=window,
                    prices=prices,
                    cfg=cfg,
                    options=options,
                    feeds=feeds,
                    state=state,
                    loop=loop,
                    logger=logger,
                    series=series,
                    dynamic_cfg=dynamic_cfg,
                    dynamic_state=dynamic_state,
                    dynamic_task=dynamic_task,
                )
                if should_stop:
                    return 0
    except Exception as exc:
        logger.write({"ts": dt.datetime.now().astimezone().isoformat(), "event": "error", "error": str(exc)})
        return 1
    finally:
        await close_runtime(feeds=feeds, dvol_task=dvol.refresh_task if dvol is not None else None, logger=logger)


def main() -> int:
    try:
        options = build_runtime_options(build_arg_parser().parse_args())
    except Exception as exc:
        print(json.dumps({"event": "error", "error": str(exc)}, separators=(",", ":")), file=sys.stderr)
        return 2
    return asyncio.run(run(options))


if __name__ == "__main__":
    raise SystemExit(main())
