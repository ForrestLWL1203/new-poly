#!/usr/bin/env python3
"""Run the BTC 5m probability-edge strategy bot."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from new_poly.bot_runtime import (
    BotConfig,
    DvolRefreshState,
    DEFAULT_CONFIG,
    DEFAULT_DYNAMIC_CONFIG,
    DEFAULT_DYNAMIC_STATE,
    JsonlLogger,
    RuntimeOptions,
    WindowPrices,
    _backtest_base_config,
    _bot_config_with_edge,
    _compact,
    _config_log_row,
    _decision_log,
    _dynamic_candidate_payload,
    _dynamic_health_payload,
    _entry_analysis,
    _exit_analysis,
    _noop_price_update,
    _polymarket_reference_recovered_row,
    _polymarket_reference_unhealthy_row,
    _position_log,
    _price_analysis,
    _reference_meta,
    _refresh_entry_retry_params,
    _refresh_exit_retry_params,
    _run_dynamic_analysis_task,
    _runtime_log_meta,
    _should_attach_reference_meta,
    _should_write_row,
    _snapshot,
    _warmup_warning_row,
    build_arg_parser,
    build_runtime_options,
    choose_settlement,
    effective_price,
    fetch_valid_dvol_with_retries,
    find_following_window,
    find_initial_window,
    is_dvol_stale,
    is_valid_dvol,
    load_bot_config,
    prune_jsonl_by_retention,
    refresh_binance_open,
    refresh_coinbase_open,
    refresh_k_price,
    refresh_polymarket_open,
)
from new_poly.market.binance import BinancePriceFeed
from new_poly.market.coinbase import CoinbaseBtcPriceFeed
from new_poly.market.polymarket_live import PolymarketChainlinkBtcPriceFeed
from new_poly.market.series import MarketSeries
from new_poly.market.stream import PriceStream
from new_poly.strategy.dynamic_params import (
    DynamicConfig,
    DynamicDecision,
    DynamicState,
    load_dynamic_config,
    load_dynamic_state,
    save_dynamic_state,
)
from new_poly.strategy.prob_edge import evaluate_entry, evaluate_exit
from new_poly.strategy.state import PositionSnapshot, StrategyState
from new_poly.trading.clob_client import prefetch_order_params
from new_poly.trading.execution import LiveFakExecutionGateway, PaperExecutionGateway


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
class LoopRuntime:
    completed_windows: int = 0
    seen_repetitive_skips: set[tuple[str, str]] | None = None
    polymarket_reference_warning_logged: bool = False
    polymarket_unhealthy_since: float | None = None

    def __post_init__(self) -> None:
        if self.seen_repetitive_skips is None:
            self.seen_repetitive_skips = set()


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
    polymarket_feed = (
        PolymarketChainlinkBtcPriceFeed(max_history_sec=15.0, stale_reconnect_sec=cfg.polymarket_stale_reconnect_sec)
        if cfg.polymarket_price_enabled
        else None
    )
    series = MarketSeries.from_known("btc-updown-5m")
    feeds = FeedContext(
        binance=None,
        coinbase=None,
        polymarket=polymarket_feed,
        stream=PriceStream(on_price=_noop_price_update),
    )
    dvol = DvolRuntime(
        state=DvolRefreshState(),
        refresh_task=None,
        refresh_market_slug=None,
        next_refresh=time.monotonic() + cfg.dvol_refresh_sec,
    )
    state = StrategyState()
    loop = LoopRuntime()

    gateway = (
        LiveFakExecutionGateway(
            live_risk_ack=options.live_risk_ack,
            retry_count=cfg.execution.retry_count,
            retry_interval_sec=cfg.execution.retry_interval_sec,
            buy_price_buffer_ticks=cfg.execution.buy_price_buffer_ticks,
            buy_retry_price_buffer_ticks=cfg.execution.buy_retry_price_buffer_ticks,
            sell_price_buffer_ticks=cfg.execution.sell_price_buffer_ticks,
            sell_retry_price_buffer_ticks=cfg.execution.sell_retry_price_buffer_ticks,
            batch_exit_enabled=cfg.execution.batch_exit_enabled,
            batch_exit_min_shares=cfg.execution.batch_exit_min_shares,
            batch_exit_min_notional_usd=cfg.execution.batch_exit_min_notional_usd,
            batch_exit_slices=cfg.execution.batch_exit_slices,
            batch_exit_extra_buffer_ticks=cfg.execution.batch_exit_extra_buffer_ticks,
        )
        if options.mode == "live"
        else PaperExecutionGateway(stream=feeds.stream, config=cfg.execution)
    )

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
            return 1
        dvol.state.apply_refresh_result(startup_dvol)
        logger.write({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "event": "dvol_ready",
            "mode": options.mode,
            "phase": "startup",
            "volatility": startup_dvol.to_json(),
        })
        active = WindowContext(window=find_initial_window(series), prices=WindowPrices())
        window = active.window
        prices = active.prices
        state.reset_for_market(window.slug)
        feeds.binance = BinancePriceFeed("btcusdt")
        await feeds.binance.start()
        if feeds.polymarket is not None:
            await feeds.polymarket.start()
        if cfg.coinbase_enabled and feeds.coinbase is None:
            feeds.coinbase = CoinbaseBtcPriceFeed()
            await feeds.coinbase.start()
        await feeds.stream.connect([window.up_token, window.down_token])
        if options.mode == "live":
            await asyncio.to_thread(prefetch_order_params, window.up_token)
            await asyncio.to_thread(prefetch_order_params, window.down_token)
        warmup_deadline = time.monotonic() + max(0.0, cfg.warmup_timeout_sec)
        while time.monotonic() < warmup_deadline:
            if feeds.binance.latest_price is not None:
                break
            await asyncio.sleep(0.1)
        if feeds.binance.latest_price is None:
            logger.write(_warmup_warning_row(
                now=dt.datetime.now(dt.timezone.utc),
                mode=options.mode,
                market_slug=window.slug,
                unhealthy_log_after_sec=cfg.polymarket_unhealthy_log_after_sec,
            ))

        while True:
            if dynamic_task is not None and dynamic_task.done():
                try:
                    decision, dynamic_state = dynamic_task.result()
                    if dynamic_state is not None:
                        save_dynamic_state(options.dynamic_state, dynamic_state)
                    logger.write(decision.to_log_row(
                        mode=options.mode,
                        window_id=window.slug,
                        failed_health_checks=dynamic_state.failed_health_checks if dynamic_state is not None else 0,
                    ))
                except Exception as exc:
                    logger.write({
                        "ts": dt.datetime.now().astimezone().isoformat(),
                        "event": "dynamic_error",
                        "mode": options.mode,
                        "market_slug": window.slug,
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                        "action": "keep_current",
                    })
                finally:
                    dynamic_task = None
            now = dt.datetime.now(dt.timezone.utc)
            age_sec = (now - window.start_time).total_seconds()
            await refresh_k_price(window, prices, age_sec)
            if feeds.polymarket is not None:
                await refresh_polymarket_open(feeds.polymarket, window, prices, age_sec)
                pm_age = feeds.polymarket.latest_age_sec()
                pm_healthy = feeds.polymarket.latest_price is not None and (pm_age is None or pm_age <= cfg.max_polymarket_price_age_sec)
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
            if dvol.refresh_task is not None and dvol.refresh_task.done():
                try:
                    refreshed = dvol.refresh_task.result()
                except Exception:
                    refreshed = None
                if dvol.state.apply_refresh_result(refreshed):
                    logger.write({
                        "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "event": "dvol_recovered",
                        "mode": options.mode,
                        "market_slug": dvol.refresh_market_slug or window.slug,
                        "volatility": dvol.state.current.to_json() if dvol.state.current is not None else None,
                    })
                else:
                    logger.write({
                        "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "event": "dvol_refresh_failed",
                        "mode": options.mode,
                        "market_slug": dvol.refresh_market_slug or window.slug,
                        "failed_refreshes": dvol.state.failed_refreshes,
                        "last_error": dvol.state.last_error,
                        "kept_previous": dvol.state.current.to_json() if dvol.state.current is not None else None,
                    })
                dvol.refresh_task = None
                dvol.refresh_market_slug = None
                dvol.next_refresh = time.monotonic() + cfg.dvol_refresh_sec
            if dvol.refresh_task is None and time.monotonic() >= dvol.next_refresh:
                refresh_market_slug = window.slug
                dvol.refresh_market_slug = refresh_market_slug
                dvol.refresh_task = asyncio.create_task(fetch_valid_dvol_with_retries(
                    retry_interval_sec=cfg.dvol_retry_interval_sec,
                    max_retries=cfg.dvol_retry_attempts,
                    on_retry=lambda attempt, snapshot, error: logger.write({
                        "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "event": "dvol_retry",
                        "mode": options.mode,
                        "market_slug": refresh_market_slug,
                        "phase": "refresh",
                        "attempt": attempt,
                        "max_retries": cfg.dvol_retry_attempts,
                        "retry_interval_sec": cfg.dvol_retry_interval_sec,
                        "snapshot": snapshot.to_json() if snapshot is not None else None,
                        "error": error,
                    }),
                ))
            dvol_stale = is_dvol_stale(dvol.state.current, now_monotonic=time.monotonic(), max_age_sec=cfg.max_dvol_age_sec)
            sigma_eff = None if dvol_stale or dvol.state.current is None else dvol.state.current.sigma
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
                decision = evaluate_exit(snap, state.open_position, cfg.edge, state)
                row["decision"] = _decision_log(decision)
                if decision.model_prob is not None:
                    state.record_model_prob(
                        snap.age_sec,
                        decision.model_prob,
                        retention_sec=max(cfg.edge.prob_stagnation_window_sec, cfg.edge.prob_drop_exit_window_sec, 5.0),
                    )
                if decision.action == "exit":
                    exiting_position = replace(state.open_position)
                    result = await gateway.sell(
                        state.open_position.token_id,
                        state.open_position.filled_shares,
                        min_price=decision.limit_price,
                        exit_reason=decision.reason,
                        retry_refresh=lambda attempt, position=exiting_position: _refresh_exit_retry_params(
                            window=window,
                            prices=prices,
                            feed=feeds.binance,
                            coinbase_feed=feeds.coinbase,
                            polymarket_feed=feeds.polymarket,
                            stream=feeds.stream,
                            cfg=cfg,
                            sigma_eff=sigma_eff,
                            state=state,
                            position=position,
                            exit_reason=decision.reason,
                        ),
                    )
                    row["order"] = result.__dict__
                    if options.analysis_logs:
                        row["analysis"] = {"price_sources": price_analysis, **_exit_analysis(decision, result)}
                    if result.success:
                        pnl, closed = state.record_partial_exit(result.avg_price, result.filled_size, decision.reason)
                        row["event"] = "exit" if closed else "partial_exit"
                        row["exit_reason"] = decision.reason
                        row["exit_price"] = _compact(result.avg_price)
                        row["exit_shares"] = _compact(result.filled_size)
                        row["exit_pnl"] = _compact(pnl, 4)
                        if options.analysis_logs:
                            row["position_before_exit"] = _position_log(exiting_position, compact=False)
                            row["position_after_exit"] = _position_log(state.open_position, compact=False)
                    else:
                        row["event"] = "order_no_fill"
                        row["order_intent"] = "exit"
                        if options.analysis_logs:
                            row["analysis"] = {"price_sources": price_analysis, **_exit_analysis(decision, result)}
            else:
                decision = evaluate_entry(snap, state, cfg.edge)
                row["decision"] = _decision_log(decision)
                if decision.action == "enter":
                    token_id = window.up_token if decision.side == "up" else window.down_token
                    result = await gateway.buy(
                        token_id,
                        cfg.amount_usd,
                        max_price=decision.limit_price,
                        best_ask=decision.best_ask,
                        price_hint_base=decision.depth_limit_price,
                        retry_refresh=lambda attempt, side=decision.side: _refresh_entry_retry_params(
                            window=window,
                            prices=prices,
                            feed=feeds.binance,
                            coinbase_feed=feeds.coinbase,
                            polymarket_feed=feeds.polymarket,
                            stream=feeds.stream,
                            cfg=cfg,
                            sigma_eff=sigma_eff,
                            state=state,
                            original_side=side,
                        ),
                    )
                    row["order"] = result.__dict__
                    if options.analysis_logs:
                        row["analysis"] = {"price_sources": price_analysis, **_entry_analysis(decision, result)}
                    if result.success and decision.side is not None and decision.model_prob is not None and decision.edge is not None:
                        state.record_entry(PositionSnapshot(
                            market_slug=window.slug,
                            token_side=decision.side,
                            token_id=token_id,
                            entry_time=snap.age_sec,
                            entry_avg_price=result.avg_price,
                            filled_shares=result.filled_size,
                            entry_model_prob=decision.model_prob,
                            entry_edge=decision.edge,
                        ))
                        row["event"] = "entry"
                        row["entry_side"] = decision.side
                        row["entry_price"] = _compact(result.avg_price)
                        row["entry_shares"] = _compact(result.filled_size)
                        if options.analysis_logs and state.open_position is not None:
                            row["position_after_entry"] = _position_log(state.open_position, compact=False)
                    else:
                        row["event"] = "order_no_fill"
                        row["order_intent"] = "entry"
                        if options.analysis_logs:
                            row["analysis"] = {"price_sources": price_analysis, **_entry_analysis(decision, result)}

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
                if state.has_position and state.open_position is not None:
                    settlement_price = effective_price(
                        feeds.binance,
                        feeds.coinbase,
                        prices,
                        coinbase_enabled=cfg.coinbase_enabled,
                        polymarket_feed=feeds.polymarket,
                        polymarket_enabled=cfg.polymarket_price_enabled,
                    ).effective
                    settlement = choose_settlement(prices, settlement_price, boundary_usd=cfg.settlement_boundary_usd)
                    settled_position = state.open_position
                    if settlement["winning_side"] is not None:
                        pnl = state.record_settlement(settlement["winning_side"])
                    else:
                        pnl = state.record_exit(settled_position.entry_avg_price, "unsettled_missing_price")
                    logger.write({
                        "ts": dt.datetime.now().astimezone().isoformat(),
                        "mode": options.mode,
                        "event": "settlement",
                        "market_slug": window.slug,
                        **settlement,
                        "settlement_price": _compact(settlement.get("settlement_price"), 2),
                        "settlement_proxy_price": _compact(settlement_price, 2),
                        "k_price": _compact(prices.k_price, 2),
                        "position": settled_position.__dict__,
                        "settlement_pnl": _compact(pnl, 4),
                        "realized_pnl": _compact(state.realized_pnl, 4),
                    })
                if prices.k_price is not None:
                    loop.completed_windows += 1
                if loop.completed_windows > 0 and loop.completed_windows % options.log_prune_every_windows == 0:
                    removed_log_rows = logger.prune()
                    if removed_log_rows:
                        logger.write({
                            "ts": dt.datetime.now().astimezone().isoformat(),
                            "event": "log_retention",
                            "mode": options.mode,
                            "retention_hours": options.log_retention_hours,
                            "prune_every_windows": options.log_prune_every_windows,
                            "removed_rows": removed_log_rows,
                        })
                if (
                    dynamic_cfg is not None
                    and dynamic_state is not None
                    and options.jsonl is not None
                    and loop.completed_windows > 0
                    and loop.completed_windows % dynamic_cfg.check_every_windows == 0
                    and dynamic_task is None
                ):
                    dynamic_task = asyncio.create_task(_run_dynamic_analysis_task(
                        jsonl_path=options.jsonl,
                        dynamic_cfg=dynamic_cfg,
                        dynamic_state=dynamic_state,
                        base_config=_backtest_base_config(cfg),
                        mode=options.mode,
                        current_window_id=window.slug,
                        realized_drawdown=state.drawdown,
                    ))
                elif dynamic_cfg is not None and dynamic_state is not None and options.jsonl is None and loop.completed_windows > 0 and loop.completed_windows % dynamic_cfg.check_every_windows == 0:
                    logger.write({
                        "ts": dt.datetime.now().astimezone().isoformat(),
                        "event": "dynamic_error",
                        "mode": options.mode,
                        "market_slug": window.slug,
                        "error_type": "missing_jsonl",
                        "message": "--dynamic-params requires --jsonl for analysis",
                        "action": "keep_current",
                    })
                if options.windows is not None and loop.completed_windows >= options.windows:
                    return 0
                next_window = find_following_window(window, series)
                if dynamic_cfg is not None and dynamic_state is not None and dynamic_state.pending_profile is not None:
                    try:
                        old_profile = dynamic_state.active_profile
                        old_edge = cfg.edge
                        profile = dynamic_cfg.profile(dynamic_state.pending_profile)
                        cfg = _bot_config_with_edge(cfg, profile.apply_to(cfg.edge))
                        now_ts = dt.datetime.now(dt.timezone.utc).astimezone().isoformat()
                        history = list(dynamic_state.switch_history)
                        history.append({
                            "from_profile": old_profile,
                            "to_profile": profile.name,
                            "applied_at_window": next_window.slug,
                            "switched_at_ts": now_ts,
                            "health_check": dynamic_state.last_check_result,
                        })
                        dynamic_state = replace(
                            dynamic_state,
                            active_profile=profile.name,
                            pending_profile=None,
                            switched_at_window_id=next_window.slug,
                            switched_at_ts=now_ts,
                            switch_history=history,
                        )
                        save_dynamic_state(options.dynamic_state, dynamic_state)
                        logger.write({
                            "ts": now_ts,
                            "event": "config_update",
                            "mode": options.mode,
                            "from_profile": old_profile,
                            "to_profile": profile.name,
                            "applied_at_window": next_window.slug,
                            "reason": "dynamic_params",
                            "health_check": _dynamic_health_payload(dynamic_state.last_check_result),
                            "candidate_results": _dynamic_candidate_payload(dynamic_state.last_check_result),
                            "old_signal_params": {
                                "entry_start_age_sec": old_edge.entry_start_age_sec,
                                "entry_end_age_sec": old_edge.entry_end_age_sec,
                                "early_required_edge": old_edge.early_required_edge,
                                "core_required_edge": old_edge.core_required_edge,
                                "max_entries_per_market": old_edge.max_entries_per_market,
                            },
                            "new_signal_params": profile.signal_params(),
                        })
                    except Exception as exc:
                        logger.write({
                            "ts": dt.datetime.now().astimezone().isoformat(),
                            "event": "dynamic_error",
                            "mode": options.mode,
                            "market_slug": next_window.slug,
                            "error_type": type(exc).__name__,
                            "message": str(exc),
                            "action": "keep_current",
                        })
                active = WindowContext(window=next_window, prices=WindowPrices())
                window = active.window
                prices = active.prices
                state.reset_for_market(window.slug)
                loop.seen_repetitive_skips.clear()
                await asyncio.wait_for(feeds.stream.switch_tokens([window.up_token, window.down_token]), timeout=8.0)
                if options.mode == "live":
                    await asyncio.to_thread(prefetch_order_params, window.up_token)
                    await asyncio.to_thread(prefetch_order_params, window.down_token)
    except Exception as exc:
        logger.write({"ts": dt.datetime.now().astimezone().isoformat(), "event": "error", "error": str(exc)})
        return 1
    finally:
        if dvol.refresh_task is not None:
            dvol.refresh_task.cancel()
            await asyncio.gather(dvol.refresh_task, return_exceptions=True)
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


def main() -> int:
    try:
        options = build_runtime_options(build_arg_parser().parse_args())
    except Exception as exc:
        print(json.dumps({"event": "error", "error": str(exc)}, separators=(",", ":")), file=sys.stderr)
        return 2
    return asyncio.run(run(options))


if __name__ == "__main__":
    raise SystemExit(main())
