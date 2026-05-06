"""Loop helpers for the BTC 5m probability-edge bot."""

from __future__ import annotations

import asyncio
import datetime as dt
import time
from dataclasses import dataclass, replace
from typing import Any

from new_poly.bot_runtime import (
    BotConfig,
    DvolRefreshState,
    JsonlLogger,
    RuntimeOptions,
    WindowPrices,
    _backtest_base_config,
    _bot_config_with_edge,
    _compact,
    _decision_log,
    _dynamic_candidate_payload,
    _dynamic_health_payload,
    _entry_analysis,
    _exit_analysis,
    _polymarket_reference_recovered_row,
    _polymarket_reference_unhealthy_row,
    _position_log,
    _refresh_entry_retry_params,
    _refresh_exit_retry_params,
    _run_dynamic_analysis_task,
    choose_settlement,
    effective_price,
    fetch_valid_dvol_with_retries,
    find_following_window,
    is_dvol_stale,
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
from new_poly.strategy.dynamic_params import DynamicConfig, DynamicDecision, DynamicState
from new_poly.strategy.dynamic_params import save_dynamic_state
from new_poly.strategy.prob_edge import evaluate_entry, evaluate_exit
from new_poly.strategy.state import PositionSnapshot, StrategyState
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
class LoopRuntime:
    completed_windows: int = 0
    seen_repetitive_skips: set[tuple[str, str]] | None = None
    polymarket_reference_warning_logged: bool = False
    polymarket_unhealthy_since: float | None = None

    def __post_init__(self) -> None:
        if self.seen_repetitive_skips is None:
            self.seen_repetitive_skips = set()


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
        if dvol.state.apply_refresh_result(refreshed):
            logger.write({
                "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                "event": "dvol_recovered",
                "mode": options.mode,
                "market_slug": dvol.refresh_market_slug or window_slug,
                "volatility": dvol.state.current.to_json() if dvol.state.current is not None else None,
            })
        else:
            logger.write({
                "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                "event": "dvol_refresh_failed",
                "mode": options.mode,
                "market_slug": dvol.refresh_market_slug or window_slug,
                "failed_refreshes": dvol.state.failed_refreshes,
                "last_error": dvol.state.last_error,
                "kept_previous": dvol.state.current.to_json() if dvol.state.current is not None else None,
            })
        dvol.refresh_task = None
        dvol.refresh_market_slug = None
        dvol.next_refresh = time.monotonic() + cfg.dvol_refresh_sec
    if dvol.refresh_task is None and time.monotonic() >= dvol.next_refresh:
        refresh_market_slug = window_slug
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
    return sigma_eff, dvol_stale


async def _handle_open_position_tick(
    *,
    row: dict[str, Any],
    snap,
    window: Any,
    prices: WindowPrices,
    feeds: FeedContext,
    cfg: BotConfig,
    options: RuntimeOptions,
    gateway,
    state: StrategyState,
    sigma_eff: float | None,
    price_analysis: dict[str, Any],
) -> Any:
    assert state.open_position is not None
    decision = evaluate_exit(snap, state.open_position, cfg.edge, state)
    row["decision"] = _decision_log(decision)
    if decision.model_prob is not None:
        state.record_model_prob(
            snap.age_sec,
            decision.model_prob,
            retention_sec=max(cfg.edge.prob_stagnation_window_sec, cfg.edge.prob_drop_exit_window_sec, 5.0),
        )
    if decision.action != "exit":
        return decision

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
    return decision


async def _handle_flat_tick(
    *,
    row: dict[str, Any],
    snap,
    window: Any,
    prices: WindowPrices,
    feeds: FeedContext,
    cfg: BotConfig,
    options: RuntimeOptions,
    gateway,
    state: StrategyState,
    sigma_eff: float | None,
    price_analysis: dict[str, Any],
) -> Any:
    decision = evaluate_entry(snap, state, cfg.edge)
    row["decision"] = _decision_log(decision)
    if decision.action != "enter":
        return decision

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
    return decision


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
    dynamic_cfg: DynamicConfig | None,
    dynamic_state: DynamicState | None,
    dynamic_task: asyncio.Task[tuple[DynamicDecision, DynamicState]] | None,
) -> tuple[Any, WindowPrices, BotConfig, DynamicState | None, asyncio.Task[tuple[DynamicDecision, DynamicState]] | None, bool]:
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
        return window, prices, cfg, dynamic_state, dynamic_task, True

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

    window = next_window
    prices = WindowPrices()
    state.reset_for_market(window.slug)
    loop.seen_repetitive_skips.clear()
    await asyncio.wait_for(feeds.stream.switch_tokens([window.up_token, window.down_token]), timeout=8.0)
    if options.mode == "live":
        await asyncio.to_thread(prefetch_order_params, window.up_token)
        await asyncio.to_thread(prefetch_order_params, window.down_token)
    return window, prices, cfg, dynamic_state, dynamic_task, False
