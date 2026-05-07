"""Entry and exit execution flow helpers for the probability-edge bot."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from new_poly.bot_log_schema import _compact, _decision_log, _entry_analysis, _exit_analysis, _position_log
from new_poly.bot_runtime import (
    BotConfig,
    RuntimeOptions,
    WindowPrices,
    _refresh_entry_retry_params,
    _refresh_exit_retry_params,
)
from new_poly.strategy.prob_edge import evaluate_entry, evaluate_exit
from new_poly.strategy.state import PositionSnapshot, StrategyState


def _apply_closed_trade_risk(row: dict[str, Any], *, state: StrategyState, cfg: BotConfig, pnl: float) -> None:
    event = state.apply_closed_trade_risk(
        pnl,
        loss_limit=cfg.risk.consecutive_loss_limit,
        pause_windows=cfg.risk.loss_pause_windows,
    )
    if event is not None:
        row["risk_event"] = event


async def handle_open_position_tick(
    *,
    row: dict[str, Any],
    snap,
    window: Any,
    prices: WindowPrices,
    feeds,
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
        pnl, closed = state.record_partial_exit(result.avg_price, result.filled_size, decision.reason, snap.age_sec)
        row["event"] = "exit" if closed else "partial_exit"
        row["exit_reason"] = decision.reason
        row["exit_price"] = _compact(result.avg_price)
        row["exit_shares"] = _compact(result.filled_size)
        row["exit_pnl"] = _compact(pnl, 4)
        if closed:
            _apply_closed_trade_risk(row, state=state, cfg=cfg, pnl=pnl)
        if options.analysis_logs:
            row["position_before_exit"] = _position_log(exiting_position, compact=False)
            row["position_after_exit"] = _position_log(state.open_position, compact=False)
    elif (
        options.mode == "live"
        and cfg.risk.stop_on_live_no_sellable_balance
        and result.fatal_stop_reason is not None
    ):
        state.fatal_stop_reason = result.fatal_stop_reason
        row["event"] = "fatal_stop"
        row["fatal_stop_reason"] = result.fatal_stop_reason
        row["order_intent"] = "exit"
    else:
        row["event"] = "order_no_fill"
        row["order_intent"] = "exit"
        if options.analysis_logs:
            row["analysis"] = {"price_sources": price_analysis, **_exit_analysis(decision, result)}
    return decision


async def handle_flat_tick(
    *,
    row: dict[str, Any],
    snap,
    window: Any,
    prices: WindowPrices,
    feeds,
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
