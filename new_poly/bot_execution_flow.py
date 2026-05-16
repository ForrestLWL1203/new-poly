"""Entry and exit execution flow helpers for the probability-edge bot."""

from __future__ import annotations

import asyncio
import datetime as dt
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
from new_poly.strategy.prob_edge import StrategyDecision
from new_poly.strategy.poly_source import entry_amount_usd, evaluate_poly_entry, evaluate_poly_exit
from new_poly.strategy.state import PositionSnapshot, StrategyState, UnknownEntryOrder
from new_poly.trading.clob_client import get_token_balance


UNKNOWN_ENTRY_SAFETY_REMAINING_SEC = 90.0
UNKNOWN_ENTRY_SAFETY_MIN_AGE_SEC = 30.0

_ORDER_INTENT_OMIT_IF_NONE = {
    "model_prob",
    "phase",
    "required_edge",
    "best_ask",
    "depth_limit_price",
    "edge",
}


def _order_intent_row(
    *,
    row: dict[str, Any],
    intent: str,
    token_id: str,
    decision,
    price_analysis: dict[str, Any],
    options: RuntimeOptions,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event = "order_intent" if intent == "entry" else "exit_intent"
    intent_field = "order_intent" if intent == "entry" else "exit_intent"
    out = {
        "ts": row.get("ts"),
        "mode": options.mode,
        "event": event,
        "market_slug": row.get("market_slug"),
        "age_sec": row.get("age_sec"),
        "remaining_sec": row.get("remaining_sec"),
        intent_field: intent,
        "token_id": token_id,
        "side": decision.side,
        f"{intent}_side": decision.side,
        "reason": decision.reason,
        "model_prob": _compact(decision.model_prob),
        "signal_price": _compact(decision.price),
        "limit_price": _compact(decision.limit_price),
        "best_ask": _compact(decision.best_ask),
        "depth_limit_price": _compact(decision.depth_limit_price),
        "edge": _compact(decision.edge),
        "phase": decision.phase,
        "required_edge": _compact(decision.required_edge),
    }
    if extra:
        out.update(extra)
    if options.analysis_logs:
        strategy_analysis = _entry_analysis(decision, None) if intent == "entry" else _exit_analysis(decision, None)
        out["analysis"] = {"price_sources": price_analysis, **strategy_analysis}
    return {
        key: value
        for key, value in out.items()
        if value is not None or key not in _ORDER_INTENT_OMIT_IF_NONE
    }


def _score_component_log_mode(cfg: BotConfig) -> str:
    return cfg.poly_source.poly_score_component_logs


def _apply_closed_trade_risk(row: dict[str, Any], *, state: StrategyState, cfg: BotConfig, pnl: float) -> None:
    event = state.apply_closed_trade_risk(
        pnl,
        loss_limit=cfg.risk.consecutive_loss_limit,
        pause_windows=cfg.risk.loss_pause_windows,
    )
    if event is not None:
        row["risk_event"] = event


def _unknown_buy_needs_safety_check(*, state: StrategyState, snap, window: Any, cfg: BotConfig, options: RuntimeOptions) -> bool:
    pending = state.unresolved_unknown_entry
    if options.mode != "live" or pending is None:
        return False
    if pending.safety_checked or pending.market_slug != window.slug:
        return False
    if snap.age_sec - pending.entry_time < UNKNOWN_ENTRY_SAFETY_MIN_AGE_SEC:
        return False
    entry_end_age_sec = cfg.poly_source.entry_end_age_sec
    return snap.remaining_sec <= UNKNOWN_ENTRY_SAFETY_REMAINING_SEC or snap.age_sec >= entry_end_age_sec


def _is_unconfirmed_unknown_buy(result) -> bool:
    timing = result.timing if isinstance(result.timing, dict) else {}
    return timing.get("reconciliation") == "unknown_buy_no_balance_after_delayed_checks"


def _record_unknown_entry_candidate(
    *,
    state: StrategyState,
    decision,
    token_id: str,
    window: Any,
    snap,
    cfg: BotConfig,
    result,
) -> None:
    if decision.side is None or decision.edge is None:
        return
    timing = result.timing if isinstance(result.timing, dict) else {}
    created_at = timing.get("sent_at_epoch_ms")
    amount_usd = entry_amount_usd(
        cfg.amount_usd,
        score=decision.poly_entry_score,
        entry_price=decision.best_ask or decision.price or decision.limit_price,
        reference_distance_bps=decision.poly_reference_distance_bps,
        direction_quality=decision.direction_quality,
        direction_cross_count_recent=decision.direction_cross_count_recent,
        direction_confidence=decision.direction_confidence,
        cfg=cfg.poly_source,
        phase=decision.phase,
        age_sec=snap.age_sec,
    )
    state.record_unresolved_unknown_entry(UnknownEntryOrder(
        market_slug=window.slug,
        token_side=decision.side,
        token_id=token_id,
        amount_usd=amount_usd,
        entry_time=snap.age_sec,
        entry_avg_price=decision.best_ask or decision.price or decision.limit_price or 0.0,
        entry_model_prob=decision.model_prob if decision.model_prob is not None else 0.0,
        entry_edge=decision.edge,
        entry_polymarket_divergence_bps=decision.polymarket_divergence_bps,
        entry_favorable_gap_bps=decision.favorable_gap_bps,
        entry_reference_distance_bps=decision.entry_reference_distance_bps or decision.poly_reference_distance_bps,
        created_at_epoch_ms=int(created_at) if created_at is not None else None,
        signal_price=decision.price,
        limit_price=decision.limit_price,
        best_ask=decision.best_ask,
        depth_limit_price=decision.depth_limit_price,
    ))


def _filled_notional(result, fallback_amount_usd: float) -> float:
    if result is not None and result.success and result.avg_price > 0 and result.filled_size > 0:
        return result.avg_price * result.filled_size
    return fallback_amount_usd


async def _query_unknown_entry_safety_balance(
    *,
    state: StrategyState,
    snap,
    window: Any,
    cfg: BotConfig,
    options: RuntimeOptions,
    logger,
) -> tuple[UnknownEntryOrder, float | None, dict[str, Any]] | None:
    if not _unknown_buy_needs_safety_check(state=state, snap=snap, window=window, cfg=cfg, options=options):
        return None
    pending = state.unresolved_unknown_entry
    assert pending is not None
    balance = await asyncio.to_thread(get_token_balance, pending.token_id, safe=True)
    if balance is not None:
        pending.safety_checked = True
    check_row = {
        "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        "mode": options.mode,
        "event": "unknown_entry_balance_safety_check",
        "market_slug": window.slug,
        "age_sec": snap.age_sec,
        "remaining_sec": snap.remaining_sec,
        "token_id": pending.token_id,
        "entry_side": pending.token_side,
        "balance": _compact(balance),
    }
    logger.write(check_row)
    return pending, balance, check_row


async def _finish_pending_entry_order(
    *,
    token_id: str,
    decision,
    snap,
    window: Any,
    cfg: BotConfig,
    options: RuntimeOptions,
    state: StrategyState,
    logger,
    order_coro,
    price_analysis: dict[str, Any],
    amount_usd: float,
) -> None:
    try:
        result = await order_coro
        row: dict[str, Any] = {
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "mode": options.mode,
            "market_slug": window.slug,
            "age_sec": snap.age_sec,
            "remaining_sec": snap.remaining_sec,
            "order": result.__dict__,
        }
        if state.current_market_slug != window.slug:
            row["order_intent"] = "entry"
            row["token_id"] = token_id
            row["entry_side"] = decision.side
            row["amount_usd"] = _compact(_filled_notional(result, amount_usd))
            if result.success:
                row["event"] = "orphan_entry_after_window_switch"
                row["action"] = "manual_or_orphan_balance_review_required"
                row["entry_price"] = _compact(result.avg_price)
                row["entry_shares"] = _compact(result.filled_size)
            elif options.mode == "live" and _is_unconfirmed_unknown_buy(result):
                row["event"] = "orphan_unknown_entry_after_window_switch"
                row["action"] = "manual_or_orphan_balance_review_required"
            else:
                row["event"] = "order_reconcile_stale"
                row["action"] = "ignore_result_after_window_switch"
            logger.write(row)
            return
        if result.success and decision.side is not None and decision.edge is not None:
            filled_notional = _filled_notional(result, amount_usd)
            state.record_entry(PositionSnapshot(
                market_slug=window.slug,
                token_side=decision.side,
                token_id=token_id,
                entry_time=snap.age_sec,
                entry_avg_price=result.avg_price,
                filled_shares=result.filled_size,
                entry_model_prob=decision.model_prob if decision.model_prob is not None else 0.0,
                entry_edge=decision.edge,
                entry_amount_usd=filled_notional,
                entry_polymarket_divergence_bps=decision.polymarket_divergence_bps,
                entry_favorable_gap_bps=decision.favorable_gap_bps,
                entry_reference_distance_bps=decision.entry_reference_distance_bps or decision.poly_reference_distance_bps,
            ))
            row["event"] = "entry"
            row["entry_side"] = decision.side
            row["entry_price"] = _compact(result.avg_price)
            row["entry_shares"] = _compact(result.filled_size)
            row["amount_usd"] = _compact(filled_notional)
            row["position_after_entry"] = _position_log(state.open_position, compact=not options.analysis_logs)
        elif (
            options.mode == "live"
            and cfg.risk.stop_on_live_insufficient_cash_balance
            and result.fatal_stop_reason is not None
        ):
            state.fatal_stop_reason = result.fatal_stop_reason
            row["event"] = "fatal_stop"
            row["fatal_stop_reason"] = result.fatal_stop_reason
            row["order_intent"] = "entry"
        else:
            row["event"] = "order_no_fill"
            row["order_intent"] = "entry"
            if options.mode == "live" and _is_unconfirmed_unknown_buy(result):
                logger.write({
                    "ts": row["ts"],
                    "mode": options.mode,
                    "event": "order_unknown_reconcile_pending",
                    "market_slug": window.slug,
                    "age_sec": snap.age_sec,
                    "remaining_sec": snap.remaining_sec,
                    "order_intent": "entry",
                    "token_id": token_id,
                    "entry_side": decision.side,
                    "order": result.__dict__,
                })
                _record_unknown_entry_candidate(
                    state=state,
                    decision=decision,
                    token_id=token_id,
                    window=window,
                    snap=snap,
                    cfg=cfg,
                    result=result,
                )
        if options.analysis_logs:
            row["analysis"] = {"price_sources": price_analysis, **_entry_analysis(decision, result)}
        logger.write(row)
    except Exception as exc:
        logger.write({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "mode": options.mode,
            "event": "order_reconcile_error",
            "market_slug": window.slug,
            "order_intent": "entry",
            "error_type": type(exc).__name__,
            "message": str(exc),
        })
    finally:
        if state.pending_execution_market_slug == window.slug:
            state.clear_pending_execution()


async def _finish_pending_exit_order(
    *,
    decision,
    snap,
    window: Any,
    cfg: BotConfig,
    options: RuntimeOptions,
    state: StrategyState,
    logger,
    order_coro,
    price_analysis: dict[str, Any],
    exiting_position: PositionSnapshot,
) -> None:
    try:
        result = await order_coro
        row: dict[str, Any] = {
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "mode": options.mode,
            "market_slug": window.slug,
            "age_sec": snap.age_sec,
            "remaining_sec": snap.remaining_sec,
            "order": result.__dict__,
        }
        if state.current_market_slug != window.slug:
            row["event"] = "order_reconcile_stale"
            row["exit_intent"] = "exit"
            row["action"] = "ignore_result_after_window_switch"
            logger.write(row)
            return
        if result.success:
            pnl, closed = state.record_partial_exit(result.avg_price, result.filled_size, decision.reason, snap.age_sec)
            row["event"] = "exit" if closed else "position_reduce"
            row["exit_reason"] = decision.reason
            row["exit_price"] = _compact(result.avg_price)
            row["exit_shares"] = _compact(result.filled_size)
            row["exit_pnl"] = _compact(pnl, 4)
            if not closed:
                row["exit_status"] = "residual_open"
                row["remaining_shares"] = _compact(
                    state.open_position.filled_shares if state.open_position is not None else 0.0
                )
            if closed:
                _apply_closed_trade_risk(row, state=state, cfg=cfg, pnl=pnl)
            row["position_before_exit"] = _position_log(exiting_position, compact=not options.analysis_logs)
            row["position_after_exit"] = _position_log(state.open_position, compact=not options.analysis_logs)
        elif result.message.startswith("live dust sell skipped"):
            pnl = state.record_exit(0.0, "dust_position", snap.age_sec)
            row["event"] = "dust_position"
            row["exit_reason"] = "dust_position"
            row["exit_price"] = 0.0
            row["exit_shares"] = _compact(exiting_position.filled_shares)
            row["exit_pnl"] = _compact(pnl, 4)
            row["exit_intent"] = "exit"
            row["position_before_exit"] = _position_log(exiting_position, compact=not options.analysis_logs)
            row["position_after_exit"] = _position_log(state.open_position, compact=not options.analysis_logs)
        else:
            row["event"] = "order_no_fill"
            row["exit_intent"] = "exit"
        if options.analysis_logs:
            row["analysis"] = {"price_sources": price_analysis, **_exit_analysis(decision, result)}
        logger.write(row)
    except Exception as exc:
        logger.write({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "mode": options.mode,
            "event": "order_reconcile_error",
            "market_slug": window.slug,
            "exit_intent": "exit",
            "error_type": type(exc).__name__,
            "message": str(exc),
        })
    finally:
        if state.pending_execution_market_slug == window.slug:
            state.clear_pending_execution()


async def _maybe_recover_unknown_entry_balance(
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
    logger,
) -> Any | None:
    safety = await _query_unknown_entry_safety_balance(
        state=state,
        snap=snap,
        window=window,
        cfg=cfg,
        options=options,
        logger=logger,
    )
    if safety is None:
        return None
    pending, balance, check_row = safety
    if balance is None:
        logger.write({
            **check_row,
            "event": "unknown_entry_balance_safety_unavailable",
        })
        return None
    if balance <= 1e-9:
        state.clear_unresolved_unknown_entry()
        logger.write({
            **check_row,
            "event": "unknown_entry_balance_safety_no_balance",
        })
        return None
    if state.has_position:
        decision = StrategyDecision(action="skip", reason="unknown_balance_recovery_skipped_existing_position")
        row["decision"] = _decision_log(decision, component_logs=_score_component_log_mode(cfg))
        logger.write({
            **check_row,
            "event": "unknown_balance_recovery_skipped_existing_position",
            "orphan_balance_review_required": True,
            "open_position": _position_log(state.open_position, compact=True),
        })
        return decision
    price = pending.entry_avg_price if pending.entry_avg_price > 0 else pending.amount_usd / balance
    amount_usd = price * balance
    state.record_entry(PositionSnapshot(
        market_slug=window.slug,
        token_side=pending.token_side,
        token_id=pending.token_id,
        entry_time=snap.age_sec,
        entry_avg_price=price,
        filled_shares=balance,
        entry_model_prob=pending.entry_model_prob,
        entry_edge=pending.entry_edge,
        entry_amount_usd=amount_usd,
        entry_polymarket_divergence_bps=pending.entry_polymarket_divergence_bps,
        entry_favorable_gap_bps=pending.entry_favorable_gap_bps,
        entry_reference_distance_bps=pending.entry_reference_distance_bps,
    ))
    state.clear_unresolved_unknown_entry()
    logger.write({
        **check_row,
        "event": "entry_recovered_from_unknown_balance",
        "entry_side": pending.token_side,
        "entry_price": _compact(price),
        "entry_shares": _compact(balance),
        "amount_usd": _compact(amount_usd),
        "position_after_entry": _position_log(state.open_position, compact=not options.analysis_logs),
    })
    return await handle_open_position_tick(
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
        logger=logger,
    )


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
    logger=None,
) -> Any:
    assert state.open_position is not None
    if logger is not None:
        safety = await _query_unknown_entry_safety_balance(
            state=state,
            snap=snap,
            window=window,
            cfg=cfg,
            options=options,
            logger=logger,
        )
        if safety is not None:
            _pending, balance, check_row = safety
            if balance is None:
                logger.write({
                    **check_row,
                    "event": "unknown_entry_balance_safety_unavailable",
                })
            elif balance <= 1e-9:
                state.clear_unresolved_unknown_entry()
                logger.write({
                    **check_row,
                    "event": "unknown_entry_balance_safety_no_balance",
                })
            else:
                logger.write({
                    **check_row,
                    "event": "unknown_balance_recovery_skipped_existing_position",
                    "orphan_balance_review_required": True,
                    "open_position": _position_log(state.open_position, compact=True),
                })
    if state.has_pending_execution:
        decision = StrategyDecision(action="hold", reason="pending_order_reconciliation", side=state.open_position.token_side)
        row["decision"] = _decision_log(decision, component_logs=_score_component_log_mode(cfg))
        return decision
    decision = evaluate_poly_exit(snap, state.open_position, cfg.poly_source, state)
    row["decision"] = _decision_log(decision, component_logs=_score_component_log_mode(cfg))
    if decision.action != "exit":
        return decision

    exiting_position = replace(state.open_position)
    if logger is not None:
        logger.write(_order_intent_row(
            row=row,
            intent="exit",
            token_id=state.open_position.token_id,
            decision=decision,
            price_analysis=price_analysis,
            options=options,
            extra={
                "shares": _compact(state.open_position.filled_shares),
                "exit_reason": decision.reason,
            },
        ))
    order_coro = gateway.sell(
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
    if options.mode == "live" and logger is not None:
        task = asyncio.create_task(_finish_pending_exit_order(
            decision=decision,
            snap=snap,
            window=window,
            cfg=cfg,
            options=options,
            state=state,
            logger=logger,
            order_coro=order_coro,
            price_analysis=price_analysis,
            exiting_position=exiting_position,
        ))
        state.mark_pending_execution("exit", task)
        row["event"] = "order_reconcile_pending"
        row["exit_intent"] = "exit"
        return decision

    result = await order_coro
    row["order"] = result.__dict__
    if options.analysis_logs:
        row["analysis"] = {"price_sources": price_analysis, **_exit_analysis(decision, result)}
    if result.success:
        pnl, closed = state.record_partial_exit(result.avg_price, result.filled_size, decision.reason, snap.age_sec)
        row["event"] = "exit" if closed else "position_reduce"
        row["exit_reason"] = decision.reason
        row["exit_price"] = _compact(result.avg_price)
        row["exit_shares"] = _compact(result.filled_size)
        row["exit_pnl"] = _compact(pnl, 4)
        if not closed:
            row["exit_status"] = "residual_open"
            row["remaining_shares"] = _compact(
                state.open_position.filled_shares if state.open_position is not None else 0.0
            )
        if closed:
            _apply_closed_trade_risk(row, state=state, cfg=cfg, pnl=pnl)
        row["position_before_exit"] = _position_log(exiting_position, compact=not options.analysis_logs)
        row["position_after_exit"] = _position_log(state.open_position, compact=not options.analysis_logs)
    elif result.message.startswith("live dust sell skipped"):
        pnl = state.record_exit(0.0, "dust_position", snap.age_sec)
        row["event"] = "dust_position"
        row["exit_reason"] = "dust_position"
        row["exit_price"] = 0.0
        row["exit_shares"] = _compact(exiting_position.filled_shares)
        row["exit_pnl"] = _compact(pnl, 4)
        row["exit_intent"] = "exit"
        row["position_before_exit"] = _position_log(exiting_position, compact=not options.analysis_logs)
        row["position_after_exit"] = _position_log(state.open_position, compact=not options.analysis_logs)
    else:
        row["event"] = "order_no_fill"
        row["exit_intent"] = "exit"
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
    logger=None,
) -> Any:
    if logger is not None:
        recovered_decision = await _maybe_recover_unknown_entry_balance(
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
            logger=logger,
        )
        if recovered_decision is not None:
            return recovered_decision
    if state.has_pending_execution:
        decision = StrategyDecision(action="skip", reason="pending_order_reconciliation")
        row["decision"] = _decision_log(decision, component_logs=_score_component_log_mode(cfg))
        return decision
    if state.unresolved_unknown_entry is not None:
        decision = StrategyDecision(action="skip", reason="unresolved_unknown_entry_pending")
        row["decision"] = _decision_log(decision, component_logs=_score_component_log_mode(cfg))
        return decision
    state.record_reference_baseline(snap)
    decision = evaluate_poly_entry(snap, state, cfg.poly_source)
    row["decision"] = _decision_log(decision, component_logs=_score_component_log_mode(cfg))
    if (
        decision.reason == "outside_entry_time"
        and cfg.poly_source.pre_entry_observation_start_age_sec > 0
        and snap.age_sec >= cfg.poly_source.pre_entry_observation_start_age_sec
        and snap.age_sec < cfg.poly_source.entry_start_age_sec
    ):
        row["force_write_tick"] = True
        row["observation_reason"] = "pre_entry_observation"
    if decision.action != "enter":
        return decision

    token_id = window.up_token if decision.side == "up" else window.down_token
    amount_usd = entry_amount_usd(
        cfg.amount_usd,
        score=decision.poly_entry_score,
        entry_price=decision.best_ask or decision.price or decision.limit_price,
        reference_distance_bps=decision.poly_reference_distance_bps,
        direction_quality=decision.direction_quality,
        direction_cross_count_recent=decision.direction_cross_count_recent,
        direction_confidence=decision.direction_confidence,
        cfg=cfg.poly_source,
        phase=decision.phase,
        age_sec=snap.age_sec,
    )
    if logger is not None:
        logger.write(_order_intent_row(
            row=row,
            intent="entry",
            token_id=token_id,
            decision=decision,
            price_analysis=price_analysis,
            options=options,
            extra={"amount_usd": _compact(amount_usd)},
        ))
    order_coro = gateway.buy(
        token_id,
        amount_usd,
        max_price=decision.limit_price,
        best_ask=decision.best_ask,
        price_hint_base=decision.depth_limit_price,
        retry_refresh=lambda attempt, token_id=token_id, max_price=decision.limit_price: _refresh_entry_retry_params(
            stream=feeds.stream,
            token_id=token_id,
            max_price=max_price,
            cfg=cfg,
        ),
    )
    if options.mode == "live" and logger is not None:
        task = asyncio.create_task(_finish_pending_entry_order(
            token_id=token_id,
            decision=decision,
            snap=snap,
            window=window,
            cfg=cfg,
            options=options,
            state=state,
            logger=logger,
            order_coro=order_coro,
            price_analysis=price_analysis,
            amount_usd=amount_usd,
        ))
        state.mark_pending_execution("entry", task)
        row["event"] = "order_reconcile_pending"
        row["order_intent"] = "entry"
        return decision

    result = await order_coro
    row["order"] = result.__dict__
    if options.analysis_logs:
        row["analysis"] = {"price_sources": price_analysis, **_entry_analysis(decision, result)}
    if result.success and decision.side is not None and decision.edge is not None:
        filled_notional = _filled_notional(result, amount_usd)
        state.record_entry(PositionSnapshot(
            market_slug=window.slug,
            token_side=decision.side,
            token_id=token_id,
            entry_time=snap.age_sec,
            entry_avg_price=result.avg_price,
            filled_shares=result.filled_size,
            entry_model_prob=decision.model_prob if decision.model_prob is not None else 0.0,
            entry_edge=decision.edge,
            entry_amount_usd=filled_notional,
            entry_polymarket_divergence_bps=decision.polymarket_divergence_bps,
            entry_favorable_gap_bps=decision.favorable_gap_bps,
            entry_reference_distance_bps=decision.entry_reference_distance_bps or decision.poly_reference_distance_bps,
        ))
        row["event"] = "entry"
        row["entry_side"] = decision.side
        row["entry_price"] = _compact(result.avg_price)
        row["entry_shares"] = _compact(result.filled_size)
        row["amount_usd"] = _compact(filled_notional)
        row["position_after_entry"] = _position_log(state.open_position, compact=not options.analysis_logs)
    elif (
        options.mode == "live"
        and cfg.risk.stop_on_live_insufficient_cash_balance
        and result.fatal_stop_reason is not None
    ):
        state.fatal_stop_reason = result.fatal_stop_reason
        row["event"] = "fatal_stop"
        row["fatal_stop_reason"] = result.fatal_stop_reason
        row["order_intent"] = "entry"
    else:
        row["event"] = "order_no_fill"
        row["order_intent"] = "entry"
        if options.analysis_logs:
            row["analysis"] = {"price_sources": price_analysis, **_entry_analysis(decision, result)}
    return decision
