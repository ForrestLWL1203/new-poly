"""JSONL row construction helpers for the probability-edge bot."""

from __future__ import annotations

from typing import Any

from new_poly.bot_log_schema import _compact, _position_log
from new_poly.bot_loop import DvolRuntime, LoopRuntime
from new_poly.bot_runtime import (
    JsonlLogger,
    RuntimeOptions,
    _should_attach_reference_meta,
    _should_write_row,
)
from new_poly.strategy.prob_edge import StrategyDecision
from new_poly.strategy.state import StrategyState


def _compact_token_state(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    fields = (
        "bid",
        "ask",
        "ask_avg",
        "ask_limit",
        "bid_avg",
        "bid_limit",
        "bid_depth_ok",
        "ask_age_ms",
        "bid_age_ms",
        "book_age_ms",
    )
    return {
        key: value.get(key)
        for key in fields
        if key in value and value.get(key) is not None
    }


def build_tick_row(
    meta: dict[str, Any],
    *,
    options: RuntimeOptions,
    dvol: DvolRuntime,
    state: StrategyState,
    sigma_eff: float | None,
    dvol_stale: bool,
) -> dict[str, Any]:
    row = {
        "ts": meta.get("ts"),
        "market_slug": meta.get("market_slug"),
        "age_sec": meta.get("age_sec"),
        "remaining_sec": meta.get("remaining_sec"),
        "price_source": meta.get("price_source"),
        "s_price": meta.get("s_price"),
        "k_price": meta.get("k_price"),
        "basis_bps": meta.get("basis_bps"),
        "mode": options.mode,
        "event": "tick",
        "sigma_source": dvol.state.current.source if dvol.state.current is not None else "missing",
        "sigma_eff": _compact(sigma_eff),
        "volatility_stale": dvol_stale,
        "up": _compact_token_state(meta.get("up")),
        "down": _compact_token_state(meta.get("down")),
    }
    for key in (
        "source_spread_bps",
        "polymarket_price",
        "polymarket_price_age_sec",
        "polymarket_reference_prob_up",
        "polymarket_reference_prob_down",
        "polymarket_divergence_bps",
    ):
        if meta.get(key) is not None:
            row[key] = meta.get(key)
    if options.analysis_logs:
        row["position"] = _position_log(state.open_position, compact=True)
        row["realized_pnl"] = _compact(state.realized_pnl, 4)
    if isinstance(meta.get("clob_ws"), dict):
        row["_clob_ws"] = meta["clob_ws"]
    return row


def _clob_diag_should_attach(
    *,
    diag: dict[str, Any],
    options: RuntimeOptions,
    state: StrategyState,
    row: dict[str, Any],
    decision: StrategyDecision | None,
) -> bool:
    if not options.analysis_logs:
        return False
    event = str(row.get("event") or "tick")
    if event in {"order_no_fill", "exit", "position_reduce"}:
        return True
    if decision is not None and decision.reason in {"stale_book", "stale_book_wait", "risk_exit"}:
        return True
    depth_age_ms = diag.get("last_depth_update_age_ms")
    try:
        return depth_age_ms is not None and float(depth_age_ms) > 1000.0
    except (TypeError, ValueError):
        return False


def write_tick_row(
    *,
    logger: JsonlLogger,
    loop: LoopRuntime,
    options: RuntimeOptions,
    state: StrategyState,
    row: dict[str, Any],
    reference_meta: dict[str, Any],
    decision: StrategyDecision | None,
) -> None:
    clob_diag = row.pop("_clob_ws", None)
    if isinstance(clob_diag, dict) and _clob_diag_should_attach(
        diag=clob_diag,
        options=options,
        state=state,
        row=row,
        decision=decision,
    ):
        row["clob_ws"] = clob_diag
    if _should_attach_reference_meta(
        reference_meta,
        analysis_logs=options.analysis_logs,
        has_position=state.has_position,
        decision=decision,
    ):
        row["reference"] = reference_meta
    if _should_write_row(row, loop.seen_repetitive_skips, analysis_logs=options.analysis_logs):
        logger.write(row)
