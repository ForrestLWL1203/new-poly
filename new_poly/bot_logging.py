"""JSONL row construction helpers for the probability-edge bot."""

from __future__ import annotations

from typing import Any

from new_poly.bot_log_schema import _compact, _position_log
from new_poly.bot_loop import DvolRuntime, LoopRuntime
from new_poly.bot_runtime import (
    JsonlLogger,
    RuntimeOptions,
    _runtime_log_meta,
    _should_attach_reference_meta,
    _should_write_row,
)
from new_poly.strategy.prob_edge import StrategyDecision
from new_poly.strategy.state import StrategyState


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
        **_runtime_log_meta(meta),
        "mode": options.mode,
        "event": "tick",
        "sigma_source": dvol.state.current.source if dvol.state.current is not None else "missing",
        "sigma_eff": _compact(sigma_eff),
        "volatility_stale": dvol_stale,
        "position": _position_log(state.open_position, compact=True),
        "realized_pnl": _compact(state.realized_pnl, 4),
    }
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
    if event in {"order_no_fill", "exit", "partial_exit"}:
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
    if _should_write_row(row, loop.seen_repetitive_skips):
        logger.write(row)
