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
    return {
        **_runtime_log_meta(meta),
        "mode": options.mode,
        "event": "tick",
        "sigma_source": dvol.state.current.source if dvol.state.current is not None else "missing",
        "sigma_eff": _compact(sigma_eff),
        "volatility_stale": dvol_stale,
        "position": _position_log(state.open_position, compact=True),
        "realized_pnl": _compact(state.realized_pnl, 4),
    }


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
    if _should_attach_reference_meta(
        reference_meta,
        analysis_logs=options.analysis_logs,
        has_position=state.has_position,
        decision=decision,
    ):
        row["reference"] = reference_meta
    if _should_write_row(row, loop.seen_repetitive_skips):
        logger.write(row)
