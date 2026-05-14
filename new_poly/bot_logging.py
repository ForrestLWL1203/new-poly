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

# Minimal high-frequency paper fields consumed by backtest replay.
# Keep this in sync with new_poly.backtest.prob_edge_replay.snapshot_from_row.
_POLY_SOURCE_BACKTEST_FIELDS = {
    "event",
    "ts",
    "market_slug",
    "age_sec",
    "remaining_sec",
    "k_price",
    "s_price",
    "polymarket_price",
    "lead_polymarket_return_1s_bps",
    "lead_polymarket_return_3s_bps",
    "lead_polymarket_return_5s_bps",
    "lead_polymarket_return_10s_bps",
    "lead_polymarket_return_15s_bps",
    "warnings",
    "clob_ws",
    "observation_reason",
}

# Post-exit observations are compact ticks with a few exit-context fields.
_POST_EXIT_OBSERVATION_FIELDS = {
    "decision",
    "last_exit_reason",
    "last_exit_side",
    "last_exit_age_sec",
    "observation_interval_sec",
}

# Minimal per-side book fields consumed by backtest replay.
_BACKTEST_TOKEN_FIELDS = {
    "ask",
    "bid_avg",
    "bid_limit",
    "bid_depth_ok",
    "book_age_ms",
}


def build_tick_row(
    meta: dict[str, Any],
    *,
    options: RuntimeOptions,
    dvol: DvolRuntime | None,
    state: StrategyState,
    sigma_eff: float | None,
    dvol_stale: bool,
) -> dict[str, Any]:
    row = {
        **_runtime_log_meta(meta, strategy_mode=options.config.strategy_mode),
        "mode": options.mode,
        "event": "tick",
        "sigma_source": dvol.state.current.source if dvol is not None and dvol.state.current is not None else "not_used",
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
        logger.write(compact_high_frequency_row(row, options=options))


def compact_high_frequency_row(row: dict[str, Any], *, options: RuntimeOptions) -> dict[str, Any]:
    if options.config.strategy_mode != "poly_single_source":
        return row
    if options.mode == "live":
        return row
    if row.get("event") not in {"tick", "post_exit_observation"}:
        return row

    compacted = {
        key: value
        for key in _POLY_SOURCE_BACKTEST_FIELDS
        if key in row and (value := row.get(key)) is not None and value != "missing"
    }
    if row.get("event") == "post_exit_observation":
        compacted.update({
            key: value
            for key in _POST_EXIT_OBSERVATION_FIELDS
            if key in row and (value := row.get(key)) is not None
        })

    _merge_price_context(compacted, row.get("reference"))
    _merge_price_context(compacted, row.get("analysis"))
    for side in ("up", "down"):
        token_row = _compact_token_row(row.get(side))
        if token_row:
            compacted[side] = token_row
    return compacted


def _merge_price_context(target: dict[str, Any], source: Any) -> None:
    if not isinstance(source, dict):
        return
    for key in _POLY_SOURCE_BACKTEST_FIELDS:
        if key in target:
            continue
        value = source.get(key)
        if value is not None and value != "missing":
            target[key] = value
    price_source = source.get("price_sources")
    if isinstance(price_source, dict):
        _merge_price_context(target, price_source)


def _compact_token_row(source: Any) -> dict[str, Any]:
    if not isinstance(source, dict):
        return {}
    return {
        key: value
        for key in _BACKTEST_TOKEN_FIELDS
        if key in source and (value := source.get(key)) is not None
    }
