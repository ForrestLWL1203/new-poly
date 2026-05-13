from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

MODE_RE = re.compile(r"^(live|paper)-")
BEIJING = dt.timezone(dt.timedelta(hours=8))


def build_dashboard_status(
    mode: str,
    log_dir: Path,
    *,
    running_pids: list[int] | None = None,
    log_path: Path | None = None,
    stopped: bool = False,
    now: dt.datetime | None = None,
    select_latest: bool = True,
) -> dict[str, Any]:
    mode = _validate_mode(mode)
    now_utc = _ensure_utc(now or dt.datetime.now(dt.timezone.utc))
    log_dir = Path(log_dir)
    log_path = Path(log_path) if log_path is not None else (_select_log(mode, log_dir) if select_latest else None)
    if log_path is not None and not log_path.exists():
        log_path = _select_log(mode, log_dir) if select_latest else None
    running_pids = running_pids or []

    if log_path is None:
        return {
            "mode": mode,
            "run_status": "idle" if not running_pids else "unknown",
            "pid": running_pids[0] if running_pids else None,
            "pids": running_pids,
            "log_path": None,
            "log_name": None,
            "error": None if not running_pids else f"no {mode} jsonl logs found in {log_dir}",
            "configured_windows": None,
            "completed_windows": 0,
            "pending_windows": None,
            "run_remaining_sec": None,
            "current_window": None,
            "latest_tick": None,
            "entries": [],
            "exits": [],
            "trades": [],
            "window_records": [],
            "cashflows": [],
            "realized_pnl": 0.0,
            "open_position": None,
            "warnings": [],
            "errors": [],
            "fatal_stop_reason": None,
            "last_event_ts": None,
            "parse_warnings": [],
        }

    rows, parse_warnings = _read_rows(log_path)
    summary = _summarize_rows(mode, rows, now_utc=now_utc)
    run_status = "running" if running_pids else ("stopped" if stopped else _infer_finished(summary, now_utc))
    if run_status == "finished" and summary.get("configured_windows") is not None:
        summary["completed_windows"] = max(int(summary.get("completed_windows") or 0), int(summary["configured_windows"]))
        summary["pending_windows"] = 0
        summary["run_remaining_sec"] = 0
    if run_status == "finished" and isinstance(summary.get("current_window"), dict):
        summary["current_window"]["remaining_sec"] = 0
        summary["current_window"]["window_end_remaining_sec"] = 0
        summary["current_window"]["pre_start_sec"] = 0
    summary.update({
        "mode": mode,
        "run_status": run_status,
        "pid": running_pids[0] if running_pids else None,
        "pids": running_pids,
        "log_path": str(log_path),
        "log_name": log_path.name,
        "log_mtime": _iso_from_timestamp(log_path.stat().st_mtime),
        "parse_warnings": parse_warnings,
    })
    return summary


def _select_log(mode: str, log_dir: Path) -> Path | None:
    if not log_dir.exists():
        return None
    candidates = [path for path in log_dir.glob(f"{mode}-*.jsonl") if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def _read_rows(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError as exc:
                warnings.append({"line": line_no, "error": str(exc)})
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows, warnings


def _summarize_rows(mode: str, rows: list[dict[str, Any]], *, now_utc: dt.datetime) -> dict[str, Any]:
    configured_windows = None
    completed_windows = 0
    entry_start_age_sec = 120.0
    entry_end_age_sec = 220.0
    current_window: dict[str, Any] = {}
    latest_tick: dict[str, Any] | None = None
    entries: list[dict[str, Any]] = []
    exits: list[dict[str, Any]] = []
    cashflows: list[dict[str, Any]] = []
    warnings: list[str] = []
    errors: list[dict[str, Any]] = []
    window_records: list[dict[str, Any]] = []
    window_index_by_slug: dict[str, int] = {}
    pnl_events: list[float] = []
    latest_realized_pnl: float | None = None
    open_position = None
    realized_pnl = 0.0
    fatal_stop_reason = None
    last_ts = None

    for row in rows:
        event = row.get("event")
        last_ts = row.get("ts") or last_ts
        if row.get("realized_pnl") is not None:
            latest_realized_pnl = _float_or(0.0, row.get("realized_pnl"))
        if event == "config":
            configured_windows = _int_or_none(row.get("windows"))
            poly_source = row.get("poly_source") if isinstance(row.get("poly_source"), dict) else {}
            entry_start_age_sec = _float_or(entry_start_age_sec, poly_source.get("entry_start_age_sec"))
            entry_end_age_sec = _float_or(entry_end_age_sec, poly_source.get("entry_end_age_sec"))
        elif event == "window_selected":
            completed_windows = max(completed_windows, int(row.get("completed_windows") or 0))
            current_window = {
                "market_slug": row.get("market_slug"),
                "window_start": row.get("window_start"),
                "window_end": row.get("window_end"),
                "selected_ts": row.get("ts"),
            }
            _add_window_display(current_window)
            window_index = int(row.get("completed_windows") or 0) + 1
            record = _window_record(row, window_index=window_index)
            slug = str(record["market_slug"])
            if slug not in window_index_by_slug:
                window_index_by_slug[slug] = window_index
                window_records.append(record)
        elif event == "tick":
            latest_tick = _latest_tick(row)
            current_window.setdefault("market_slug", row.get("market_slug"))
            if isinstance(row.get("position"), dict):
                open_position = row.get("position")
            elif row.get("position") is None:
                open_position = None
            realized_pnl = _float_or(realized_pnl, row.get("realized_pnl"))
        elif event == "entry":
            item = _trade_item(row, intent="entry", success_default=True)
            _attach_cashflow(row, item, kind="buy")
            entries.append(item)
            cashflows.append(_cashflow_item(row, item, kind="buy"))
            if isinstance(row.get("position_after_entry"), dict):
                open_position = row["position_after_entry"]
            elif item.get("status") == "filled":
                open_position = _position_from_entry(row)
            realized_pnl = _float_or(realized_pnl, row.get("realized_pnl"))
        elif event == "exit":
            item = _trade_item(row, intent="exit", success_default=True)
            _attach_cashflow(row, item, kind="sell")
            exits.append(item)
            cashflows.append(_cashflow_item(row, item, kind="sell"))
            open_position = row.get("position_after_exit") if "position_after_exit" in row else None
            if row.get("exit_pnl") is not None:
                item["pnl"] = round(float(row["exit_pnl"]), 6)
                pnl_events.append(float(row["exit_pnl"]))
            realized_pnl = _float_or(realized_pnl, row.get("realized_pnl"))
            if row.get("exit_pnl") is not None and row.get("realized_pnl") is None:
                realized_pnl = round(realized_pnl + float(row["exit_pnl"]), 6)
        elif event == "position_reduce":
            item = _trade_item(row, intent="exit", success_default=True)
            item["status"] = "partial"
            _attach_cashflow(row, item, kind="sell")
            exits.append(item)
            cashflows.append(_cashflow_item(row, item, kind="sell"))
            open_position = row.get("position_after_exit") or row.get("position") or _position_after_reduce(row)
            if row.get("exit_pnl") is not None:
                item["pnl"] = round(float(row["exit_pnl"]), 6)
                pnl_events.append(float(row["exit_pnl"]))
            realized_pnl = _float_or(realized_pnl, row.get("realized_pnl"))
        elif event == "order_no_fill":
            intent = "exit" if row.get("exit_intent") == "exit" or row.get("order_intent") == "exit" else "entry"
            item = _trade_item(row, intent=intent, success_default=False)
            item["status"] = "failed"
            item["failure_reason"] = _failure_reason(row)
            (exits if intent == "exit" else entries).append(item)
        elif event in {"settlement", "window_settlement"}:
            _apply_window_settlement(window_records, row)
            if event == "settlement" and row.get("settlement_pnl") is not None:
                pnl_events.append(float(row["settlement_pnl"]))
        elif event == "fatal_stop":
            fatal_stop_reason = row.get("fatal_stop_reason") or row.get("reason")
            errors.append({"ts": row.get("ts"), "event": event, "reason": fatal_stop_reason})
        elif event in {"error", "volatility_startup_failed"}:
            errors.append({"ts": row.get("ts"), "event": event, "reason": row.get("error") or row.get("action")})

        row_warnings = row.get("warnings")
        if isinstance(row_warnings, list):
            warnings.extend(str(item) for item in row_warnings[-3:])
        elif isinstance(row_warnings, str):
            warnings.append(row_warnings)

    pending_windows = None
    if configured_windows is not None:
        pending_windows = max(0, configured_windows - completed_windows)
    if pending_windows == 0 and current_window:
        current_window["remaining_sec"] = 0
        current_window["window_end_remaining_sec"] = 0
        current_window["pre_start_sec"] = 0
    elif current_window and current_window.get("window_end"):
        _add_window_timing(current_window, now_utc)
    if current_window:
        _add_entry_window_status(
            current_window,
            entries=entries,
            entry_start_age_sec=entry_start_age_sec,
            entry_end_age_sec=entry_end_age_sec,
        )
    run_remaining_sec = _run_remaining_sec(
        current_window=current_window,
        configured_windows=configured_windows,
        completed_windows=completed_windows,
    )
    if pnl_events:
        realized_pnl = round(sum(pnl_events), 6)
    elif latest_realized_pnl is not None:
        realized_pnl = latest_realized_pnl

    return {
        "configured_windows": configured_windows,
        "completed_windows": completed_windows,
        "pending_windows": pending_windows,
        "run_remaining_sec": run_remaining_sec,
        "current_window": current_window or None,
        "latest_tick": latest_tick,
        "entries": entries[-25:],
        "exits": exits[-25:],
        "trades": _merge_trades(entries, exits, window_index_by_slug=window_index_by_slug)[-25:],
        "window_records": window_records[-50:],
        "cashflows": [item for item in cashflows if item is not None][-50:],
        "realized_pnl": round(realized_pnl, 6),
        "open_position": open_position,
        "warnings": warnings[-20:],
        "errors": errors[-20:],
        "fatal_stop_reason": fatal_stop_reason,
        "last_event_ts": last_ts,
    }


def _trade_item(row: dict[str, Any], *, intent: str, success_default: bool) -> dict[str, Any]:
    order = row.get("order") if isinstance(row.get("order"), dict) else {}
    decision = row.get("decision") if isinstance(row.get("decision"), dict) else {}
    position_before_exit = row.get("position_before_exit") if isinstance(row.get("position_before_exit"), dict) else {}
    side = row.get(f"{intent}_side") or row.get("side") or decision.get("side") or position_before_exit.get("token_side")
    price = row.get(f"{intent}_price")
    shares = row.get(f"{intent}_shares")
    if price is None and intent == "entry":
        price = row.get("entry_avg_price")
    if price is None and intent == "exit":
        price = row.get("avg_price")
    if shares is None:
        shares = order.get("filled_size") or row.get("shares")
    success = bool(order.get("success", success_default))
    return {
        "ts": row.get("ts"),
        "mode": row.get("mode"),
        "market_slug": row.get("market_slug"),
        "side": side,
        "status": "filled" if success else "failed",
        "price": _round_or_none(price),
        "shares": _round_or_none(shares),
        "reason": row.get("reason") or row.get(f"{intent}_reason") or decision.get("reason"),
        "reason_text": translate_reason(
            row.get("reason")
            or row.get(f"{intent}_reason")
            or decision.get("reason")
            or order.get("message")
            or row.get("message")
        ),
        "message": order.get("message") or row.get("message"),
        "order_result_text": _order_result_text(row, success=success),
    }


def _position_from_entry(row: dict[str, Any]) -> dict[str, Any] | None:
    side = row.get("entry_side") or row.get("side")
    price = row.get("entry_price")
    shares = row.get("entry_shares")
    if side is None or price is None or shares is None:
        return None
    return {
        "market_slug": row.get("market_slug"),
        "token_side": side,
        "entry_time": row.get("age_sec"),
        "entry_avg_price": _round_or_none(price),
        "filled_shares": _round_or_none(shares),
        "exit_status": "open",
    }


def _position_after_reduce(row: dict[str, Any]) -> dict[str, Any] | None:
    remaining = row.get("remaining_shares")
    if remaining is None:
        return None
    before = row.get("position_before_exit") if isinstance(row.get("position_before_exit"), dict) else {}
    return {
        "market_slug": row.get("market_slug"),
        "token_side": before.get("token_side") or row.get("exit_side") or row.get("side"),
        "entry_time": before.get("entry_time"),
        "entry_avg_price": before.get("entry_avg_price"),
        "filled_shares": _round_or_none(remaining),
        "exit_status": "residual_open",
    }


def _merge_trades(
    entries: list[dict[str, Any]],
    exits: list[dict[str, Any]],
    *,
    window_index_by_slug: dict[str, int],
) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    used_exit_indexes: set[int] = set()
    for entry in entries:
        exit_index, exit_row = _find_exit_for_entry(entry, exits, used_exit_indexes)
        if exit_index is not None:
            used_exit_indexes.add(exit_index)
        trades.append(_trade_record(entry, exit_row, window_index_by_slug=window_index_by_slug))
    for index, exit_row in enumerate(exits):
        if index in used_exit_indexes:
            continue
        trades.append(_trade_record(None, exit_row, window_index_by_slug=window_index_by_slug))
    return trades


def _find_exit_for_entry(
    entry: dict[str, Any],
    exits: list[dict[str, Any]],
    used_exit_indexes: set[int],
) -> tuple[int | None, dict[str, Any] | None]:
    for index, exit_row in enumerate(exits):
        if index in used_exit_indexes:
            continue
        if exit_row.get("market_slug") != entry.get("market_slug"):
            continue
        if exit_row.get("side") != entry.get("side"):
            continue
        if _ts_sort_key(exit_row.get("ts")) < _ts_sort_key(entry.get("ts")):
            continue
        return index, exit_row
    return None, None


def _trade_record(
    entry: dict[str, Any] | None,
    exit_row: dict[str, Any] | None,
    *,
    window_index_by_slug: dict[str, int],
) -> dict[str, Any]:
    source = entry or exit_row or {}
    slug = str(source.get("market_slug") or "")
    return {
        "window_index": window_index_by_slug.get(slug),
        "direction": _display_side(source.get("side")),
        "market_slug": source.get("market_slug"),
        "buy_time": _format_bj(_parse_ts(entry.get("ts"))) if entry else None,
        "buy_price": entry.get("price") if entry else None,
        "buy_status": entry.get("status") if entry else None,
        "buy_reason": _trade_buy_reason(entry),
        "sell_time": _format_bj(_parse_ts(exit_row.get("ts"))) if exit_row else None,
        "sell_price": exit_row.get("price") if exit_row else None,
        "pnl": exit_row.get("pnl") if exit_row else None,
        "exit_reason": _trade_exit_reason(exit_row),
    }


def _trade_buy_reason(entry: dict[str, Any] | None) -> str | None:
    if entry is None:
        return None
    if entry.get("status") == "failed":
        return translate_reason(entry.get("failure_reason") or entry.get("message") or entry.get("reason"))
    return entry.get("order_result_text") or "成功"


def _trade_exit_reason(exit_row: dict[str, Any] | None) -> str | None:
    if exit_row is None:
        return None
    if exit_row.get("status") == "failed":
        return translate_reason(exit_row.get("failure_reason") or exit_row.get("reason") or exit_row.get("message"))
    return translate_reason(exit_row.get("reason") or exit_row.get("message"))


def translate_reason(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value)
    normalized = raw.strip().lower().replace(" ", "_")
    mapping = {
        "poly_edge": "策略信号触发",
        "edge": "策略信号触发",
        "poly_hold_score_exit": "持仓评分转弱，主动退出",
        "hold_to_settlement": "接近结算，继续持有",
        "final_force_exit": "临近结束，强制退出",
        "risk_exit": "风控退出",
        "market_disagrees_exit": "盘口走势转弱，风控退出",
        "polymarket_divergence_exit": "参考价格背离，风控退出",
        "order_no_fill": "未成交，盘口流动性不足或价格未触达",
        "no_fill": "未成交，盘口流动性不足或价格未触达",
        "no_liquidity": "未成交，盘口流动性不足或价格未触达",
        "stale_book": "盘口数据过旧，跳过",
        "stale_book_wait": "等待盘口数据刷新",
        "missing_exit_depth": "卖出盘口深度不足",
        "fatal_stop": "运行触发保护停止",
        "live_insufficient_cash_balance": "账户余额不足，已停止",
        "live_no_sellable_balance": "当前代币无可卖余额",
    }
    if normalized in mapping:
        return mapping[normalized]
    if "paper_buy_filled" in normalized or "paper_sell_filled" in normalized:
        return "成功（模拟）"
    if "matched" in normalized or "filled" in normalized:
        return "成功"
    if "no_match" in normalized or "no_orders" in normalized or "unmatched" in normalized:
        return "未成交，盘口流动性不足或价格未触达"
    if "service_not_ready" in normalized or "425" in normalized:
        return "交易服务暂不可用，稍后重试"
    if "no_fill" in normalized or "no_orders_found" in normalized:
        return "未成交，盘口流动性不足或价格未触达"
    if "insufficient" in normalized and "balance" in normalized:
        return "账户余额不足，已停止"
    return "其他原因"


def _order_result_text(row: dict[str, Any], *, success: bool) -> str:
    if success:
        return "成功（模拟）" if row.get("mode") == "paper" else "成功"
    order = row.get("order") if isinstance(row.get("order"), dict) else {}
    return translate_reason(row.get("failure_reason") or order.get("message") or row.get("message") or row.get("reason")) or "失败"


def _cashflow_item(row: dict[str, Any], trade: dict[str, Any], *, kind: str) -> dict[str, Any] | None:
    price = trade.get("price")
    shares = trade.get("shares")
    if price is None or shares is None or trade.get("status") == "failed":
        return None
    amount = round(float(price) * float(shares), 6)
    if kind == "buy":
        amount = -amount
    return {
        "ts": row.get("ts"),
        "market_slug": row.get("market_slug"),
        "kind": kind,
        "side": trade.get("side"),
        "price": price,
        "shares": shares,
        "cashflow": amount,
    }


def _attach_cashflow(row: dict[str, Any], trade: dict[str, Any], *, kind: str) -> None:
    item = _cashflow_item(row, trade, kind=kind)
    if item is not None:
        trade["cashflow"] = item["cashflow"]


def _latest_tick(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in ("ts", "market_slug", "remaining_sec", "k_price", "polymarket_price", "lead_polymarket_return_10s_bps")
        if key in row
    }


def _window_record(row: dict[str, Any], *, window_index: int) -> dict[str, Any]:
    start = _parse_ts(row.get("window_start"))
    end = _parse_ts(row.get("window_end"))
    return {
        "window_index": window_index,
        "market_slug": row.get("market_slug"),
        "event_type": _event_type(row.get("market_slug")),
        "window_date": _format_bj_date(start),
        "window_start_time": _format_bj_time(start),
        "window_end_time": _format_bj_time(end),
        "actual_settlement_side": "未知",
    }


def _apply_window_settlement(window_records: list[dict[str, Any]], row: dict[str, Any]) -> None:
    slug = row.get("market_slug")
    if not slug:
        return
    side = _display_side(row.get("winning_side")) or "未知"
    for record in window_records:
        if record.get("market_slug") == slug:
            record["actual_settlement_side"] = side
            return


def _add_window_display(window: dict[str, Any]) -> None:
    event_type = _event_type(window.get("market_slug"))
    start = _parse_ts(window.get("window_start"))
    end = _parse_ts(window.get("window_end"))
    window["event_type"] = event_type
    window["window_start_bj"] = _format_bj(start)
    window["window_end_bj"] = _format_bj(end)
    if start is not None and end is not None:
        window["display_name"] = (
            f"{event_type}\n"
            f"{start.astimezone(BEIJING).strftime('%Y-%m-%d %H:%M:%S')} 至 "
            f"{end.astimezone(BEIJING).strftime('%H:%M:%S')}"
        )
    else:
        window["display_name"] = event_type


def _event_type(market_slug: Any) -> str:
    slug = str(market_slug or "")
    if slug.startswith("btc-updown-5m-"):
        return "BTC 5min Up/Down"
    return slug or "未知事件"


def _format_bj(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(BEIJING).strftime("%Y-%m-%d %H:%M:%S")


def _format_bj_date(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(BEIJING).strftime("%Y-%m-%d")


def _format_bj_time(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(BEIJING).strftime("%H:%M:%S")


def _format_bj_compact(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(BEIJING).strftime("%Y-%m-%d %H-%M-%S")


def _display_side(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).lower()
    if raw == "up":
        return "UP"
    if raw == "down":
        return "DOWN"
    return str(value)


def _ts_sort_key(value: Any) -> str:
    return str(value or "")


def _remaining_until_window_end(value: Any, now_utc: dt.datetime) -> int | None:
    parsed = _parse_ts(value)
    if parsed is None:
        return None
    return max(0, int((parsed - now_utc).total_seconds()))


def _add_window_timing(window: dict[str, Any], now_utc: dt.datetime) -> None:
    start = _parse_ts(window.get("window_start"))
    end = _parse_ts(window.get("window_end"))
    if end is None:
        return
    total_remaining = max(0, int((end - now_utc).total_seconds()))
    pre_start = max(0, int((start - now_utc).total_seconds())) if start is not None else 0
    if start is not None and now_utc < start:
        current_remaining = max(0, int((end - start).total_seconds()))
    else:
        current_remaining = total_remaining
    window["remaining_sec"] = current_remaining
    window["window_end_remaining_sec"] = total_remaining
    window["pre_start_sec"] = pre_start


def _add_entry_window_status(
    window: dict[str, Any],
    *,
    entries: list[dict[str, Any]],
    entry_start_age_sec: float,
    entry_end_age_sec: float,
) -> None:
    window["entry_start_age_sec"] = entry_start_age_sec
    window["entry_end_age_sec"] = entry_end_age_sec
    window["entry_window_missed"] = False
    window["entry_window_message"] = None

    start = _parse_ts(window.get("window_start"))
    selected = _parse_ts(window.get("selected_ts"))
    if start is None or selected is None:
        return

    market_slug = window.get("market_slug")
    has_entry = any(item.get("market_slug") == market_slug and item.get("status") == "filled" for item in entries)
    selected_age_sec = (selected - start).total_seconds()
    if selected_age_sec > entry_end_age_sec and not has_entry:
        window["entry_window_missed"] = True
        window["entry_window_message"] = "错过交易窗口，跳过"


def _run_remaining_sec(
    *,
    current_window: dict[str, Any],
    configured_windows: int | None,
    completed_windows: int,
) -> int | None:
    if not current_window:
        return None
    current_to_end = current_window.get("window_end_remaining_sec")
    if current_to_end is None:
        current_to_end = current_window.get("remaining_sec")
    if current_to_end is None:
        return None
    duration = _window_duration_sec(current_window) or 300
    if int(current_window.get("pre_start_sec") or 0) > 0:
        active_windows = 1
        if configured_windows is not None:
            active_windows = max(0, configured_windows - completed_windows)
        return active_windows * duration
    future_windows = 0
    if configured_windows is not None:
        # pending includes the current selected window while it is running.
        future_windows = max(0, configured_windows - completed_windows - 1)
    return int(current_to_end) + future_windows * duration


def _window_duration_sec(window: dict[str, Any]) -> int | None:
    start = _parse_ts(window.get("window_start"))
    end = _parse_ts(window.get("window_end"))
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds()))


def _infer_finished(summary: dict[str, Any], now_utc: dt.datetime) -> str:
    if summary.get("fatal_stop_reason"):
        return "finished"
    pending = summary.get("pending_windows")
    remaining = (summary.get("current_window") or {}).get("remaining_sec") if isinstance(summary.get("current_window"), dict) else None
    if pending == 0 and (remaining is None or remaining == 0):
        return "finished"
    if remaining == 0 and summary.get("fatal_stop_reason") is None:
        return "finished"
    last_ts = _parse_ts(summary.get("last_event_ts"))
    if last_ts and (now_utc - last_ts).total_seconds() > 120:
        return "finished"
    return "unknown"


def _failure_reason(row: dict[str, Any]) -> str | None:
    order = row.get("order") if isinstance(row.get("order"), dict) else {}
    return row.get("skip_reason") or row.get("reason") or order.get("message") or row.get("message")


def _validate_mode(mode: str) -> str:
    if mode not in {"live", "paper"}:
        raise ValueError("mode must be live or paper")
    return mode


def _parse_ts(value: Any) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _iso_from_timestamp(value: float) -> str:
    return dt.datetime.fromtimestamp(value, dt.timezone.utc).isoformat()


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _float_or(default: float, value: Any) -> float:
    if value is None:
        return default
    return float(value)


def _round_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize latest live/paper bot log for dashboard display")
    parser.add_argument("--mode", choices=("live", "paper"), required=True)
    parser.add_argument("--log-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    print(json.dumps(build_dashboard_status(args.mode, args.log_dir), ensure_ascii=False, indent=2, sort_keys=True))
    return 0
