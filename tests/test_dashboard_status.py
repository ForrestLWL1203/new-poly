from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from new_poly.dashboard.status import build_dashboard_status


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n", encoding="utf-8")


def test_dashboard_status_summarizes_latest_finished_paper_run(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-3w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 3},
            {
                "ts": "2026-05-13T01:05:00+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:05:00+00:00",
                "window_end": "2026-05-13T01:10:00+00:00",
                "completed_windows": 1,
            },
            {
                "ts": "2026-05-13T01:07:10+00:00",
                "event": "entry",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "entry_side": "up",
                "entry_price": 0.42,
                "entry_shares": 2.5,
                "order": {"success": True, "message": "paper buy filled"},
                "realized_pnl": 0.0,
            },
            {
                "ts": "2026-05-13T01:08:00+00:00",
                "event": "exit",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "exit_side": "up",
                "exit_price": 0.61,
                "exit_shares": 2.5,
                "exit_pnl": 0.475,
                "order": {"success": True, "message": "paper sell filled"},
                "realized_pnl": 0.475,
            },
            {
                "ts": "2026-05-13T01:10:00+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-2",
                "window_start": "2026-05-13T01:10:00+00:00",
                "window_end": "2026-05-13T01:15:00+00:00",
                "completed_windows": 3,
            },
            {
                "ts": "2026-05-13T01:10:01+00:00",
                "event": "tick",
                "mode": "paper",
                "market_slug": "btc-updown-5m-2",
                "remaining_sec": 299,
                "polymarket_price": 81234.5,
            },
        ],
    )

    status = build_dashboard_status("paper", tmp_path, running_pids=[], now=dt.datetime(2026, 5, 13, 1, 16, tzinfo=dt.timezone.utc))

    assert status["mode"] == "paper"
    assert status["run_status"] == "finished"
    assert status["configured_windows"] == 3
    assert status["completed_windows"] == 3
    assert status["pending_windows"] == 0
    assert status["current_window"]["market_slug"] == "btc-updown-5m-2"
    assert status["current_window"]["remaining_sec"] == 0
    assert status["realized_pnl"] == 0.475
    assert status["entries"][0]["cashflow"] == -1.05
    assert status["exits"][0]["cashflow"] == 1.525


def test_dashboard_status_reconstructs_position_from_compact_live_events(tmp_path: Path) -> None:
    log = tmp_path / "live-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "live", "windows": 1},
            {
                "ts": "2026-05-13T01:00:01+00:00",
                "event": "window_selected",
                "mode": "live",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:00:00+00:00",
                "window_end": "2026-05-13T01:05:00+00:00",
            },
            {
                "ts": "2026-05-13T01:02:10+00:00",
                "event": "entry",
                "mode": "live",
                "market_slug": "btc-updown-5m-1",
                "age_sec": 130,
                "entry_side": "up",
                "entry_price": 0.42,
                "entry_shares": 2.5,
                "order": {"success": True},
            },
        ],
    )

    status = build_dashboard_status(
        "live",
        tmp_path,
        running_pids=[123],
        now=dt.datetime(2026, 5, 13, 1, 2, 20, tzinfo=dt.timezone.utc),
    )

    assert status["open_position"]["token_side"] == "up"
    assert status["open_position"]["entry_avg_price"] == 0.42
    assert status["open_position"]["filled_shares"] == 2.5


def test_dashboard_status_prefers_running_log_and_reports_order_no_fill(tmp_path: Path) -> None:
    old_log = tmp_path / "live-sweden-12w-20260512T010000Z.jsonl"
    new_log = tmp_path / "live-sweden-12w-20260513T010000Z.jsonl"
    _write_jsonl(old_log, [{"ts": "2026-05-12T01:00:00+00:00", "event": "config", "mode": "live", "windows": 12}])
    _write_jsonl(
        new_log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "live", "windows": 12},
            {
                "ts": "2026-05-13T01:02:00+00:00",
                "event": "order_no_fill",
                "mode": "live",
                "market_slug": "btc-updown-5m-live",
                "order_intent": "entry",
                "entry_side": "down",
                "reason": "no_liquidity",
                "order": {"success": False, "message": "no fill"},
            },
            {
                "ts": "2026-05-13T01:02:30+00:00",
                "event": "tick",
                "mode": "live",
                "market_slug": "btc-updown-5m-live",
                "remaining_sec": 120,
                "position": {"token_side": "down", "entry_avg_price": 0.33, "filled_shares": 3.0},
            },
        ],
    )

    status = build_dashboard_status("live", tmp_path, running_pids=[4321], now=dt.datetime(2026, 5, 13, 1, 3, tzinfo=dt.timezone.utc))

    assert status["run_status"] == "running"
    assert status["pid"] == 4321
    assert status["log_path"].endswith(new_log.name)
    assert status["entries"][0]["status"] == "failed"
    assert status["entries"][0]["failure_reason"] == "no_liquidity"
    assert status["open_position"]["token_side"] == "down"


def test_dashboard_status_tolerates_bad_json_lines(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    log.write_text('{"ts":"2026-05-13T01:00:00+00:00","event":"config","mode":"paper","windows":1}\nnot json\n', encoding="utf-8")

    status = build_dashboard_status("paper", tmp_path, running_pids=[])

    assert status["parse_warnings"][0]["line"] == 2


def test_dashboard_status_recomputes_remaining_from_window_end(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 1},
            {
                "ts": "2026-05-13T01:00:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:05:00+00:00",
                "window_end": "2026-05-13T01:10:00+00:00",
            },
            {
                "ts": "2026-05-13T01:00:02+00:00",
                "event": "tick",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "remaining_sec": 600,
            },
        ],
    )

    status = build_dashboard_status(
        "paper",
        tmp_path,
        running_pids=[123],
        now=dt.datetime(2026, 5, 13, 1, 1, tzinfo=dt.timezone.utc),
    )

    assert status["current_window"]["remaining_sec"] == 300
    assert status["current_window"]["window_end_remaining_sec"] == 540
    assert status["current_window"]["pre_start_sec"] == 240
    assert status["run_remaining_sec"] == 300


def test_dashboard_status_keeps_total_remaining_constant_before_selected_window_starts(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-2w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 2},
            {
                "ts": "2026-05-13T01:00:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:05:00+00:00",
                "window_end": "2026-05-13T01:10:00+00:00",
                "completed_windows": 0,
            },
        ],
    )

    status = build_dashboard_status(
        "paper",
        tmp_path,
        running_pids=[123],
        now=dt.datetime(2026, 5, 13, 1, 1, tzinfo=dt.timezone.utc),
    )

    assert status["current_window"]["pre_start_sec"] == 240
    assert status["run_remaining_sec"] == 600


def test_dashboard_status_reports_in_window_remaining_without_prestart_time(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 1},
            {
                "ts": "2026-05-13T01:00:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:05:00+00:00",
                "window_end": "2026-05-13T01:10:00+00:00",
            },
        ],
    )

    status = build_dashboard_status(
        "paper",
        tmp_path,
        running_pids=[123],
        now=dt.datetime(2026, 5, 13, 1, 7, tzinfo=dt.timezone.utc),
    )

    assert status["current_window"]["remaining_sec"] == 180
    assert status["current_window"]["window_end_remaining_sec"] == 180
    assert status["current_window"]["pre_start_sec"] == 0
    assert status["run_remaining_sec"] == 180


def test_dashboard_status_reports_total_run_remaining_across_future_windows(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-2w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 2},
            {
                "ts": "2026-05-13T01:00:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:00:00+00:00",
                "window_end": "2026-05-13T01:05:00+00:00",
                "completed_windows": 0,
            },
        ],
    )

    status = build_dashboard_status(
        "paper",
        tmp_path,
        running_pids=[123],
        now=dt.datetime(2026, 5, 13, 1, 1, 30, tzinfo=dt.timezone.utc),
    )

    assert status["current_window"]["remaining_sec"] == 210
    assert status["run_remaining_sec"] == 510


def test_dashboard_status_flags_window_selected_after_entry_window(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-2w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {
                "ts": "2026-05-13T01:00:00+00:00",
                "event": "config",
                "mode": "paper",
                "windows": 2,
                "poly_source": {"entry_start_age_sec": 120, "entry_end_age_sec": 220},
            },
            {
                "ts": "2026-05-13T01:04:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:00:00+00:00",
                "window_end": "2026-05-13T01:05:00+00:00",
                "completed_windows": 0,
            },
        ],
    )

    status = build_dashboard_status(
        "paper",
        tmp_path,
        running_pids=[123],
        now=dt.datetime(2026, 5, 13, 1, 4, 10, tzinfo=dt.timezone.utc),
    )

    assert status["current_window"]["entry_window_missed"] is True
    assert status["current_window"]["entry_window_message"] == "错过交易窗口，跳过"


def test_dashboard_status_does_not_flag_normal_window_after_entry_window_passes(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {
                "ts": "2026-05-13T01:00:00+00:00",
                "event": "config",
                "mode": "paper",
                "windows": 1,
                "poly_source": {"entry_start_age_sec": 120, "entry_end_age_sec": 220},
            },
            {
                "ts": "2026-05-13T01:00:05+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:00:00+00:00",
                "window_end": "2026-05-13T01:05:00+00:00",
                "completed_windows": 0,
            },
        ],
    )

    status = build_dashboard_status(
        "paper",
        tmp_path,
        running_pids=[123],
        now=dt.datetime(2026, 5, 13, 1, 4, 10, tzinfo=dt.timezone.utc),
    )

    assert status["current_window"]["entry_window_missed"] is False
    assert status["current_window"]["entry_window_message"] is None


def test_dashboard_status_reports_stopped_run_before_window_end(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 1},
            {
                "ts": "2026-05-13T01:00:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:05:00+00:00",
                "window_end": "2026-05-13T01:10:00+00:00",
            },
        ],
    )

    status = build_dashboard_status(
        "paper",
        tmp_path,
        running_pids=[],
        stopped=True,
        now=dt.datetime(2026, 5, 13, 1, 6, tzinfo=dt.timezone.utc),
    )

    assert status["run_status"] == "stopped"
    assert status["current_window"]["remaining_sec"] == 240
    assert status["pending_windows"] == 1


def test_dashboard_status_marks_single_window_run_finished_after_window_end_without_next_window(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 1},
            {
                "ts": "2026-05-13T01:00:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:00:00+00:00",
                "window_end": "2026-05-13T01:05:00+00:00",
            },
            {
                "ts": "2026-05-13T01:04:30+00:00",
                "event": "tick",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "remaining_sec": 30,
            },
        ],
    )

    status = build_dashboard_status(
        "paper",
        tmp_path,
        running_pids=[],
        now=dt.datetime(2026, 5, 13, 1, 6, tzinfo=dt.timezone.utc),
    )

    assert status["run_status"] == "finished"
    assert status["completed_windows"] == 1
    assert status["pending_windows"] == 0


def test_dashboard_status_adds_beijing_window_display(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 1},
            {
                "ts": "2026-05-13T01:00:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1778655300",
                "window_start": "2026-05-13T06:55:00+00:00",
                "window_end": "2026-05-13T07:00:00+00:00",
            },
        ],
    )

    status = build_dashboard_status("paper", tmp_path, running_pids=[123], now=dt.datetime(2026, 5, 13, 6, 56, tzinfo=dt.timezone.utc))

    assert status["current_window"]["event_type"] == "BTC 5min Up/Down"
    assert status["current_window"]["window_start_bj"] == "2026-05-13 14:55:00"
    assert status["current_window"]["window_end_bj"] == "2026-05-13 15:00:00"
    assert status["current_window"]["display_name"] == "BTC 5min Up/Down\n2026-05-13 14:55:00 至 15:00:00"


def test_dashboard_status_builds_window_records_and_trade_window_index(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-2w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 2},
            {
                "ts": "2026-05-13T01:00:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:05:00+00:00",
                "window_end": "2026-05-13T01:10:00+00:00",
                "completed_windows": 0,
            },
            {
                "ts": "2026-05-13T01:07:00+00:00",
                "event": "entry",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "entry_side": "up",
                "entry_price": 0.42,
                "entry_shares": 2.5,
                "order": {"success": True, "message": "paper buy filled"},
            },
            {
                "ts": "2026-05-13T01:11:30+00:00",
                "event": "window_settlement",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "winning_side": "up",
                "window_start": "2026-05-13T01:05:00+00:00",
                "window_end": "2026-05-13T01:10:00+00:00",
            },
            {
                "ts": "2026-05-13T01:10:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-2",
                "window_start": "2026-05-13T01:10:00+00:00",
                "window_end": "2026-05-13T01:15:00+00:00",
                "completed_windows": 1,
            },
        ],
    )

    status = build_dashboard_status("paper", tmp_path, running_pids=[123], now=dt.datetime(2026, 5, 13, 1, 12, tzinfo=dt.timezone.utc))

    assert status["window_records"] == [
        {
            "window_index": 1,
            "market_slug": "btc-updown-5m-1",
            "event_type": "BTC 5min Up/Down",
            "window_date": "2026-05-13",
            "window_start_time": "09:05:00",
            "window_end_time": "09:10:00",
            "actual_settlement_side": "UP",
        },
    ]
    assert status["trades"][0]["window_index"] == 1


def test_dashboard_status_does_not_record_selected_window_without_trade(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 1},
            {
                "ts": "2026-05-13T01:00:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:05:00+00:00",
                "window_end": "2026-05-13T01:10:00+00:00",
                "completed_windows": 0,
            },
            {
                "ts": "2026-05-13T01:00:02+00:00",
                "event": "tick",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "remaining_sec": 500,
            },
        ],
    )

    status = build_dashboard_status("paper", tmp_path, running_pids=[123], now=dt.datetime(2026, 5, 13, 1, 1, tzinfo=dt.timezone.utc))

    assert status["window_records"] == []
    assert status["current_window"]["market_slug"] == "btc-updown-5m-1"


def test_dashboard_status_records_entry_no_fill_window(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 1},
            {
                "ts": "2026-05-13T01:00:01+00:00",
                "event": "window_selected",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "window_start": "2026-05-13T01:05:00+00:00",
                "window_end": "2026-05-13T01:10:00+00:00",
                "completed_windows": 0,
            },
            {
                "ts": "2026-05-13T01:07:00+00:00",
                "event": "order_no_fill",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "order_intent": "entry",
                "entry_side": "up",
                "reason": "entry_no_fill",
                "order": {"success": False, "message": "paper no fill"},
            },
        ],
    )

    status = build_dashboard_status("paper", tmp_path, running_pids=[])

    assert status["window_records"] == [
        {
            "window_index": 1,
            "market_slug": "btc-updown-5m-1",
            "event_type": "BTC 5min Up/Down",
            "window_date": "2026-05-13",
            "window_start_time": "09:05:00",
            "window_end_time": "09:10:00",
            "actual_settlement_side": "未知",
        }
    ]
    assert status["trades"][0]["window_index"] == 1
    assert status["trades"][0]["buy_status"] == "failed"


def test_dashboard_status_merges_settlement_into_trade_record(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 1},
            {
                "ts": "2026-05-13T01:07:00+00:00",
                "event": "entry",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "entry_side": "down",
                "entry_price": 0.54,
                "entry_shares": 1.851852,
                "amount_usd": 1.0,
                "order": {"success": True, "message": "paper buy filled"},
            },
            {
                "ts": "2026-05-13T01:11:30+00:00",
                "event": "settlement",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "winning_side": "down",
                "position": {
                    "token_side": "down",
                    "entry_avg_price": 0.54,
                    "filled_shares": 1.851852,
                    "entry_amount_usd": 1.0,
                },
                "settlement_pnl": 0.851852,
                "realized_pnl": 0.851852,
            },
        ],
    )

    status = build_dashboard_status("paper", tmp_path, running_pids=[])

    assert status["trades"][0]["sell_time"] == "2026-05-13 09:11:30"
    assert status["trades"][0]["sell_price"] == 1.0
    assert status["trades"][0]["pnl"] == 0.851852
    assert status["trades"][0]["exit_reason"] == "结算完成"


def test_dashboard_status_sums_exit_pnl_when_exit_rows_have_stale_realized_pnl(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-2w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 2},
            {
                "ts": "2026-05-13T01:03:00+00:00",
                "event": "exit",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "exit_side": "down",
                "exit_price": 0.65,
                "exit_shares": 1.45,
                "exit_pnl": -0.058,
                "realized_pnl": 0.0,
                "order": {"success": True},
            },
            {
                "ts": "2026-05-13T01:08:00+00:00",
                "event": "exit",
                "mode": "paper",
                "market_slug": "btc-updown-5m-2",
                "exit_side": "up",
                "exit_price": 0.68,
                "exit_shares": 1.43,
                "exit_pnl": -0.0286,
                "realized_pnl": -0.058,
                "order": {"success": True},
            },
        ],
    )

    status = build_dashboard_status("paper", tmp_path, running_pids=[])

    assert status["realized_pnl"] == -0.0866


def test_dashboard_status_merges_entry_and_exit_into_trade_record(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "paper", "windows": 1},
            {
                "ts": "2026-05-13T01:02:00+00:00",
                "event": "entry",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "entry_side": "up",
                "entry_price": 0.42,
                "entry_shares": 2.5,
                "amount_usd": 1.05,
                "decision": {"reason": "poly_edge"},
                "order": {"success": True, "message": "paper buy filled"},
            },
            {
                "ts": "2026-05-13T01:03:00+00:00",
                "event": "exit",
                "mode": "paper",
                "market_slug": "btc-updown-5m-1",
                "exit_side": "up",
                "exit_price": 0.61,
                "exit_shares": 2.5,
                "exit_pnl": 0.475,
                "reason": "late_ev_exit",
                "order": {"success": True, "message": "paper sell filled"},
            },
        ],
    )

    status = build_dashboard_status("paper", tmp_path, running_pids=[])

    assert status["trades"] == [
        {
            "window_index": 1,
            "direction": "UP",
            "market_slug": "btc-updown-5m-1",
            "buy_time": "2026-05-13 09:02:00",
            "buy_price": 0.42,
            "buy_amount_usd": 1.05,
            "buy_status": "filled",
            "buy_reason": "成功（模拟）",
            "sell_time": "2026-05-13 09:03:00",
            "sell_price": 0.61,
            "pnl": 0.475,
            "exit_reason": "末期前段 reference 反穿确认，主动止损",
        }
    ]


def test_dashboard_status_uses_position_entry_amount_and_recomputes_missing_exit_pnl(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T020000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T02:00:00+00:00", "event": "config", "mode": "paper", "windows": 1},
            {
                "ts": "2026-05-13T02:02:00+00:00",
                "event": "entry",
                "mode": "paper",
                "market_slug": "btc-updown-5m-2",
                "entry_side": "up",
                "entry_price": 0.60,
                "entry_shares": 5.0,
                "position_after_entry": {
                    "token_side": "up",
                    "entry_avg_price": 0.60,
                    "filled_shares": 5.0,
                    "entry_amount_usd": 3.0,
                },
                "order": {"success": True, "message": "paper buy filled"},
            },
            {
                "ts": "2026-05-13T02:03:00+00:00",
                "event": "exit",
                "mode": "paper",
                "market_slug": "btc-updown-5m-2",
                "exit_side": "up",
                "exit_price": 0.70,
                "exit_shares": 5.0,
                "position_before_exit": {
                    "token_side": "up",
                    "entry_avg_price": 0.60,
                    "filled_shares": 5.0,
                    "entry_amount_usd": 3.0,
                },
                "order": {"success": True, "message": "paper sell filled"},
            },
        ],
    )

    status = build_dashboard_status("paper", tmp_path, running_pids=[])

    assert status["trades"][0]["buy_amount_usd"] == 3.0
    assert status["trades"][0]["pnl"] == 0.5
    assert status["realized_pnl"] == 0.5


def test_dashboard_status_trade_record_keeps_failed_entry_readable(tmp_path: Path) -> None:
    log = tmp_path / "live-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(
        log,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config", "mode": "live", "windows": 1},
            {
                "ts": "2026-05-13T01:02:00+00:00",
                "event": "order_no_fill",
                "mode": "live",
                "market_slug": "btc-updown-5m-1",
                "order_intent": "entry",
                "entry_side": "down",
                "reason": "no_liquidity",
                "order": {"success": False, "message": "no fill"},
            },
        ],
    )

    status = build_dashboard_status("live", tmp_path, running_pids=[])

    assert status["trades"][0]["direction"] == "DOWN"
    assert status["trades"][0]["buy_status"] == "failed"
    assert status["trades"][0]["buy_reason"] == "未成交，盘口流动性不足或价格未触达"
    assert status["trades"][0]["sell_time"] is None
