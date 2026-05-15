from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from pathlib import Path

from new_poly.dashboard.paths import DashboardPaths
from new_poly.dashboard.server import STATIC_DIR
from new_poly.dashboard.server import create_server


class StubController:
    def __init__(self) -> None:
        self.restart_calls = []

    def status(self, mode: str, *, log_stem: str | None = None):
        payload = {"mode": mode, "run_status": "finished"}
        if log_stem:
            payload["log_stem"] = log_stem
        return payload

    def stop(self, mode: str):
        return {"ok": True, "mode": mode}

    def restart(self, mode: str, windows: int):
        self.restart_calls.append((mode, windows))
        return {"ok": True, "mode": mode, "windows": windows}


def _login_cookie(base: str) -> str:
    body = json.dumps({"user": "admin", "password": "secret"}).encode("utf-8")
    req = urllib.request.Request(
        base + "/api/login",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=2) as resp:
        assert json.loads(resp.read()) == {"ok": True}
        cookie = resp.headers["Set-Cookie"]
    return cookie.split(";", 1)[0]


def test_server_requires_session_for_api(tmp_path: Path) -> None:
    paths = DashboardPaths("local", tmp_path, tmp_path, Path("/python"), False)
    server = create_server("127.0.0.1", 0, paths=paths, controller=StubController(), user="admin", password="secret")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/api/status?mode=paper"
    try:
        try:
            urllib.request.urlopen(url, timeout=2)
        except urllib.error.HTTPError as exc:
            assert exc.code == 401
        else:
            raise AssertionError("expected 401")
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_server_login_status_and_restart_api(tmp_path: Path) -> None:
    controller = StubController()
    paths = DashboardPaths("local", tmp_path, tmp_path, Path("/python"), False)
    server = create_server("127.0.0.1", 0, paths=paths, controller=controller, user="admin", password="secret")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        cookie = _login_cookie(base)

        req = urllib.request.Request(base + "/api/status?mode=paper", headers={"Cookie": cookie})
        with urllib.request.urlopen(req, timeout=2) as resp:
            assert json.loads(resp.read()) == {"mode": "paper", "run_status": "finished"}

        req = urllib.request.Request(
            base + "/api/status?mode=paper&stem=paper-sweden-1w-20260513T010000Z",
            headers={"Cookie": cookie},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            assert json.loads(resp.read())["log_stem"] == "paper-sweden-1w-20260513T010000Z"

        body = json.dumps({"mode": "paper", "windows": 7}).encode("utf-8")
        req = urllib.request.Request(
            base + "/api/restart",
            data=body,
            headers={"Cookie": cookie, "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            assert json.loads(resp.read())["windows"] == 7
        assert controller.restart_calls == [("paper", 7)]
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_server_log_list_and_delete_api(tmp_path: Path) -> None:
    stem = "paper-sweden-1w-20260513T010000Z"
    (tmp_path / f"{stem}.jsonl").write_text(
        '{"ts":"2026-05-13T01:00:00+00:00","event":"config"}\n'
        '{"ts":"2026-05-13T01:00:09+00:00","event":"tick"}\n',
        encoding="utf-8",
    )
    (tmp_path / f"{stem}.out").write_text("out\n", encoding="utf-8")
    controller = StubController()
    paths = DashboardPaths("local", tmp_path, tmp_path, Path("/python"), False)
    server = create_server("127.0.0.1", 0, paths=paths, controller=controller, user="admin", password="secret")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        cookie = _login_cookie(base)

        req = urllib.request.Request(base + "/api/logs?mode=paper", headers={"Cookie": cookie})
        with urllib.request.urlopen(req, timeout=2) as resp:
            payload = json.loads(resp.read())
        assert payload["runs"][0]["stem"] == stem
        assert payload["runs"][0]["duration_sec"] == 9

        body = json.dumps({"stems": [stem]}).encode("utf-8")
        req = urllib.request.Request(
            base + "/api/logs/delete",
            data=body,
            headers={"Cookie": cookie, "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            deleted = json.loads(resp.read())
        assert deleted["deleted_stems"] == [stem]
        assert not (tmp_path / f"{stem}.jsonl").exists()
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_server_root_head_redirects_without_session(tmp_path: Path) -> None:
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    paths = DashboardPaths("local", tmp_path, tmp_path, Path("/python"), False)
    server = create_server("127.0.0.1", 0, paths=paths, controller=StubController(), user="admin", password="secret")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    opener = urllib.request.build_opener(NoRedirect)
    try:
        req = urllib.request.Request(f"http://127.0.0.1:{server.server_address[1]}/", method="HEAD")
        try:
            opener.open(req, timeout=2)
        except urllib.error.HTTPError as exc:
            assert exc.code == 302
            assert exc.headers["Location"] == "/login"
        else:
            raise AssertionError("expected redirect")
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_static_countdown_uses_persisted_dashboard_state() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    start = html.index("function updateCountdown()")
    end = html.index("function remainingFromEnd", start)
    update_countdown = html[start:end]

    assert "runRemaining(state.data)" in update_countdown
    assert "state.data.run_remaining_sec" in update_countdown
    assert "runRemaining(data)" not in update_countdown
    assert "?? data.run_remaining_sec" not in update_countdown
    assert "windowInfo.window_end_remaining_sec" in update_countdown


def test_static_current_window_countdown_counts_to_window_end() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    start = html.index("function currentWindowRemaining")
    end = html.index("function currentPreStart", start)
    current_window_remaining = html[start:end]

    assert "remainingFromEnd(endValue)" in current_window_remaining
    assert "window_start" not in current_window_remaining
    assert "end - start" not in current_window_remaining


def test_static_current_window_shows_wait_before_selected_window_starts() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    countdown_start = html.index("function updateCountdown()")
    countdown_end = html.index("function remainingFromEnd", countdown_start)
    countdown = html[countdown_start:countdown_end]
    pre_start_start = html.index("function currentPreStart")
    pre_start_end = html.index("function windowStatusHint", pre_start_start)
    pre_start = html[pre_start_start:pre_start_end]

    assert "currentPreStart(windowInfo)" in countdown
    assert "`等待 ${formatSeconds(currentDynamic)}`" in countdown
    assert 'id="remainingLabel"' in html
    assert '"跳过窗口剩余"' in countdown
    assert '"当前窗口剩余"' in countdown
    assert "windowInfo.window_start" in pre_start
    assert "windowInfo.pre_start_sec" in pre_start


def test_static_current_window_hint_waits_for_next_window() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    start = html.index("function windowStatusHint")
    end = html.index("function runRemaining", start)
    hint = html[start:end]

    assert "目标窗口" in html
    assert "当前窗口</div>" not in html
    assert "跳过当前窗口，等待目标窗口开始" in hint
    assert "currentPreStart(windowInfo)" in hint
    assert "entry_window_missed" in hint
    assert "formatSeconds(preStart)" not in hint


def test_static_total_remaining_excludes_wait_before_selected_window_starts() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    start = html.index("function runRemaining")
    end = html.index("function windowDuration", start)
    run_remaining = html[start:end]

    assert "currentPreStart(windowInfo)" in run_remaining
    assert "activeWindows * duration" in run_remaining
    assert "currentToEnd + futureWindows * duration" in run_remaining


def test_static_dashboard_reconciles_timers_after_status_render() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    render_start = html.index("function render(data)")
    render_end = html.index("function renderTable", render_start)
    render_block = html[render_start:render_end]
    wait_start = html.index("async function waitForStatus")
    wait_end = html.index("function sleep", wait_start)
    wait_block = html[wait_start:wait_end]

    assert "reconcileTimers();" in render_block
    assert "function reconcileTimers()" in html
    assert "function startTimers()" not in html
    assert "reconcileTimers();" in wait_block


def test_static_dashboard_has_blocking_loading_overlay() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    set_loading_start = html.index("function setLoading")
    set_loading_end = html.index("function updateControls", set_loading_start)
    set_loading = html[set_loading_start:set_loading_end]

    assert 'id="loadingOverlay"' in html
    assert 'id="loadingText"' in html
    assert "loadingOverlay" in set_loading
    assert "is-visible" in set_loading
    assert 'aria-busy' in set_loading


def test_static_log_actions_are_single_select_and_disabled_without_selection() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    controls_start = html.index("function updateControls")
    controls_end = html.index("async function waitForStatus", controls_start)
    controls = html[controls_start:controls_end]

    assert "分析日志" in html
    assert "删除日志" in html
    assert "分析所选" not in html
    assert "删除所选" not in html
    assert "handleLogSelectionChange" in html
    assert "node.checked = false" in html
    assert "hasSelectedLog" in controls
    assert "analyzeLog" in controls


def test_static_log_actions_are_locked_while_running() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    controls_start = html.index("function updateControls")
    controls_end = html.index("async function waitForStatus", controls_start)
    controls = html[controls_start:controls_end]
    render_start = html.index("function render(data)")
    render_end = html.index("function selectedLogStems", render_start)
    render_block = html[render_start:render_end]

    assert "function historyLocked()" in html
    assert "clearLogSelection()" in render_block
    assert "historyLocked()" in html
    assert 'data-running="' in html
    assert "node.disabled = Boolean(running)" in controls
    assert "running || !hasSelectedLog" in controls


def test_static_log_table_deletes_rows_with_inline_trash_button() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    logs_start = html.index("function renderLogs")
    logs_end = html.index("function handleLogSelectionChange", logs_start)
    logs_block = html[logs_start:logs_end]

    assert 'class="log-delete icon-button danger"' in logs_block
    assert 'title="删除日志"' in logs_block
    assert "🗑" in logs_block
    assert "handleLogDeleteClick" in html
    assert '$("logTable").addEventListener("click", handleLogDeleteClick);' in html
    assert "deleteSelectedLogs" not in html
    assert 'id="deleteLogs"' not in html


def test_static_log_table_shows_planned_and_completed_windows_without_file_details() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    logs_start = html.index("function renderLogs")
    logs_end = html.index("function handleLogSelectionChange", logs_start)
    logs_block = html[logs_start:logs_end]

    assert "计划窗口数" in logs_block
    assert "实际完成窗口数" in logs_block
    assert "文件数" not in logs_block
    assert "主日志" not in logs_block
    assert "primary_log" not in logs_block
    assert "file_count" not in logs_block
    assert "row.completed_windows" in logs_block


def test_static_dashboard_renders_window_records_and_trade_window_index() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    render_start = html.index("function render(data)")
    render_end = html.index("function selectedLogStems", render_start)
    render_block = html[render_start:render_end]

    assert 'id="windowTable"' in html
    assert "窗口记录" in html
    assert "data.window_records || []" in render_block
    assert '["window_index", "窗口序号"]' in render_block
    assert '["window_date", "日期"]' in render_block
    assert '["window_start_time", "开始时间"]' in render_block
    assert '["window_end_time", "结束时间"]' in render_block
    assert '["actual_settlement_side", "实际结算方向"]' in render_block


def test_static_dashboard_paginates_window_and_trade_records_with_expected_sorting() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    render_start = html.index("function render(data)")
    render_end = html.index("function selectedLogStems", render_start)
    render_block = html[render_start:render_end]

    assert "windowPageSize: 10" in html
    assert "tradePageSize: 20" in html
    assert 'id="windowPager"' in html
    assert 'id="tradePager"' in html
    assert 'paginateRows(orderedRows(data.window_records || [], { reverse: false }), state.windowPage, state.windowPageSize)' in render_block
    assert 'paginateRows(orderedRows(data.trades || [], { reverse: false }), state.tradePage, state.tradePageSize)' in render_block
    assert '"windowTable"' in render_block
    assert '"tradeTable"' in render_block


def test_static_dashboard_renders_trade_direction_accuracy_panel() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    render_start = html.index("function render(data)")
    render_end = html.index("function selectedLogStems", render_start)
    render_block = html[render_start:render_end]

    assert "方向判断正确率" in html
    assert 'id="directionAccuracy"' in html
    assert 'data.trade_direction_accuracy' in render_block
