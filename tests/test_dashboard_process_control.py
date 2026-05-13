from __future__ import annotations

import re
from pathlib import Path

from new_poly.dashboard.paths import DashboardPaths
from new_poly.dashboard.process_control import DashboardProcessController, validate_windows


def _paths(tmp_path: Path) -> DashboardPaths:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "scripts").mkdir()
    (repo / "configs").mkdir()
    (repo / "scripts" / "run_prob_edge_bot.py").write_text("print('bot')\n", encoding="utf-8")
    (repo / "configs" / "prob_poly_single_source.yaml").write_text("strategy: {}\n", encoding="utf-8")
    logs = tmp_path / "logs"
    logs.mkdir()
    return DashboardPaths(env="local", repo_root=repo, log_dir=logs, python=Path("/venv/bin/python"), allow_live_control=False)


def test_validate_windows_rejects_unsafe_values() -> None:
    assert validate_windows(12) == 12
    assert validate_windows("300") == 300
    for value in (0, -1, 10001, "12; rm -rf /", "abc"):
        try:
            validate_windows(value)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected invalid windows: {value!r}")


def test_restart_builds_whitelisted_paper_command(tmp_path: Path) -> None:
    launched = []

    controller = DashboardProcessController(
        _paths(tmp_path),
        process_lister=lambda: [],
        popen=lambda cmd, **kwargs: launched.append((cmd, kwargs)) or object(),
    )

    result = controller.restart("paper", 12)

    assert result["ok"] is True
    cmd = launched[0][0]
    assert cmd[:2] == ["/venv/bin/python", str(tmp_path / "repo" / "scripts" / "run_prob_edge_bot.py")]
    assert "--mode" in cmd and "paper" in cmd
    assert "--windows" in cmd and "12" in cmd
    assert "--analysis-logs" in cmd
    assert "--i-understand-live-risk" not in cmd
    assert result["jsonl_path"].endswith(".jsonl")
    assert re.search(r"paper-sweden-12w-\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_BJ\.jsonl$", result["jsonl_path"])


def test_restart_rejects_live_control_in_local_environment(tmp_path: Path) -> None:
    controller = DashboardProcessController(_paths(tmp_path), process_lister=lambda: [])

    result = controller.restart("live", 12)

    assert result["ok"] is False
    assert "disabled" in result["error"]


def test_stop_only_targets_matching_bot_process(tmp_path: Path) -> None:
    killed = []
    processes = [
        {"pid": 111, "cmd": "/venv/bin/python /repo/scripts/run_prob_edge_bot.py --mode paper --jsonl paper.jsonl"},
        {"pid": 222, "cmd": "python unrelated.py --mode paper"},
    ]
    controller = DashboardProcessController(
        _paths(tmp_path),
        process_lister=lambda: processes,
        terminator=lambda pid: killed.append(pid),
    )

    result = controller.stop("paper")

    assert result["ok"] is True
    assert killed == [111]
    assert result["pids"] == [111]


def test_status_is_idle_after_stop_process_disappears_without_selected_log(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    running_log = paths.log_dir / "paper-sweden-1w-20260513T020000Z.jsonl"
    running_log.write_text(
        '{"ts":"2026-05-13T02:00:00+00:00","event":"config","mode":"paper","windows":1}\n'
        '{"ts":"2026-05-13T02:00:01+00:00","event":"window_selected","mode":"paper","market_slug":"btc-updown-5m-1","window_start":"2026-05-13T02:05:00+00:00","window_end":"2026-05-13T02:10:00+00:00"}\n',
        encoding="utf-8",
    )
    process_rows = [{
        "pid": 333,
        "cmd": f"/venv/bin/python {paths.repo_root}/scripts/run_prob_edge_bot.py --mode paper --jsonl {running_log}",
    }]

    controller = DashboardProcessController(paths, process_lister=lambda: process_rows, terminator=lambda _pid: None)
    controller.stop("paper")
    process_rows.clear()

    result = controller.status("paper")

    assert result["run_status"] == "idle"
    assert result["log_path"] is None


def test_status_analyzes_selected_log_when_not_running(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    log = paths.log_dir / "paper-sweden-1w-20260513T020000Z.jsonl"
    log.write_text(
        '{"ts":"2026-05-13T02:00:00+00:00","event":"config","mode":"paper","windows":1}\n',
        encoding="utf-8",
    )
    controller = DashboardProcessController(paths, process_lister=lambda: [])

    result = controller.status("paper", log_stem="paper-sweden-1w-20260513T020000Z")

    assert result["log_path"] == str(log)


def test_status_uses_running_process_jsonl_path(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    old_log = paths.log_dir / "paper-sweden-1w-20260513T010000Z.jsonl"
    running_log = paths.log_dir / "paper-sweden-1w-20260513T020000Z.jsonl"
    old_log.write_text('{"ts":"2026-05-13T01:00:00+00:00","event":"config","mode":"paper","windows":1}\n', encoding="utf-8")
    running_log.write_text('{"ts":"2026-05-13T02:00:00+00:00","event":"config","mode":"paper","windows":1}\n', encoding="utf-8")
    processes = [{
        "pid": 333,
        "cmd": f"/venv/bin/python {paths.repo_root}/scripts/run_prob_edge_bot.py --mode paper --jsonl {running_log}",
    }]
    controller = DashboardProcessController(paths, process_lister=lambda: processes)

    result = controller.status("paper")

    assert result["pid"] == 333
    assert result["log_path"] == str(running_log)


def test_status_ignores_shell_commands_that_mention_bot_name(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    log = paths.log_dir / "paper-sweden-1w-20260513T010000Z.jsonl"
    log.write_text('{"ts":"2026-05-13T01:00:00+00:00","event":"config","mode":"paper","windows":1}\n', encoding="utf-8")
    processes = [{
        "pid": 444,
        "cmd": "zsh -lc pgrep -af 'run_prob_edge_bot.py.*--mode paper'",
    }]
    controller = DashboardProcessController(paths, process_lister=lambda: processes)

    result = controller.status("paper")

    assert result["pid"] is None
    assert result["run_status"] != "running"
