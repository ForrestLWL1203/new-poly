from __future__ import annotations

import datetime as dt
import os
import signal
import shlex
import subprocess
from pathlib import Path
from typing import Any, Callable

from new_poly.dashboard.logs import log_path_for_stem
from new_poly.dashboard.paths import DashboardPaths
from new_poly.dashboard.status import build_dashboard_status

ProcessLister = Callable[[], list[dict[str, Any]]]
Terminator = Callable[[int], None]
PopenFactory = Callable[..., Any]
BEIJING = dt.timezone(dt.timedelta(hours=8))


def validate_windows(value: Any) -> int:
    try:
        windows = int(value)
    except (TypeError, ValueError):
        raise ValueError("windows must be an integer") from None
    if windows < 1 or windows > 10000:
        raise ValueError("windows must be between 1 and 10000")
    return windows


def list_bot_processes() -> list[dict[str, Any]]:
    try:
        proc = subprocess.run(["ps", "-axo", "pid=,command="], check=True, text=True, capture_output=True)
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        pid_text, _, cmd = stripped.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        rows.append({"pid": pid, "cmd": cmd.strip()})
    return rows


class DashboardProcessController:
    def __init__(
        self,
        paths: DashboardPaths,
        *,
        process_lister: ProcessLister | None = None,
        terminator: Terminator | None = None,
        popen: PopenFactory | None = None,
    ) -> None:
        self.paths = paths
        self.process_lister = process_lister or list_bot_processes
        self.terminator = terminator or (lambda pid: os.kill(pid, signal.SIGTERM))
        self.popen = popen or subprocess.Popen
        self._stopped_logs: dict[str, Path] = {}

    def status(self, mode: str, *, log_stem: str | None = None) -> dict[str, Any]:
        matches = self._matching_processes(mode)
        pids = [item["pid"] for item in matches]
        running_log = _extract_jsonl_path(str(matches[0].get("cmd") or "")) if matches else None
        selected_log = None if matches or not log_stem else log_path_for_stem(self.paths.log_dir, log_stem, mode=mode)
        return build_dashboard_status(
            mode,
            self.paths.log_dir,
            running_pids=pids,
            log_path=running_log or selected_log,
            stopped=False,
            select_latest=False,
        )

    def stop(self, mode: str) -> dict[str, Any]:
        mode = _validate_mode(mode)
        matches = self._matching_processes(mode)
        pids: list[int] = []
        for proc in matches:
            pid = int(proc["pid"])
            jsonl_path = _extract_jsonl_path(str(proc.get("cmd") or ""))
            if jsonl_path is not None:
                self._stopped_logs[mode] = jsonl_path
            self.terminator(pid)
            pids.append(pid)
        return {"ok": True, "mode": mode, "pids": pids, "message": "stop signal sent" if pids else "no matching process"}

    def restart(self, mode: str, windows: Any) -> dict[str, Any]:
        mode = _validate_mode(mode)
        windows_int = validate_windows(windows)
        if mode == "live" and not self.paths.allow_live_control:
            return {"ok": False, "mode": mode, "error": "live control disabled in this environment"}
        running = self._matching_processes(mode)
        if running:
            return {"ok": False, "mode": mode, "error": f"{mode} process already running", "pids": [item["pid"] for item in running]}
        self._stopped_logs.pop(mode, None)

        stem = _run_stem(mode, windows_int)
        jsonl_path = self.paths.log_dir / f"{stem}.jsonl"
        out_path = self.paths.log_dir / f"{stem}.out"
        pid_path = self.paths.log_dir / f"{stem}.pid"
        self.paths.log_dir.mkdir(parents=True, exist_ok=True)

        cmd = self._bot_command(mode, windows_int, jsonl_path)
        out_handle = out_path.open("a", encoding="utf-8")
        try:
            proc = self.popen(
                cmd,
                cwd=str(self.paths.repo_root),
                stdout=out_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        finally:
            out_handle.close()
        pid = getattr(proc, "pid", None)
        if pid is not None:
            pid_path.write_text(str(pid) + "\n", encoding="utf-8")
        return {
            "ok": True,
            "mode": mode,
            "pid": pid,
            "command": cmd,
            "jsonl_path": str(jsonl_path),
            "out_path": str(out_path),
            "pid_path": str(pid_path),
        }

    def _bot_command(self, mode: str, windows: int, jsonl_path: Path) -> list[str]:
        cmd = [
            str(self.paths.python),
            str(self.paths.repo_root / "scripts" / "run_prob_edge_bot.py"),
            "--config",
            str(self.paths.repo_root / "configs" / "prob_poly_single_source.yaml"),
            "--mode",
            mode,
            "--windows",
            str(windows),
            "--jsonl",
            str(jsonl_path),
        ]
        if mode == "live":
            cmd.append("--i-understand-live-risk")
        else:
            cmd.append("--analysis-logs")
        return cmd

    def _matching_processes(self, mode: str) -> list[dict[str, Any]]:
        mode = _validate_mode(mode)
        matches: list[dict[str, Any]] = []
        for proc in self.process_lister():
            cmd = str(proc.get("cmd") or "")
            parts = _split_cmd(cmd)
            if not any(part.endswith("run_prob_edge_bot.py") for part in parts):
                continue
            if not _has_mode(parts, mode):
                continue
            matches.append(proc)
        return matches


def _run_stem(mode: str, windows: int) -> str:
    ts = dt.datetime.now(BEIJING).strftime("%Y-%m-%d_%H-%M-%S_BJ")
    return f"{mode}-sweden-{windows}w-{ts}"


def _extract_jsonl_path(cmd: str) -> Path | None:
    parts = _split_cmd(cmd)
    for index, part in enumerate(parts):
        if part == "--jsonl" and index + 1 < len(parts):
            return Path(parts[index + 1])
        if part.startswith("--jsonl="):
            return Path(part.split("=", 1)[1])
    return None


def _split_cmd(cmd: str) -> list[str]:
    try:
        return shlex.split(cmd)
    except ValueError:
        return cmd.split()


def _has_mode(parts: list[str], mode: str) -> bool:
    for index, part in enumerate(parts):
        if part == "--mode" and index + 1 < len(parts) and parts[index + 1] == mode:
            return True
        if part == f"--mode={mode}":
            return True
    return False


def _validate_mode(mode: str) -> str:
    if mode not in {"live", "paper"}:
        raise ValueError("mode must be live or paper")
    return mode
