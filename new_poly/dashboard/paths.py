from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


LOCAL_REPO_ROOT = Path("/Users/forrestliao/workspace/new-poly")
VPS_REPO_ROOT = Path("/opt/new-poly/repo")


@dataclass(frozen=True)
class DashboardPaths:
    env: str
    repo_root: Path
    log_dir: Path
    python: Path
    allow_live_control: bool


def resolve_dashboard_paths() -> DashboardPaths:
    env = os.environ.get("NEW_POLY_ENV", "local").strip().lower()
    if env not in {"local", "vps"}:
        raise ValueError("NEW_POLY_ENV must be local or vps")

    if env == "vps":
        repo_root = Path(os.environ.get("NEW_POLY_REPO", str(VPS_REPO_ROOT)))
        log_dir = Path(os.environ.get("NEW_POLY_LOG_DIR", "/opt/new-poly/logs"))
        python = Path(os.environ.get("NEW_POLY_PYTHON", "/opt/new-poly/venv/bin/python"))
        default_live = True
    else:
        repo_root = Path(os.environ.get("NEW_POLY_REPO", str(LOCAL_REPO_ROOT)))
        log_dir = Path(os.environ.get("NEW_POLY_LOG_DIR", str(repo_root / "data" / "live_runs")))
        python = Path(os.environ.get("NEW_POLY_PYTHON", str(repo_root / ".venv" / "bin" / "python")))
        default_live = False

    allow_live = _env_bool("NEW_POLY_ALLOW_LIVE_CONTROL", default_live)
    return DashboardPaths(env=env, repo_root=repo_root, log_dir=log_dir, python=python, allow_live_control=allow_live)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}
