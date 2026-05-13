from __future__ import annotations

import datetime as dt
import re
import shutil
from pathlib import Path
from typing import Any

RUN_STEM_RE = re.compile(
    r"^(live|paper)-([a-z0-9_-]+)-([1-9][0-9]*)w-"
    r"(?P<ts>(?:\d{8}T\d{6}Z)|(?:\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_BJ))$"
)
LOG_SUFFIXES = (".jsonl", ".out", ".pid", ".yaml", ".tgz")
TRASH_DIR = ".trash"


def list_log_runs(log_dir: Path, *, mode: str = "all", running_stems: set[str] | None = None) -> dict[str, Any]:
    mode = _validate_mode_filter(mode)
    running_stems = running_stems or set()
    groups: dict[str, list[Path]] = {}
    if log_dir.exists():
        for path in log_dir.iterdir():
            if not path.is_file():
                continue
            stem = _run_stem_from_path(path)
            if stem is None:
                continue
            match = RUN_STEM_RE.match(stem)
            if match is None:
                continue
            if mode != "all" and match.group(1) != mode:
                continue
            groups.setdefault(stem, []).append(path)

    runs = [_log_run_item(stem, paths, stem in running_stems) for stem, paths in groups.items()]
    runs.sort(key=_sort_key, reverse=True)
    return {"ok": True, "mode": mode, "runs": runs}


def delete_log_runs(log_dir: Path, stems: Any, *, running_stems: set[str] | None = None) -> dict[str, Any]:
    if not isinstance(stems, list) or not stems:
        raise ValueError("stems must be a non-empty list")
    running_stems = running_stems or set()
    unique_stems = []
    for raw in stems:
        stem = str(raw or "")
        if not RUN_STEM_RE.match(stem):
            raise ValueError(f"invalid log stem: {stem}")
        if stem in running_stems:
            raise ValueError(f"cannot delete running log: {stem}")
        if stem not in unique_stems:
            unique_stems.append(stem)

    trash = log_dir / TRASH_DIR / dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    moved: list[dict[str, str]] = []
    for stem in unique_stems:
        for path in _companion_files(log_dir, stem):
            trash.mkdir(parents=True, exist_ok=True)
            target = _unique_target(trash / path.name)
            shutil.move(str(path), str(target))
            moved.append({"stem": stem, "from": str(path), "to": str(target)})
    return {"ok": True, "deleted_stems": unique_stems, "moved": moved}


def log_path_for_stem(log_dir: Path, stem: str, *, mode: str | None = None) -> Path:
    match = RUN_STEM_RE.match(stem)
    if match is None:
        raise ValueError(f"invalid log stem: {stem}")
    if mode is not None and match.group(1) != mode:
        raise ValueError(f"selected log is {match.group(1)}, not {mode}")
    path = log_dir / f"{stem}.jsonl"
    if not path.exists() or not path.is_file():
        raise ValueError(f"log not found: {stem}")
    return path


def _log_run_item(stem: str, paths: list[Path], running: bool) -> dict[str, Any]:
    match = RUN_STEM_RE.match(stem)
    if match is None:
        raise ValueError(f"invalid log stem: {stem}")
    mode = match.group(1)
    region = match.group(2)
    windows_raw = match.group(3)
    ts_raw = match.group("ts")
    files = sorted(paths, key=lambda path: path.name)
    jsonl = next((path for path in files if path.name == f"{stem}.jsonl"), None)
    first_ts, last_ts = _jsonl_first_last_ts(jsonl) if jsonl is not None else (None, None)
    filename_start = _filename_timestamp(ts_raw)
    started_at = first_ts or filename_start
    ended_at = last_ts
    duration_sec = _duration_sec(first_ts, last_ts)
    return {
        "stem": stem,
        "mode": mode,
        "region": region,
        "windows": int(windows_raw),
        "started_at": _iso(started_at),
        "started_at_text": _display_timestamp(started_at),
        "ended_at": _iso(ended_at),
        "ended_at_text": _display_timestamp(ended_at),
        "duration_sec": duration_sec,
        "duration_text": _duration_text(duration_sec),
        "file_count": len(files),
        "total_size_bytes": sum(path.stat().st_size for path in files),
        "total_size_text": _size_text(sum(path.stat().st_size for path in files)),
        "status": "running" if running else "archived",
        "files": [path.name for path in files],
        "primary_log": f"{stem}.jsonl" if jsonl is not None else files[0].name,
    }


def _sort_key(item: dict[str, Any]) -> tuple[str, str]:
    started = str(item.get("started_at") or "")
    stem = str(item.get("stem") or "")
    return (started, stem)


def _run_stem_from_path(path: Path) -> str | None:
    for suffix in LOG_SUFFIXES:
        if path.name.endswith(suffix):
            return path.name[: -len(suffix)]
    return None


def _companion_files(log_dir: Path, stem: str) -> list[Path]:
    return [path for suffix in LOG_SUFFIXES if (path := log_dir / f"{stem}{suffix}").exists() and path.is_file()]


def _jsonl_first_last_ts(path: Path | None) -> tuple[dt.datetime | None, dt.datetime | None]:
    if path is None or not path.exists():
        return None, None
    first = None
    last = None
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            ts = _extract_ts(line)
            if ts is None:
                continue
            if first is None:
                first = ts
            last = ts
    return first, last


def _extract_ts(line: str) -> dt.datetime | None:
    marker = '"ts"'
    index = line.find(marker)
    if index < 0:
        return None
    colon = line.find(":", index + len(marker))
    if colon < 0:
        return None
    first_quote = line.find('"', colon + 1)
    second_quote = line.find('"', first_quote + 1)
    if first_quote < 0 or second_quote < 0:
        return None
    return _parse_ts(line[first_quote + 1:second_quote])


def _filename_timestamp(value: str) -> dt.datetime | None:
    formats = (
        ("%Y%m%dT%H%M%SZ", dt.timezone.utc),
        ("%Y-%m-%d_%H-%M-%S_BJ", dt.timezone(dt.timedelta(hours=8))),
    )
    for fmt, timezone in formats:
        try:
            return dt.datetime.strptime(value, fmt).replace(tzinfo=timezone).astimezone(dt.timezone.utc)
        except ValueError:
            continue
    return None


def _parse_ts(value: str) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _duration_sec(start: dt.datetime | None, end: dt.datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds()))


def _duration_text(value: int | None) -> str:
    if value is None:
        return "-"
    h = value // 3600
    m = (value % 3600) // 60
    s = value % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _size_text(value: int) -> str:
    units = ("B", "KB", "MB", "GB")
    amount = float(value)
    unit = units[0]
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            break
        amount /= 1024
    return f"{amount:.1f} {unit}" if unit != "B" else f"{int(amount)} B"


def _iso(value: dt.datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _display_timestamp(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    beijing = dt.timezone(dt.timedelta(hours=8))
    return value.astimezone(beijing).strftime("%Y-%m-%d %H:%M:%S")


def _unique_target(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 1000):
        candidate = path.with_name(f"{path.stem}.{index}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise ValueError(f"too many duplicate trash files for {path.name}")


def _validate_mode_filter(mode: str) -> str:
    if mode not in {"all", "live", "paper"}:
        raise ValueError("mode must be all, live, or paper")
    return mode
