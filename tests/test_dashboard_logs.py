from __future__ import annotations

import json
from pathlib import Path

import pytest

from new_poly.dashboard.logs import delete_log_runs, list_log_runs


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n", encoding="utf-8")


def test_list_log_runs_groups_companions_and_sorts_newest_first(tmp_path: Path) -> None:
    short = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    long = tmp_path / "paper-sweden-12w-20260513T020000Z.jsonl"
    _write_jsonl(
        short,
        [
            {"ts": "2026-05-13T01:00:00+00:00", "event": "config"},
            {"ts": "2026-05-13T01:00:12+00:00", "event": "tick"},
        ],
    )
    _write_jsonl(
        long,
        [
            {"ts": "2026-05-13T02:00:00+00:00", "event": "config"},
            {"ts": "2026-05-13T02:10:00+00:00", "event": "tick"},
        ],
    )
    (tmp_path / "paper-sweden-1w-20260513T010000Z.out").write_text("out\n", encoding="utf-8")
    (tmp_path / "paper-sweden-1w-20260513T010000Z.pid").write_text("123\n", encoding="utf-8")
    (tmp_path / "unrelated.txt").write_text("ignore\n", encoding="utf-8")

    result = list_log_runs(tmp_path, mode="paper")

    assert [item["stem"] for item in result["runs"]] == [
        "paper-sweden-12w-20260513T020000Z",
        "paper-sweden-1w-20260513T010000Z",
    ]
    assert result["runs"][1]["duration_sec"] == 12
    assert result["runs"][1]["ended_at_text"] == "2026-05-13 09:00:12"
    assert result["runs"][1]["file_count"] == 3
    assert result["runs"][1]["status"] == "archived"


def test_list_log_runs_marks_running_stem(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-1w-20260513T010000Z.jsonl"
    _write_jsonl(log, [{"ts": "2026-05-13T01:00:00+00:00", "event": "config"}])

    result = list_log_runs(tmp_path, running_stems={"paper-sweden-1w-20260513T010000Z"})

    assert result["runs"][0]["status"] == "running"


def test_list_log_runs_supports_beijing_readable_filenames(tmp_path: Path) -> None:
    log = tmp_path / "paper-sweden-2w-2026-05-13_23-45-12_BJ.jsonl"
    _write_jsonl(log, [{"ts": "2026-05-13T15:45:12+00:00", "event": "config"}])

    result = list_log_runs(tmp_path, mode="paper")

    assert result["runs"][0]["stem"] == "paper-sweden-2w-2026-05-13_23-45-12_BJ"
    assert result["runs"][0]["started_at_text"] == "2026-05-13 23:45:12"


def test_delete_log_runs_moves_companions_to_trash(tmp_path: Path) -> None:
    stem = "paper-sweden-1w-20260513T010000Z"
    for suffix in (".jsonl", ".out", ".pid"):
        (tmp_path / f"{stem}{suffix}").write_text(suffix, encoding="utf-8")

    result = delete_log_runs(tmp_path, [stem])

    assert result["deleted_stems"] == [stem]
    assert not (tmp_path / f"{stem}.jsonl").exists()
    moved_names = {Path(item["to"]).name for item in result["moved"]}
    assert moved_names == {f"{stem}.jsonl", f"{stem}.out", f"{stem}.pid"}
    assert all((tmp_path / ".trash").glob(f"*/{name}") for name in moved_names)


def test_delete_log_runs_rejects_paths_and_running_stems(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="invalid log stem"):
        delete_log_runs(tmp_path, ["../paper-sweden-1w-20260513T010000Z"])

    with pytest.raises(ValueError, match="cannot delete running"):
        delete_log_runs(
            tmp_path,
            ["paper-sweden-1w-20260513T010000Z"],
            running_stems={"paper-sweden-1w-20260513T010000Z"},
        )
