"""Removed dynamic profile support.

The previous dynamic parameter system tuned the old dual-source probability
strategy. It is intentionally kept as inert compatibility types so runtime
imports stay stable while the active bot uses only ``PolySourceConfig``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SignalProfile:
    name: str


@dataclass(frozen=True)
class DynamicConfig:
    profiles: list[SignalProfile] = field(default_factory=list)
    active_profile: str = "disabled"
    check_every_windows: int = 0
    analysis_timeout_sec: float = 0.0

    def profile_names(self) -> list[str]:
        return [profile.name for profile in self.profiles]


@dataclass(frozen=True)
class DynamicState:
    active_profile: str
    pending_profile: str | None = None
    switched_at_window_id: str | None = None
    switched_at_ts: str | None = None
    failed_health_checks: int = 0
    last_check_window_id: str | None = None
    last_check_result: dict[str, Any] = field(default_factory=dict)
    switch_history: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class DynamicDecision:
    action: str
    reason: str
    active_profile: str
    selected_profile: str | None = None
    health_check: Any | None = None
    candidate_results: list[Any] = field(default_factory=list)

    def to_log_row(self, *, mode: str, window_id: str, failed_health_checks: int) -> dict[str, Any]:
        return {
            "event": "dynamic_check",
            "mode": mode,
            "window_id": window_id,
            "active_profile": self.active_profile,
            "selected_profile": self.selected_profile,
            "action": self.action,
            "reason": self.reason,
            "failed_health_checks": failed_health_checks,
        }


def save_dynamic_state(path: Path, state: DynamicState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.__dict__, indent=2, sort_keys=True), encoding="utf-8")


def analyze_dynamic_params(*_args: Any, **_kwargs: Any) -> tuple[DynamicDecision, DynamicState]:
    raise RuntimeError("dynamic parameter analysis was removed with the old dual-source strategy")
