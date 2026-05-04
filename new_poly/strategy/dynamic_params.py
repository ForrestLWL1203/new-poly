"""Window-bound dynamic parameter guardrails for the probability-edge bot."""

from __future__ import annotations

import datetime as dt
import json
import math
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Iterable

from new_poly.backtest.prob_edge_replay import BacktestConfig, run_backtest
from new_poly.strategy.prob_edge import EdgeConfig

try:
    import yaml
except ImportError:  # pragma: no cover - pyyaml is installed in normal runtimes
    yaml = None


@dataclass(frozen=True)
class SignalProfile:
    name: str
    entry_start_age_sec: float
    entry_end_age_sec: float
    early_required_edge: float
    core_required_edge: float
    max_entries_per_market: int
    min_candidate_trades: int

    def apply_to(self, edge: EdgeConfig) -> EdgeConfig:
        return replace(
            edge,
            entry_start_age_sec=float(self.entry_start_age_sec),
            entry_end_age_sec=float(self.entry_end_age_sec),
            early_required_edge=float(self.early_required_edge),
            core_required_edge=float(self.core_required_edge),
            max_entries_per_market=int(self.max_entries_per_market),
        )

    def signal_params(self) -> dict[str, Any]:
        return {
            "entry_start_age_sec": self.entry_start_age_sec,
            "entry_end_age_sec": self.entry_end_age_sec,
            "early_required_edge": self.early_required_edge,
            "core_required_edge": self.core_required_edge,
            "max_entries_per_market": self.max_entries_per_market,
        }


@dataclass(frozen=True)
class DynamicConfig:
    profiles: list[SignalProfile]
    active_profile: str = "aggressive"
    lookback_windows: int = 50
    check_every_windows: int = 5
    min_trades: int = 20
    min_win_rate: float = 0.55
    min_pnl: float = 0.0
    failure_streak_required: int = 2
    analysis_timeout_sec: float = 60.0
    slippage_ticks: float = 3.0
    min_seconds_between_switches: float = 7200.0
    pause_dynamic_after_drawdown_usd: float = -50.0
    win_rate_score_weight: float = 1.0
    drawdown_score_weight: float = 0.1

    def profile(self, name: str) -> SignalProfile:
        for profile in self.profiles:
            if profile.name == name:
                return profile
        raise ValueError(f"unknown dynamic profile: {name}")

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
class HealthCheck:
    windows: int
    closed_trades: int
    win_rate: float
    total_pnl: float
    max_drawdown: float
    healthy: bool
    reasons: list[str]

    def to_json(self) -> dict[str, Any]:
        return {
            "windows": self.windows,
            "closed_trades": self.closed_trades,
            "win_rate": round(self.win_rate, 4),
            "total_pnl": round(self.total_pnl, 6),
            "max_drawdown": round(self.max_drawdown, 6),
            "healthy": self.healthy,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class CandidateResult:
    profile: str
    closed_trades: int
    win_rate: float
    total_pnl: float
    avg_pnl_per_trade: float
    max_drawdown: float
    score: float = 0.0
    safe: bool = False
    rejection_reason: str | None = None

    def with_score(self, cfg: DynamicConfig, profile: SignalProfile) -> "CandidateResult":
        safe = True
        rejection = None
        if self.closed_trades < profile.min_candidate_trades:
            safe = False
            rejection = "insufficient_candidate_trades"
        elif self.total_pnl < cfg.min_pnl:
            safe = False
            rejection = "candidate_pnl_negative"
        elif self.win_rate < cfg.min_win_rate:
            safe = False
            rejection = "candidate_win_rate_low"
        score = (
            self.avg_pnl_per_trade * math.sqrt(max(self.closed_trades, 0))
            + self.win_rate * cfg.win_rate_score_weight
            - abs(self.max_drawdown) * cfg.drawdown_score_weight
        )
        return replace(self, score=round(score, 6), safe=safe, rejection_reason=rejection)

    def to_json(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "closed_trades": self.closed_trades,
            "win_rate": round(self.win_rate, 4),
            "total_pnl": round(self.total_pnl, 6),
            "avg_pnl_per_trade": round(self.avg_pnl_per_trade, 6),
            "max_drawdown": round(self.max_drawdown, 6),
            "score": round(self.score, 6),
            "safe": self.safe,
            "rejection_reason": self.rejection_reason,
        }


@dataclass(frozen=True)
class DynamicDecision:
    action: str
    reason: str
    active_profile: str
    selected_profile: str | None
    health_check: HealthCheck
    candidate_results: list[CandidateResult]

    def to_log_row(self, *, mode: str, window_id: str, failed_health_checks: int) -> dict[str, Any]:
        return {
            "ts": dt.datetime.now().astimezone().isoformat(),
            "event": "dynamic_check",
            "mode": mode,
            "window_id": window_id,
            "active_profile": self.active_profile,
            "selected_profile": self.selected_profile,
            "health_check": self.health_check.to_json(),
            "failed_health_checks": failed_health_checks,
            "action": self.action,
            "reason": self.reason,
            "candidate_results": [candidate.to_json() for candidate in self.candidate_results],
        }


def default_dynamic_config() -> DynamicConfig:
    return DynamicConfig(
        profiles=[
            SignalProfile("aggressive", 100, 240, 0.14, 0.12, 4, min_candidate_trades=12),
            SignalProfile("balanced", 105, 240, 0.18, 0.16, 3, min_candidate_trades=8),
            SignalProfile("conservative", 135, 240, 0.22, 0.20, 2, min_candidate_trades=4),
            SignalProfile("strict", 150, 210, 0.22, 0.20, 2, min_candidate_trades=3),
        ]
    )


def load_dynamic_config(path: Path) -> DynamicConfig:
    if not path.exists():
        raise FileNotFoundError(path)
    text = path.read_text()
    raw = yaml.safe_load(text) if yaml is not None else _parse_dynamic_yaml(text)
    raw = raw or {}
    if not isinstance(raw, dict):
        raise ValueError("dynamic config must be a mapping")
    profiles_raw = raw.get("profiles")
    if not isinstance(profiles_raw, dict) or not profiles_raw:
        raise ValueError("dynamic config requires profiles")
    profiles: list[SignalProfile] = []
    for name, profile_raw in profiles_raw.items():
        if not isinstance(profile_raw, dict):
            raise ValueError(f"profile must be a mapping: {name}")
        profile = SignalProfile(
            name=str(name),
            entry_start_age_sec=float(profile_raw["entry_start_age_sec"]),
            entry_end_age_sec=float(profile_raw["entry_end_age_sec"]),
            early_required_edge=float(profile_raw["early_required_edge"]),
            core_required_edge=float(profile_raw["core_required_edge"]),
            max_entries_per_market=int(profile_raw["max_entries_per_market"]),
            min_candidate_trades=int(profile_raw["min_candidate_trades"]),
        )
        if profile.entry_start_age_sec > profile.entry_end_age_sec:
            raise ValueError(f"profile entry_start_age_sec > entry_end_age_sec: {name}")
        profiles.append(profile)

    health = raw.get("health") if isinstance(raw.get("health"), dict) else {}
    runtime = raw.get("runtime") if isinstance(raw.get("runtime"), dict) else {}
    scoring = raw.get("scoring") if isinstance(raw.get("scoring"), dict) else {}
    live_safety = raw.get("live_safety") if isinstance(raw.get("live_safety"), dict) else {}
    return DynamicConfig(
        profiles=profiles,
        active_profile=str(raw.get("active_profile") or profiles[0].name),
        lookback_windows=int(health.get("lookback_windows", 50)),
        check_every_windows=int(health.get("check_every_windows", 5)),
        min_trades=int(health.get("min_trades", 20)),
        min_win_rate=float(health.get("min_win_rate", 0.55)),
        min_pnl=float(health.get("min_pnl", 0.0)),
        failure_streak_required=int(health.get("failure_streak_required", 2)),
        analysis_timeout_sec=float(runtime.get("analysis_timeout_sec", 60.0)),
        slippage_ticks=float(runtime.get("slippage_ticks", 3.0)),
        min_seconds_between_switches=float(live_safety.get("min_seconds_between_switches", 7200.0)),
        pause_dynamic_after_drawdown_usd=float(live_safety.get("pause_dynamic_after_drawdown_usd", -50.0)),
        win_rate_score_weight=float(scoring.get("win_rate_score_weight", 1.0)),
        drawdown_score_weight=float(scoring.get("drawdown_score_weight", 0.1)),
    )


def _parse_scalar(value: str) -> Any:
    if value.lower() in ("true", "false"):
        return value.lower() == "true"
    try:
        if any(ch in value for ch in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value.strip('"').strip("'")


def _parse_dynamic_yaml(text: str) -> dict[str, Any]:
    root: dict[str, Any] = {}
    section: str | None = None
    profile_name: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        stripped = line.strip()
        if indent == 0:
            profile_name = None
            if stripped.endswith(":"):
                section = stripped[:-1]
                root[section] = {}
                continue
            if ":" in stripped:
                key, value = stripped.split(":", 1)
                root[key.strip()] = _parse_scalar(value.strip())
            continue
        if section is None or ":" not in stripped:
            continue
        if section == "profiles":
            if indent == 2 and stripped.endswith(":"):
                profile_name = stripped[:-1]
                root["profiles"][profile_name] = {}
                continue
            if indent >= 4 and profile_name is not None:
                key, value = stripped.split(":", 1)
                root["profiles"][profile_name][key.strip()] = _parse_scalar(value.strip())
            continue
        key, value = stripped.split(":", 1)
        root[section][key.strip()] = _parse_scalar(value.strip())
    return root


def _parse_ts(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


def _window_is_complete(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        if row.get("event") == "settlement":
            return True
        age = _float(row.get("age_sec"))
        remaining = _float(row.get("remaining_sec"))
        if age is not None and age >= 299:
            return True
        if remaining is not None and remaining <= 1:
            return True
    return False


def _float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _group_by_slug(rows: Iterable[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    order: list[str] = []
    for row in rows:
        slug = str(row.get("market_slug") or "")
        if not slug:
            continue
        if slug not in groups:
            groups[slug] = []
            order.append(slug)
        groups[slug].append(row)
    return [(slug, groups[slug]) for slug in order]


def recent_completed_window_rows(rows: Iterable[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    completed = [(slug, group) for slug, group in _group_by_slug(rows) if _window_is_complete(group)]
    selected = completed[-limit:]
    result: list[dict[str, Any]] = []
    for _, group in selected:
        result.extend(group)
    return result


def summarize_actual_results(rows: Iterable[dict[str, Any]], *, min_trades: int, min_win_rate: float, min_pnl: float) -> HealthCheck:
    groups = [(slug, group) for slug, group in _group_by_slug(rows) if _window_is_complete(group)]
    pnls: list[float] = []
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for _, group in groups:
        for row in group:
            pnl = None
            if row.get("event") == "exit":
                pnl = _float(row.get("exit_pnl"))
            elif row.get("event") == "settlement":
                pnl = _float(row.get("settlement_pnl"))
            if pnl is None:
                continue
            pnls.append(pnl)
            equity += pnl
            peak = max(peak, equity)
            max_drawdown = min(max_drawdown, equity - peak)

    wins = sum(1 for pnl in pnls if pnl > 0)
    total_pnl = round(sum(pnls), 6)
    win_rate = wins / len(pnls) if pnls else 0.0
    reasons: list[str] = []
    if len(pnls) < min_trades:
        reasons.append("insufficient_trades")
    if win_rate < min_win_rate:
        reasons.append("win_rate_low")
    if total_pnl < min_pnl:
        reasons.append("pnl_negative")
    return HealthCheck(
        windows=len(groups),
        closed_trades=len(pnls),
        win_rate=round(win_rate, 4),
        total_pnl=total_pnl,
        max_drawdown=round(max_drawdown, 6),
        healthy=not reasons,
        reasons=reasons,
    )


def _profile_index(cfg: DynamicConfig, name: str) -> int:
    try:
        return cfg.profile_names().index(name)
    except ValueError:
        return 0


def _score_candidates(candidates: Iterable[CandidateResult], cfg: DynamicConfig, active_profile: str) -> list[CandidateResult]:
    active_index = _profile_index(cfg, active_profile)
    scored: list[CandidateResult] = []
    for candidate in candidates:
        try:
            profile = cfg.profile(candidate.profile)
        except ValueError:
            continue
        if _profile_index(cfg, candidate.profile) < active_index:
            scored.append(replace(candidate.with_score(cfg, profile), safe=False, rejection_reason="more_aggressive_than_active"))
            continue
        scored.append(candidate.with_score(cfg, profile))
    return scored


def _check_result_payload(health: HealthCheck, candidates: Iterable[CandidateResult], *, action: str, reason: str) -> dict[str, Any]:
    return {
        "health": health.to_json(),
        "candidate_results": [candidate.to_json() for candidate in candidates],
        "action": action,
        "reason": reason,
    }


def _live_cooldown_active(state: DynamicState, cfg: DynamicConfig, now_ts: str | None) -> bool:
    switched = _parse_ts(state.switched_at_ts)
    now = _parse_ts(now_ts) or dt.datetime.now(dt.timezone.utc)
    if switched is None:
        return False
    if switched.tzinfo is None:
        switched = switched.replace(tzinfo=dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    return (now - switched).total_seconds() < cfg.min_seconds_between_switches


def decide_dynamic_update(
    health: HealthCheck,
    candidates: Iterable[CandidateResult],
    cfg: DynamicConfig,
    state: DynamicState,
    *,
    mode: str,
    current_window_id: str,
    now_ts: str | None = None,
    realized_drawdown: float | None = None,
) -> tuple[DynamicDecision, DynamicState]:
    if health.healthy:
        action = "no_change"
        reason = "healthy"
        new_state = replace(
            state,
            failed_health_checks=0,
            pending_profile=None,
            last_check_window_id=current_window_id,
            last_check_result=_check_result_payload(health, [], action=action, reason=reason),
        )
        return DynamicDecision(action, reason, state.active_profile, None, health, []), new_state

    failed = state.failed_health_checks + 1
    if failed < cfg.failure_streak_required:
        action = "wait_for_confirmation"
        reason = "health_check_failed_once"
        new_state = replace(
            state,
            failed_health_checks=failed,
            pending_profile=None,
            last_check_window_id=current_window_id,
            last_check_result=_check_result_payload(health, [], action=action, reason=reason),
        )
        return DynamicDecision(action, reason, state.active_profile, None, health, []), new_state

    if realized_drawdown is not None and realized_drawdown <= cfg.pause_dynamic_after_drawdown_usd:
        action = "dynamic_paused"
        reason = "drawdown_pause"
        new_state = replace(state, failed_health_checks=failed, pending_profile=None, last_check_window_id=current_window_id, last_check_result=_check_result_payload(health, [], action=action, reason=reason))
        return DynamicDecision(action, reason, state.active_profile, None, health, []), new_state

    if mode == "live" and _live_cooldown_active(state, cfg, now_ts):
        action = "cooldown_active"
        reason = "live_switch_cooldown"
        new_state = replace(state, failed_health_checks=failed, pending_profile=None, last_check_window_id=current_window_id, last_check_result=_check_result_payload(health, [], action=action, reason=reason))
        return DynamicDecision(action, reason, state.active_profile, None, health, []), new_state

    scored = _score_candidates(candidates, cfg, state.active_profile)
    safe = [candidate for candidate in scored if candidate.safe]
    if not safe:
        action = "no_safe_candidate"
        reason = "no_safe_candidate"
        new_state = replace(state, failed_health_checks=failed, pending_profile=None, last_check_window_id=current_window_id, last_check_result=_check_result_payload(health, scored, action=action, reason=reason))
        return DynamicDecision(action, reason, state.active_profile, None, health, scored), new_state

    selected = sorted(safe, key=lambda item: (item.score, item.win_rate, item.total_pnl), reverse=True)[0]
    if selected.profile == state.active_profile:
        action = "keep_current"
        reason = "current_profile_best"
        new_state = replace(state, failed_health_checks=failed, pending_profile=None, last_check_window_id=current_window_id, last_check_result=_check_result_payload(health, scored, action=action, reason=reason))
        return DynamicDecision(action, reason, state.active_profile, None, health, scored), new_state

    action = "switch_pending"
    reason = "selected_safe_profile"
    new_state = replace(
        state,
        failed_health_checks=failed,
        pending_profile=selected.profile,
        last_check_window_id=current_window_id,
        last_check_result=_check_result_payload(health, scored, action=action, reason=reason),
    )
    return DynamicDecision(action, reason, state.active_profile, selected.profile, health, scored), new_state


def candidate_results_from_rows(rows: Iterable[dict[str, Any]], cfg: DynamicConfig, base: BacktestConfig) -> list[CandidateResult]:
    materialized = list(rows)
    results: list[CandidateResult] = []
    for profile in cfg.profiles:
        result = run_backtest(
            materialized,
            BacktestConfig(
                amount_usd=base.amount_usd,
                early_required_edge=profile.early_required_edge,
                core_required_edge=profile.core_required_edge,
                entry_start_age_sec=profile.entry_start_age_sec,
                entry_end_age_sec=profile.entry_end_age_sec,
                max_book_age_ms=base.max_book_age_ms,
                max_entries_per_market=profile.max_entries_per_market,
                late_entry_enabled=base.late_entry_enabled,
                tick_size=base.tick_size,
                buy_slippage_ticks=cfg.slippage_ticks,
                sell_slippage_ticks=cfg.slippage_ticks,
                sell_price_buffer_ticks=base.sell_price_buffer_ticks,
                sell_retry_price_buffer_ticks=base.sell_retry_price_buffer_ticks,
                settlement_boundary_usd=base.settlement_boundary_usd,
            ),
        )
        summary = result.summary
        results.append(
            CandidateResult(
                profile=profile.name,
                closed_trades=int(summary["closed_trades"]),
                win_rate=float(summary["win_rate"]),
                total_pnl=float(summary["total_pnl"]),
                avg_pnl_per_trade=float(summary["avg_pnl_per_trade"]),
                max_drawdown=float(summary["max_drawdown"]),
            )
        )
    return results


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            rows.append(json.loads(line))
    return rows


def load_dynamic_state(path: Path, *, default_profile: str) -> DynamicState:
    if not path.exists():
        return DynamicState(active_profile=default_profile)
    try:
        data = json.loads(path.read_text())
        if not isinstance(data, dict):
            raise ValueError("state is not an object")
        return DynamicState(
            active_profile=str(data.get("active_profile") or default_profile),
            pending_profile=data.get("pending_profile"),
            switched_at_window_id=data.get("switched_at_window_id"),
            switched_at_ts=data.get("switched_at_ts"),
            failed_health_checks=int(data.get("failed_health_checks") or 0),
            last_check_window_id=data.get("last_check_window_id"),
            last_check_result=data.get("last_check_result") if isinstance(data.get("last_check_result"), dict) else {},
            switch_history=data.get("switch_history") if isinstance(data.get("switch_history"), list) else [],
        )
    except Exception:
        return DynamicState(active_profile=default_profile)


def save_dynamic_state(path: Path, state: DynamicState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(state), ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def analyze_dynamic_params(
    jsonl_path: Path,
    cfg: DynamicConfig,
    state: DynamicState,
    base: BacktestConfig,
    *,
    mode: str,
    current_window_id: str,
    realized_drawdown: float | None = None,
) -> tuple[DynamicDecision, DynamicState]:
    rows = read_jsonl(jsonl_path)
    recent = recent_completed_window_rows(rows, limit=cfg.lookback_windows)
    if not recent:
        health = HealthCheck(0, 0, 0.0, 0.0, 0.0, False, ["insufficient_windows"])
        return DynamicDecision("insufficient_data", "insufficient_windows", state.active_profile, None, health, []), replace(state, last_check_window_id=current_window_id, last_check_result=_check_result_payload(health, [], action="insufficient_data", reason="insufficient_windows"))
    health = summarize_actual_results(recent, min_trades=cfg.min_trades, min_win_rate=cfg.min_win_rate, min_pnl=cfg.min_pnl)
    if health.windows < cfg.lookback_windows:
        health = replace(health, healthy=False, reasons=sorted(set([*health.reasons, "insufficient_windows"])))
        return DynamicDecision("insufficient_data", "insufficient_windows", state.active_profile, None, health, []), replace(state, last_check_window_id=current_window_id, last_check_result=_check_result_payload(health, [], action="insufficient_data", reason="insufficient_windows"))
    if health.healthy:
        return decide_dynamic_update(health, [], cfg, state, mode=mode, current_window_id=current_window_id, realized_drawdown=realized_drawdown)
    if state.failed_health_checks + 1 < cfg.failure_streak_required:
        return decide_dynamic_update(health, [], cfg, state, mode=mode, current_window_id=current_window_id, realized_drawdown=realized_drawdown)
    candidates = candidate_results_from_rows(recent, cfg, base)
    return decide_dynamic_update(health, candidates, cfg, state, mode=mode, current_window_id=current_window_id, realized_drawdown=realized_drawdown)
