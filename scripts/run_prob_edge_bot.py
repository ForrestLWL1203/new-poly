#!/usr/bin/env python3
"""Run the BTC 5m probability-edge strategy bot."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import sys
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - pyyaml is installed on target hosts
    yaml = None

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from new_poly.market.binance import BinancePriceFeed
from new_poly.market.deribit import fetch_dvol_snapshot
from new_poly.market.deribit import DvolSnapshot
from new_poly.market.series import MarketSeries
from new_poly.market.stream import PriceStream
from new_poly.backtest.prob_edge_replay import BacktestConfig
from new_poly.strategy.dynamic_params import (
    DynamicConfig,
    DynamicDecision,
    DynamicState,
    analyze_dynamic_params,
    load_dynamic_config,
    load_dynamic_state,
    save_dynamic_state,
)
from new_poly.strategy.prob_edge import EdgeConfig, MarketSnapshot, StrategyDecision, evaluate_entry, evaluate_exit
from new_poly.strategy.state import PositionSnapshot, StrategyState
from new_poly.trading.clob_client import prefetch_order_params
from new_poly.trading.execution import (
    BuyRetryParams,
    ExecutionConfig,
    ExecutionResult,
    LiveFakExecutionGateway,
    PaperExecutionGateway,
    SellRetryParams,
)

from scripts.collect_prob_edge_data import (
    WindowPrices,
    effective_price,
    find_following_window,
    find_initial_window,
    refresh_binance_open,
    refresh_k_price,
    token_state,
)

DEFAULT_CONFIG = REPO_ROOT / "configs" / "prob_edge_mvp.yaml"
DEFAULT_DYNAMIC_CONFIG = REPO_ROOT / "configs" / "prob_edge_dynamic.yaml"
DEFAULT_DYNAMIC_STATE = REPO_ROOT / "data" / "prob-edge-dynamic-state.json"


@dataclass(frozen=True)
class BotConfig:
    edge: EdgeConfig
    execution: ExecutionConfig
    amount_usd: float
    interval_sec: float
    warmup_timeout_sec: float
    dvol_refresh_sec: float
    max_dvol_age_sec: float
    settlement_boundary_usd: float


@dataclass(frozen=True)
class RuntimeOptions:
    mode: str
    windows: int | None
    once: bool
    jsonl: Path | None
    config: BotConfig
    live_risk_ack: bool = False
    analysis_logs: bool = False
    dynamic_params: bool = False
    dynamic_config: Path = DEFAULT_DYNAMIC_CONFIG
    dynamic_state: Path = DEFAULT_DYNAMIC_STATE
    log_retention_hours: float | None = 24.0
    log_prune_every_windows: int = 5


class JsonlLogger:
    def __init__(self, path: Path | None, *, retention_hours: float | None = 24.0) -> None:
        self.handle = None
        self.path = path
        self.retention_hours = retention_hours
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            prune_jsonl_by_retention(path, retention_hours=retention_hours)
            self.handle = path.open("a", encoding="utf-8")

    def write(self, row: dict[str, Any]) -> None:
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
        print(line, flush=True)
        if self.handle is not None:
            self.handle.write(line + "\n")
            self.handle.flush()

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()

    def prune(self) -> int:
        if self.path is None or self.retention_hours is None or self.retention_hours <= 0:
            return 0
        if self.handle is not None:
            self.handle.flush()
            self.handle.close()
            self.handle = None
        removed = prune_jsonl_by_retention(self.path, retention_hours=self.retention_hours)
        self.handle = self.path.open("a", encoding="utf-8")
        return removed


def _parse_row_ts(value: Any) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def prune_jsonl_by_retention(path: Path, *, retention_hours: float | None, now: dt.datetime | None = None) -> int:
    if retention_hours is None or retention_hours <= 0 or not path.exists():
        return 0
    now_utc = now or dt.datetime.now(dt.timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=dt.timezone.utc)
    cutoff = now_utc.astimezone(dt.timezone.utc) - dt.timedelta(hours=float(retention_hours))
    kept: list[str] = []
    removed = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            kept.append(line)
            continue
        row_ts = _parse_row_ts(row.get("ts")) if isinstance(row, dict) else None
        if row_ts is not None and row_ts < cutoff:
            removed += 1
            continue
        kept.append(line)
    if removed:
        path.write_text(("\n".join(kept) + "\n") if kept else "", encoding="utf-8")
    return removed


def _deep_get(data: dict[str, Any], path: tuple[str, ...], default: Any) -> Any:
    node: Any = data
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return default
        node = node[key]
    return node


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    text = path.read_text()
    if yaml is not None:
        data = yaml.safe_load(text) or {}
    else:
        data = _parse_simple_yaml(text)
    if not isinstance(data, dict):
        raise ValueError(f"config must be a mapping: {path}")
    return data


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    """Parse the tiny nested YAML shape used by the default config."""
    root: dict[str, Any] = {}
    section: dict[str, Any] | None = None
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line:
            continue
        if not raw_line.startswith(" ") and line.endswith(":"):
            key = line[:-1].strip()
            section = {}
            root[key] = section
            continue
        if section is None or ":" not in line:
            continue
        key, value = line.split(":", 1)
        section[key.strip()] = _parse_scalar(value.strip())
    return root


def _parse_scalar(value: str) -> Any:
    if value.lower() in ("true", "false"):
        return value.lower() == "true"
    try:
        if any(ch in value for ch in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value.strip('"').strip("'")


def load_bot_config(path: Path) -> BotConfig:
    raw = _load_yaml(path)
    edge = EdgeConfig(
        early_required_edge=float(_deep_get(raw, ("strategy", "early_required_edge"), 0.12)),
        core_required_edge=float(_deep_get(raw, ("strategy", "core_required_edge"), 0.08)),
        model_decay_buffer=float(_deep_get(raw, ("strategy", "model_decay_buffer"), 0.02)),
        overprice_buffer=float(_deep_get(raw, ("strategy", "overprice_buffer"), 0.02)),
        entry_start_age_sec=float(_deep_get(raw, ("strategy", "entry_start_age_sec"), 90.0)),
        entry_end_age_sec=float(_deep_get(raw, ("strategy", "entry_end_age_sec"), 270.0)),
        final_no_entry_remaining_sec=float(_deep_get(raw, ("strategy", "final_no_entry_remaining_sec"), 30.0)),
        max_entries_per_market=int(_deep_get(raw, ("strategy", "max_entries_per_market"), 2)),
        max_book_age_ms=float(_deep_get(raw, ("strategy", "max_book_age_ms"), 1000.0)),
        late_entry_enabled=bool(_deep_get(raw, ("strategy", "late_entry_enabled"), False)),
        late_required_edge=float(_deep_get(raw, ("strategy", "late_required_edge"), 0.10)),
        late_max_spread=float(_deep_get(raw, ("strategy", "late_max_spread"), 0.02)),
        defensive_profit_min=float(_deep_get(raw, ("strategy", "defensive_profit_min"), 0.03)),
        protection_profit_min=float(_deep_get(raw, ("strategy", "protection_profit_min"), 0.01)),
        final_hold_min_prob=float(_deep_get(raw, ("strategy", "final_hold_min_prob"), 0.98)),
        final_hold_min_bid_avg=float(_deep_get(raw, ("strategy", "final_hold_min_bid_avg"), 0.97)),
        final_hold_min_bid_limit=float(_deep_get(raw, ("strategy", "final_hold_min_bid_limit"), 0.95)),
        prob_stagnation_window_sec=float(_deep_get(raw, ("strategy", "prob_stagnation_window_sec"), 3.0)),
        prob_stagnation_epsilon=float(_deep_get(raw, ("strategy", "prob_stagnation_epsilon"), 0.002)),
        prob_drop_exit_window_sec=float(_deep_get(raw, ("strategy", "prob_drop_exit_window_sec"), 0.0)),
        prob_drop_exit_threshold=float(_deep_get(raw, ("strategy", "prob_drop_exit_threshold"), 0.0)),
        min_fair_cap_margin_ticks=float(_deep_get(raw, ("strategy", "min_fair_cap_margin_ticks"), 0.0)),
        entry_tick_size=float(_deep_get(raw, ("strategy", "entry_tick_size"), 0.01)),
        min_entry_model_prob=float(_deep_get(raw, ("strategy", "min_entry_model_prob"), 0.0)),
    )
    execution = ExecutionConfig(
        paper_latency_sec=float(_deep_get(raw, ("execution", "paper_latency_sec"), 0.4)),
        depth_notional=float(_deep_get(raw, ("execution", "depth_notional"), 5.0)),
        depth_safety_multiplier=float(_deep_get(raw, ("execution", "depth_safety_multiplier"), 1.0)),
        max_book_age_sec=float(_deep_get(raw, ("execution", "max_book_age_sec"), 1.0)),
        retry_count=int(_deep_get(raw, ("execution", "retry_count"), 1)),
        retry_interval_sec=float(_deep_get(raw, ("execution", "retry_interval_sec"), 0.05)),
        buy_price_buffer_ticks=float(_deep_get(raw, ("execution", "buy_price_buffer_ticks"), 2.0)),
        buy_retry_price_buffer_ticks=float(_deep_get(raw, ("execution", "buy_retry_price_buffer_ticks"), 4.0)),
        sell_price_buffer_ticks=float(_deep_get(raw, ("execution", "sell_price_buffer_ticks"), 3.0)),
        sell_retry_price_buffer_ticks=float(_deep_get(raw, ("execution", "sell_retry_price_buffer_ticks"), 5.0)),
    )
    amount_usd = float(_deep_get(raw, ("execution", "amount_usd"), 5.0))
    return BotConfig(
        edge=edge,
        execution=execution,
        amount_usd=amount_usd,
        interval_sec=float(_deep_get(raw, ("runtime", "interval_sec"), 1.0)),
        warmup_timeout_sec=float(_deep_get(raw, ("runtime", "warmup_timeout_sec"), 8.0)),
        dvol_refresh_sec=float(_deep_get(raw, ("runtime", "dvol_refresh_sec"), 300.0)),
        max_dvol_age_sec=float(_deep_get(raw, ("runtime", "max_dvol_age_sec"), 900.0)),
        settlement_boundary_usd=float(_deep_get(raw, ("runtime", "settlement_boundary_usd"), 5.0)),
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BTC 5m probability-edge strategy bot")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--mode", choices=("paper", "live"), default="paper")
    parser.add_argument("--i-understand-live-risk", action="store_true")
    parser.add_argument("--windows", type=int, default=None)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--jsonl", type=Path)
    parser.add_argument("--amount-usd", type=float)
    parser.add_argument("--interval-sec", type=float)
    parser.add_argument("--analysis-logs", dest="analysis_logs", action="store_true", default=None)
    parser.add_argument("--no-analysis-logs", dest="analysis_logs", action="store_false")
    parser.add_argument("--dynamic-params", action="store_true", help="Enable window-bound dynamic signal parameter updates")
    parser.add_argument("--dynamic-config", type=Path, default=DEFAULT_DYNAMIC_CONFIG)
    parser.add_argument("--dynamic-state", type=Path, default=DEFAULT_DYNAMIC_STATE)
    parser.add_argument("--log-retention-hours", type=float, default=24.0, help="Prune JSONL rows older than this many hours; <=0 disables pruning")
    parser.add_argument("--log-prune-every-windows", type=int, default=5, help="Run JSONL retention pruning every N completed windows")
    return parser


def build_runtime_options(args: argparse.Namespace) -> RuntimeOptions:
    cfg = load_bot_config(args.config)
    if args.amount_usd is not None:
        execution = ExecutionConfig(
            paper_latency_sec=cfg.execution.paper_latency_sec,
            depth_notional=float(args.amount_usd),
            depth_safety_multiplier=cfg.execution.depth_safety_multiplier,
            max_book_age_sec=cfg.execution.max_book_age_sec,
            retry_count=cfg.execution.retry_count,
            retry_interval_sec=cfg.execution.retry_interval_sec,
            buy_price_buffer_ticks=cfg.execution.buy_price_buffer_ticks,
            buy_retry_price_buffer_ticks=cfg.execution.buy_retry_price_buffer_ticks,
            sell_price_buffer_ticks=cfg.execution.sell_price_buffer_ticks,
            sell_retry_price_buffer_ticks=cfg.execution.sell_retry_price_buffer_ticks,
        )
        cfg = BotConfig(cfg.edge, execution, float(args.amount_usd), cfg.interval_sec, cfg.warmup_timeout_sec, cfg.dvol_refresh_sec, cfg.max_dvol_age_sec, cfg.settlement_boundary_usd)
    if args.interval_sec is not None:
        cfg = BotConfig(cfg.edge, cfg.execution, cfg.amount_usd, float(args.interval_sec), cfg.warmup_timeout_sec, cfg.dvol_refresh_sec, cfg.max_dvol_age_sec, cfg.settlement_boundary_usd)
    if args.mode == "live" and not args.i_understand_live_risk:
        raise ValueError("live mode requires --i-understand-live-risk")
    if args.dynamic_params and args.jsonl is None:
        raise ValueError("--dynamic-params requires --jsonl for analysis input")
    return RuntimeOptions(
        mode=args.mode,
        windows=args.windows,
        once=args.once,
        jsonl=args.jsonl,
        config=cfg,
        live_risk_ack=args.i_understand_live_risk,
        analysis_logs=(args.analysis_logs if args.analysis_logs is not None else args.mode == "paper"),
        dynamic_params=bool(args.dynamic_params),
        dynamic_config=args.dynamic_config,
        dynamic_state=args.dynamic_state,
        log_retention_hours=(float(args.log_retention_hours) if args.log_retention_hours and args.log_retention_hours > 0 else None),
        log_prune_every_windows=max(1, int(args.log_prune_every_windows)),
    )


def _compact(value: float | None, digits: int = 6) -> float | None:
    return round(float(value), digits) if value is not None else None


def _config_log_row(options: RuntimeOptions) -> dict[str, Any]:
    cfg = options.config
    return {
        "ts": dt.datetime.now().astimezone().isoformat(),
        "event": "config",
        "mode": options.mode,
        "analysis_logs": options.analysis_logs,
        "windows": options.windows,
        "once": options.once,
        "strategy": asdict(cfg.edge),
        "execution": {
            **asdict(cfg.execution),
            "amount_usd": cfg.amount_usd,
        },
        "runtime": {
            "interval_sec": cfg.interval_sec,
            "warmup_timeout_sec": cfg.warmup_timeout_sec,
            "dvol_refresh_sec": cfg.dvol_refresh_sec,
            "max_dvol_age_sec": cfg.max_dvol_age_sec,
            "settlement_boundary_usd": cfg.settlement_boundary_usd,
        },
        "dynamic_params": {
            "enabled": options.dynamic_params,
            "config": str(options.dynamic_config) if options.dynamic_params else None,
            "state": str(options.dynamic_state) if options.dynamic_params else None,
        },
        "log_retention_hours": options.log_retention_hours,
        "log_prune_every_windows": options.log_prune_every_windows,
    }


def _dynamic_health_payload(last_check_result: dict[str, Any]) -> dict[str, Any] | None:
    value = last_check_result.get("health")
    return value if isinstance(value, dict) else None


def _dynamic_candidate_payload(last_check_result: dict[str, Any]) -> list[Any]:
    value = last_check_result.get("candidate_results")
    return value if isinstance(value, list) else []


def _entry_analysis(decision: StrategyDecision, result: ExecutionResult | None = None) -> dict[str, Any]:
    fill_price = result.avg_price if result is not None and result.success else None
    row = {
        "order_intent": "entry",
        "entry_side": decision.side,
        "entry_phase": decision.phase,
        "entry_required_edge": _compact(decision.required_edge),
        "entry_model_prob": _compact(decision.model_prob),
        "entry_signal_price": _compact(decision.price),
        "entry_best_ask": _compact(decision.best_ask),
        "entry_fair_cap": _compact(decision.limit_price),
        "entry_depth_limit_price": _compact(decision.depth_limit_price),
        "entry_edge_signal": _compact(decision.edge),
        "entry_price": _compact(fill_price),
        "entry_shares": _compact(result.filled_size if result is not None and result.success else None),
        "entry_edge_at_fill": _compact(decision.model_prob - fill_price) if decision.model_prob is not None and fill_price is not None else None,
        "order_attempt": result.attempt if result is not None else None,
        "order_total_latency_ms": result.total_latency_ms if result is not None else None,
    }
    if result is not None and result.timing:
        row["order_timing"] = result.timing
    return row


def _exit_analysis(decision: StrategyDecision, result: ExecutionResult | None = None) -> dict[str, Any]:
    fill_price = result.avg_price if result is not None and result.success else None
    row = {
        "order_intent": "exit",
        "exit_side": decision.side,
        "exit_reason": decision.reason,
        "exit_model_prob": _compact(decision.model_prob),
        "exit_signal_bid_avg": _compact(decision.price),
        "exit_min_price": _compact(decision.limit_price),
        "exit_profit_per_share": _compact(decision.profit_now),
        "exit_prob_stagnant": decision.prob_stagnant,
        "exit_prob_delta_3s": _compact(decision.prob_delta_3s),
        "exit_prob_drop_delta": _compact(decision.prob_drop_delta),
        "exit_price": _compact(fill_price),
        "exit_shares": _compact(result.filled_size if result is not None and result.success else None),
        "order_attempt": result.attempt if result is not None else None,
        "order_total_latency_ms": result.total_latency_ms if result is not None else None,
    }
    if result is not None and result.timing:
        row["order_timing"] = result.timing
    return row


def _should_write_row(row: dict[str, Any], seen_repetitive_skips: set[tuple[str, str]]) -> bool:
    decision = row.get("decision")
    if not isinstance(decision, dict):
        return True
    if row.get("event") != "tick":
        return True
    reason = decision.get("reason")
    one_per_window_reasons = {"outside_entry_time", "max_entries", "final_no_entry"}
    one_per_window_phase_reasons = {"edge_too_small"}
    if decision.get("action") != "skip" or (reason not in one_per_window_reasons and reason not in one_per_window_phase_reasons):
        return True
    phase_suffix = f":{decision.get('phase')}" if reason in one_per_window_phase_reasons else ""
    key = (str(row.get("market_slug") or ""), f"{reason}{phase_suffix}")
    if key in seen_repetitive_skips:
        return False
    seen_repetitive_skips.add(key)
    return True


async def _noop_price_update(_update) -> None:
    return None


def is_dvol_stale(volatility: DvolSnapshot | None, *, now_monotonic: float, max_age_sec: float) -> bool:
    return volatility is None or now_monotonic - volatility.fetched_at > max_age_sec


def choose_settlement(
    prices: WindowPrices,
    latest_proxy_price: float | None,
    *,
    boundary_usd: float = 5.0,
) -> dict[str, Any]:
    if prices.k_price is None:
        return {"winning_side": None, "settlement_source": "missing_k", "settlement_uncertain": True}
    if latest_proxy_price is None:
        return {"winning_side": None, "settlement_source": "missing_proxy_price", "settlement_uncertain": True}
    return {
        "winning_side": "up" if latest_proxy_price > prices.k_price else "down",
        "settlement_source": "binance_proxy",
        "settlement_price": latest_proxy_price,
        "settlement_uncertain": abs(latest_proxy_price - prices.k_price) < boundary_usd,
    }


def _bot_config_with_edge(cfg: BotConfig, edge: EdgeConfig) -> BotConfig:
    return BotConfig(
        edge=edge,
        execution=cfg.execution,
        amount_usd=cfg.amount_usd,
        interval_sec=cfg.interval_sec,
        warmup_timeout_sec=cfg.warmup_timeout_sec,
        dvol_refresh_sec=cfg.dvol_refresh_sec,
        max_dvol_age_sec=cfg.max_dvol_age_sec,
        settlement_boundary_usd=cfg.settlement_boundary_usd,
    )


def _backtest_base_config(cfg: BotConfig) -> BacktestConfig:
    return BacktestConfig(
        amount_usd=cfg.amount_usd,
        early_required_edge=cfg.edge.early_required_edge,
        core_required_edge=cfg.edge.core_required_edge,
        entry_start_age_sec=cfg.edge.entry_start_age_sec,
        entry_end_age_sec=cfg.edge.entry_end_age_sec,
        max_book_age_ms=cfg.edge.max_book_age_ms,
        max_entries_per_market=cfg.edge.max_entries_per_market,
        late_entry_enabled=cfg.edge.late_entry_enabled,
        buy_slippage_ticks=0.0,
        sell_slippage_ticks=0.0,
        sell_price_buffer_ticks=cfg.execution.sell_price_buffer_ticks,
        sell_retry_price_buffer_ticks=cfg.execution.sell_retry_price_buffer_ticks,
        prob_drop_exit_window_sec=cfg.edge.prob_drop_exit_window_sec,
        prob_drop_exit_threshold=cfg.edge.prob_drop_exit_threshold,
        settlement_boundary_usd=cfg.settlement_boundary_usd,
        min_entry_model_prob=cfg.edge.min_entry_model_prob,
    )


async def _run_dynamic_analysis_task(
    *,
    jsonl_path: Path,
    dynamic_cfg: DynamicConfig,
    dynamic_state: DynamicState,
    base_config: BacktestConfig,
    mode: str,
    current_window_id: str,
    realized_drawdown: float | None,
) -> tuple[DynamicDecision, DynamicState]:
    return await asyncio.wait_for(
        asyncio.to_thread(
            analyze_dynamic_params,
            jsonl_path,
            dynamic_cfg,
            dynamic_state,
            base_config,
            mode=mode,
            current_window_id=current_window_id,
            realized_drawdown=realized_drawdown,
        ),
        timeout=dynamic_cfg.analysis_timeout_sec,
    )


def _snapshot(window, prices: WindowPrices, feed: BinancePriceFeed, stream: PriceStream, cfg: BotConfig, sigma_eff: float | None) -> tuple[MarketSnapshot, dict[str, Any]]:
    now = dt.datetime.now(dt.timezone.utc)
    age_sec = (now - window.start_time).total_seconds()
    remaining_sec = (window.end_time - now).total_seconds()
    price_source, s_price, basis_bps = effective_price(feed, prices)
    up = token_state(stream, window.up_token, cfg.amount_usd, cfg.execution.depth_safety_multiplier)
    down = token_state(stream, window.down_token, cfg.amount_usd, cfg.execution.depth_safety_multiplier)
    snap = MarketSnapshot(
        market_slug=window.slug,
        age_sec=age_sec,
        remaining_sec=remaining_sec,
        s_price=s_price,
        k_price=prices.k_price,
        sigma_eff=sigma_eff,
        up_ask_avg=up["ask_avg"],
        down_ask_avg=down["ask_avg"],
        up_ask_limit=up["ask_limit"],
        down_ask_limit=down["ask_limit"],
        up_ask_safety_limit=up["ask_safety_limit"],
        down_ask_safety_limit=down["ask_safety_limit"],
        up_best_ask=up["ask"],
        down_best_ask=down["ask"],
        up_bid_avg=up["bid_avg"],
        down_bid_avg=down["bid_avg"],
        up_bid_limit=up["bid_limit"],
        down_bid_limit=down["bid_limit"],
        up_ask_depth_ok=bool(up["ask_depth_ok"]),
        down_ask_depth_ok=bool(down["ask_depth_ok"]),
        up_bid_depth_ok=bool(up["bid_depth_ok"]),
        down_bid_depth_ok=bool(down["bid_depth_ok"]),
        up_book_age_ms=up["book_age_ms"],
        down_book_age_ms=down["book_age_ms"],
    )
    meta = {
        "ts": now.astimezone().isoformat(),
        "market_slug": window.slug,
        "window_start": window.start_time.isoformat(),
        "window_end": window.end_time.isoformat(),
        "age_sec": int(round(age_sec)),
        "remaining_sec": int(round(remaining_sec)),
        "price_source": price_source,
        "s_price": _compact(s_price, 2),
        "k_price": _compact(prices.k_price, 2),
        "basis_bps": _compact(basis_bps, 3),
        "binance_open_price": _compact(prices.binance_open_price, 2),
        "binance_open_source": prices.binance_open_source,
        "up": up,
        "down": down,
    }
    return snap, meta


async def _refresh_entry_retry_params(
    *,
    window,
    prices: WindowPrices,
    feed: BinancePriceFeed,
    stream: PriceStream,
    cfg: BotConfig,
    sigma_eff: float | None,
    state: StrategyState,
    original_side: str | None,
) -> BuyRetryParams | None:
    snap, _meta = _snapshot(window, prices, feed, stream, cfg, sigma_eff)
    decision = evaluate_entry(snap, state, cfg.edge)
    if decision.action != "enter" or decision.side != original_side:
        return None
    return BuyRetryParams(
        max_price=decision.limit_price,
        best_ask=decision.best_ask,
        price_hint_base=decision.depth_limit_price,
    )


async def _refresh_exit_retry_params(
    *,
    window,
    prices: WindowPrices,
    feed: BinancePriceFeed,
    stream: PriceStream,
    cfg: BotConfig,
    sigma_eff: float | None,
    state: StrategyState,
    position: PositionSnapshot,
) -> SellRetryParams | None:
    snap, _meta = _snapshot(window, prices, feed, stream, cfg, sigma_eff)
    decision = evaluate_exit(snap, position, cfg.edge, state)
    if decision.action != "exit" or decision.limit_price is None:
        return None
    return SellRetryParams(min_price=decision.limit_price, exit_reason=decision.reason)


async def run(options: RuntimeOptions) -> int:
    cfg = options.config
    logger = JsonlLogger(options.jsonl, retention_hours=options.log_retention_hours)
    dynamic_cfg: DynamicConfig | None = None
    dynamic_state: DynamicState | None = None
    dynamic_task: asyncio.Task[tuple[DynamicDecision, DynamicState]] | None = None
    dynamic_start_error: str | None = None
    if options.dynamic_params:
        try:
            dynamic_cfg = load_dynamic_config(options.dynamic_config)
            dynamic_state = load_dynamic_state(options.dynamic_state, default_profile=dynamic_cfg.active_profile)
            if dynamic_state.active_profile not in dynamic_cfg.profile_names():
                dynamic_state = replace(dynamic_state, active_profile=dynamic_cfg.active_profile, pending_profile=None)
            cfg = _bot_config_with_edge(cfg, dynamic_cfg.profile(dynamic_state.active_profile).apply_to(cfg.edge))
            options = replace(options, config=cfg)
            save_dynamic_state(options.dynamic_state, dynamic_state)
        except Exception as exc:
            dynamic_cfg = None
            dynamic_state = None
            dynamic_start_error = str(exc)
    feed = BinancePriceFeed("btcusdt")
    series = MarketSeries.from_known("btc-updown-5m")
    stream = PriceStream(on_price=_noop_price_update)
    volatility: DvolSnapshot | None = None
    try:
        volatility = await asyncio.to_thread(fetch_dvol_snapshot)
    except Exception:
        volatility = None
    next_dvol_refresh = time.monotonic() + cfg.dvol_refresh_sec
    state = StrategyState()
    completed_windows = 0
    seen_repetitive_skips: set[tuple[str, str]] = set()

    gateway = (
        LiveFakExecutionGateway(
            live_risk_ack=options.live_risk_ack,
            retry_count=cfg.execution.retry_count,
            retry_interval_sec=cfg.execution.retry_interval_sec,
            buy_price_buffer_ticks=cfg.execution.buy_price_buffer_ticks,
            buy_retry_price_buffer_ticks=cfg.execution.buy_retry_price_buffer_ticks,
            sell_price_buffer_ticks=cfg.execution.sell_price_buffer_ticks,
            sell_retry_price_buffer_ticks=cfg.execution.sell_retry_price_buffer_ticks,
        )
        if options.mode == "live"
        else PaperExecutionGateway(stream=stream, config=cfg.execution)
    )

    try:
        if options.analysis_logs:
            logger.write(_config_log_row(options))
        if dynamic_start_error is not None:
            logger.write({
                "ts": dt.datetime.now().astimezone().isoformat(),
                "event": "dynamic_error",
                "mode": options.mode,
                "error_type": "startup",
                "message": dynamic_start_error,
                "action": "keep_current",
            })
        window = find_initial_window(series)
        prices = WindowPrices()
        state.reset_for_market(window.slug)
        await feed.start()
        await stream.connect([window.up_token, window.down_token])
        if options.mode == "live":
            await asyncio.to_thread(prefetch_order_params, window.up_token)
            await asyncio.to_thread(prefetch_order_params, window.down_token)
        warmup_deadline = time.monotonic() + max(0.0, cfg.warmup_timeout_sec)
        while time.monotonic() < warmup_deadline and feed.latest_price is None:
            await asyncio.sleep(0.1)

        while True:
            if dynamic_task is not None and dynamic_task.done():
                try:
                    decision, dynamic_state = dynamic_task.result()
                    if dynamic_state is not None:
                        save_dynamic_state(options.dynamic_state, dynamic_state)
                    logger.write(decision.to_log_row(
                        mode=options.mode,
                        window_id=window.slug,
                        failed_health_checks=dynamic_state.failed_health_checks if dynamic_state is not None else 0,
                    ))
                except Exception as exc:
                    logger.write({
                        "ts": dt.datetime.now().astimezone().isoformat(),
                        "event": "dynamic_error",
                        "mode": options.mode,
                        "market_slug": window.slug,
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                        "action": "keep_current",
                    })
                finally:
                    dynamic_task = None
            now = dt.datetime.now(dt.timezone.utc)
            age_sec = (now - window.start_time).total_seconds()
            await refresh_k_price(window, prices, age_sec)
            await refresh_binance_open(feed, window, prices, age_sec)
            if time.monotonic() >= next_dvol_refresh:
                try:
                    volatility = await asyncio.to_thread(fetch_dvol_snapshot)
                except Exception:
                    pass
                next_dvol_refresh = time.monotonic() + cfg.dvol_refresh_sec
            dvol_stale = is_dvol_stale(volatility, now_monotonic=time.monotonic(), max_age_sec=cfg.max_dvol_age_sec)
            sigma_eff = None if dvol_stale or volatility is None else volatility.sigma
            snap, meta = _snapshot(window, prices, feed, stream, cfg, sigma_eff)

            row: dict[str, Any] = {
                **meta,
                "mode": options.mode,
                "event": "tick",
                "sigma_source": volatility.source if volatility is not None else "missing",
                "sigma_eff": _compact(sigma_eff),
                "volatility": volatility.to_json() if volatility is not None else None,
                "volatility_stale": dvol_stale,
                "position": state.open_position.__dict__ if state.open_position else None,
                "realized_pnl": _compact(state.realized_pnl, 4),
            }

            if state.has_position and state.open_position is not None:
                decision = evaluate_exit(snap, state.open_position, cfg.edge, state)
                row["decision"] = decision.__dict__
                if decision.model_prob is not None:
                    state.record_model_prob(
                        snap.age_sec,
                        decision.model_prob,
                        retention_sec=max(cfg.edge.prob_stagnation_window_sec, cfg.edge.prob_drop_exit_window_sec, 5.0),
                    )
                if decision.action == "exit":
                    exiting_position = state.open_position
                    result = await gateway.sell(
                        state.open_position.token_id,
                        state.open_position.filled_shares,
                        min_price=decision.limit_price,
                        exit_reason=decision.reason,
                        retry_refresh=lambda attempt, position=exiting_position: _refresh_exit_retry_params(
                            window=window,
                            prices=prices,
                            feed=feed,
                            stream=stream,
                            cfg=cfg,
                            sigma_eff=sigma_eff,
                            state=state,
                            position=position,
                        ),
                    )
                    row["order"] = result.__dict__
                    if options.analysis_logs:
                        row["analysis"] = _exit_analysis(decision, result)
                    if result.success:
                        pnl = state.record_exit(result.avg_price, decision.reason)
                        row["event"] = "exit"
                        row["exit_reason"] = decision.reason
                        row["exit_price"] = _compact(result.avg_price)
                        row["exit_shares"] = _compact(result.filled_size)
                        row["exit_pnl"] = _compact(pnl, 4)
                        if options.analysis_logs:
                            row["position_before_exit"] = exiting_position.__dict__
                    else:
                        row["event"] = "order_no_fill"
                        row["order_intent"] = "exit"
            else:
                decision = evaluate_entry(snap, state, cfg.edge)
                row["decision"] = decision.__dict__
                if decision.action == "enter":
                    token_id = window.up_token if decision.side == "up" else window.down_token
                    result = await gateway.buy(
                        token_id,
                        cfg.amount_usd,
                        max_price=decision.limit_price,
                        best_ask=decision.best_ask,
                        price_hint_base=decision.depth_limit_price,
                        retry_refresh=lambda attempt, side=decision.side: _refresh_entry_retry_params(
                            window=window,
                            prices=prices,
                            feed=feed,
                            stream=stream,
                            cfg=cfg,
                            sigma_eff=sigma_eff,
                            state=state,
                            original_side=side,
                        ),
                    )
                    row["order"] = result.__dict__
                    if options.analysis_logs:
                        row["analysis"] = _entry_analysis(decision, result)
                    if result.success and decision.side is not None and decision.model_prob is not None and decision.edge is not None:
                        state.record_entry(PositionSnapshot(
                            market_slug=window.slug,
                            token_side=decision.side,
                            token_id=token_id,
                            entry_time=time.time(),
                            entry_avg_price=result.avg_price,
                            filled_shares=result.filled_size,
                            entry_model_prob=decision.model_prob,
                            entry_edge=decision.edge,
                        ))
                        row["event"] = "entry"
                        row["entry_side"] = decision.side
                        row["entry_price"] = _compact(result.avg_price)
                        row["entry_shares"] = _compact(result.filled_size)
                        if options.analysis_logs and state.open_position is not None:
                            row["position_after_entry"] = state.open_position.__dict__
                    else:
                        row["event"] = "order_no_fill"
                        row["order_intent"] = "entry"

            if _should_write_row(row, seen_repetitive_skips):
                logger.write(row)
            if options.once:
                return 0
            await asyncio.sleep(cfg.interval_sec)
            if dt.datetime.now(dt.timezone.utc) >= window.end_time:
                if state.has_position and state.open_position is not None:
                    settlement = choose_settlement(prices, feed.latest_price, boundary_usd=cfg.settlement_boundary_usd)
                    settled_position = state.open_position
                    if settlement["winning_side"] is not None:
                        pnl = state.record_settlement(settlement["winning_side"])
                    else:
                        pnl = state.record_exit(settled_position.entry_avg_price, "unsettled_missing_price")
                    logger.write({
                        "ts": dt.datetime.now().astimezone().isoformat(),
                        "mode": options.mode,
                        "event": "settlement",
                        "market_slug": window.slug,
                        **settlement,
                        "settlement_price": _compact(settlement.get("settlement_price"), 2),
                        "settlement_proxy_price": _compact(feed.latest_price, 2),
                        "k_price": _compact(prices.k_price, 2),
                        "position": settled_position.__dict__,
                        "settlement_pnl": _compact(pnl, 4),
                        "realized_pnl": _compact(state.realized_pnl, 4),
                    })
                if prices.k_price is not None:
                    completed_windows += 1
                if completed_windows > 0 and completed_windows % options.log_prune_every_windows == 0:
                    removed_log_rows = logger.prune()
                    if removed_log_rows:
                        logger.write({
                            "ts": dt.datetime.now().astimezone().isoformat(),
                            "event": "log_retention",
                            "mode": options.mode,
                            "retention_hours": options.log_retention_hours,
                            "prune_every_windows": options.log_prune_every_windows,
                            "removed_rows": removed_log_rows,
                        })
                if (
                    dynamic_cfg is not None
                    and dynamic_state is not None
                    and options.jsonl is not None
                    and completed_windows > 0
                    and completed_windows % dynamic_cfg.check_every_windows == 0
                    and dynamic_task is None
                ):
                    dynamic_task = asyncio.create_task(_run_dynamic_analysis_task(
                        jsonl_path=options.jsonl,
                        dynamic_cfg=dynamic_cfg,
                        dynamic_state=dynamic_state,
                        base_config=_backtest_base_config(cfg),
                        mode=options.mode,
                        current_window_id=window.slug,
                        realized_drawdown=state.drawdown,
                    ))
                elif dynamic_cfg is not None and dynamic_state is not None and options.jsonl is None and completed_windows > 0 and completed_windows % dynamic_cfg.check_every_windows == 0:
                    logger.write({
                        "ts": dt.datetime.now().astimezone().isoformat(),
                        "event": "dynamic_error",
                        "mode": options.mode,
                        "market_slug": window.slug,
                        "error_type": "missing_jsonl",
                        "message": "--dynamic-params requires --jsonl for analysis",
                        "action": "keep_current",
                    })
                if options.windows is not None and completed_windows >= options.windows:
                    return 0
                next_window = find_following_window(window, series)
                if dynamic_cfg is not None and dynamic_state is not None and dynamic_state.pending_profile is not None:
                    try:
                        old_profile = dynamic_state.active_profile
                        old_edge = cfg.edge
                        profile = dynamic_cfg.profile(dynamic_state.pending_profile)
                        cfg = _bot_config_with_edge(cfg, profile.apply_to(cfg.edge))
                        now_ts = dt.datetime.now(dt.timezone.utc).astimezone().isoformat()
                        history = list(dynamic_state.switch_history)
                        history.append({
                            "from_profile": old_profile,
                            "to_profile": profile.name,
                            "applied_at_window": next_window.slug,
                            "switched_at_ts": now_ts,
                            "health_check": dynamic_state.last_check_result,
                        })
                        dynamic_state = replace(
                            dynamic_state,
                            active_profile=profile.name,
                            pending_profile=None,
                            switched_at_window_id=next_window.slug,
                            switched_at_ts=now_ts,
                            switch_history=history,
                        )
                        save_dynamic_state(options.dynamic_state, dynamic_state)
                        logger.write({
                            "ts": now_ts,
                            "event": "config_update",
                            "mode": options.mode,
                            "from_profile": old_profile,
                            "to_profile": profile.name,
                            "applied_at_window": next_window.slug,
                            "reason": "dynamic_params",
                            "health_check": _dynamic_health_payload(dynamic_state.last_check_result),
                            "candidate_results": _dynamic_candidate_payload(dynamic_state.last_check_result),
                            "old_signal_params": {
                                "entry_start_age_sec": old_edge.entry_start_age_sec,
                                "entry_end_age_sec": old_edge.entry_end_age_sec,
                                "early_required_edge": old_edge.early_required_edge,
                                "core_required_edge": old_edge.core_required_edge,
                                "max_entries_per_market": old_edge.max_entries_per_market,
                            },
                            "new_signal_params": profile.signal_params(),
                        })
                    except Exception as exc:
                        logger.write({
                            "ts": dt.datetime.now().astimezone().isoformat(),
                            "event": "dynamic_error",
                            "mode": options.mode,
                            "market_slug": next_window.slug,
                            "error_type": type(exc).__name__,
                            "message": str(exc),
                            "action": "keep_current",
                        })
                window = next_window
                prices = WindowPrices()
                state.reset_for_market(window.slug)
                await asyncio.wait_for(stream.switch_tokens([window.up_token, window.down_token]), timeout=8.0)
                if options.mode == "live":
                    await asyncio.to_thread(prefetch_order_params, window.up_token)
                    await asyncio.to_thread(prefetch_order_params, window.down_token)
    except Exception as exc:
        logger.write({"ts": dt.datetime.now().astimezone().isoformat(), "event": "error", "error": str(exc)})
        return 1
    finally:
        for closer in (stream.close(), feed.stop()):
            try:
                await asyncio.wait_for(closer, timeout=5.0)
            except Exception:
                pass
        logger.close()


def main() -> int:
    try:
        options = build_runtime_options(build_arg_parser().parse_args())
    except Exception as exc:
        print(json.dumps({"event": "error", "error": str(exc)}, separators=(",", ":")), file=sys.stderr)
        return 2
    return asyncio.run(run(options))


if __name__ == "__main__":
    raise SystemExit(main())
