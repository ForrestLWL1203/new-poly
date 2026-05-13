"""Runtime helpers for the BTC 5m probability-edge bot."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Callable, Protocol

try:
    import yaml
except ImportError:  # pragma: no cover - pyyaml is installed on target hosts
    yaml = None

REPO_ROOT = Path(__file__).resolve().parents[1]

from new_poly.market.binance import BinancePriceFeed
from new_poly.market.binance_rv import BinanceRvSnapshot, fetch_binance_rv_snapshot
from new_poly.market.coinbase import CoinbaseBtcPriceFeed
from new_poly.market.deribit import fetch_dvol_snapshot
from new_poly.market.deribit import DvolSnapshot
from new_poly.market.prob_edge_data import (
    WindowPrices,
    effective_price,
    find_following_window,
    find_initial_window,
    lead_delta,
    price_return_bps,
    refresh_binance_open,
    refresh_coinbase_open,
    refresh_k_price,
    refresh_polymarket_open,
    side_vs_k,
    token_state,
)
from new_poly.market.polymarket_live import PolymarketChainlinkBtcPriceFeed
from new_poly.market.stream import PriceStream
from new_poly.backtest.prob_edge_replay import BacktestConfig
from new_poly.bot_log_schema import _compact
from new_poly.strategy.dynamic_params import (
    DynamicConfig,
    DynamicDecision,
    DynamicState,
    analyze_dynamic_params,
)
from new_poly.strategy.prob_edge import MarketSnapshot, StrategyDecision
from new_poly.strategy.poly_source import PolySourceConfig, evaluate_poly_exit
from new_poly.strategy.state import StrategyState
from new_poly.trading.execution import (
    BuyRetryParams,
    ExecutionConfig,
    ExecutionResult,
    SellRetryParams,
)

DEFAULT_CONFIG = REPO_ROOT / "configs" / "prob_poly_single_source.yaml"
DEFAULT_DYNAMIC_CONFIG = DEFAULT_CONFIG
DEFAULT_DYNAMIC_STATE = REPO_ROOT / "data" / "prob-edge-dynamic-state.json"


class VolatilitySnapshot(Protocol):
    source: str
    sigma: float | None
    fetched_at: float

    def to_json(self) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class RiskConfig:
    consecutive_loss_limit: int = 5
    loss_pause_windows: int = 3
    stop_on_live_insufficient_cash_balance: bool = True


@dataclass(frozen=True)
class BotConfig:
    strategy_mode: str
    poly_source: PolySourceConfig
    execution: ExecutionConfig
    risk: RiskConfig
    amount_usd: float
    interval_sec: float
    warmup_timeout_sec: float
    volatility_source: str
    rv_refresh_sec: float
    rv_lookback_minutes: int
    rv_ewma_half_life_minutes: float
    rv_floor_annual: float
    rv_cap_annual: float
    dvol_fallback_enabled: bool
    dvol_refresh_sec: float
    max_dvol_age_sec: float
    dvol_retry_interval_sec: float
    dvol_retry_attempts: int
    settlement_boundary_usd: float
    coinbase_enabled: bool = False
    polymarket_price_enabled: bool = True
    max_polymarket_price_age_sec: float = 4.0
    polymarket_stale_reconnect_sec: float = 5.0
    polymarket_unhealthy_log_after_sec: float = 10.0
    post_exit_observation_enabled: bool = False
    post_exit_observation_interval_sec: float = 10.0
    config_warnings: tuple[str, ...] = ()


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
    post_exit_observation_enabled: bool = False
    post_exit_observation_interval_sec: float = 10.0


@dataclass
class DvolRefreshState:
    current: VolatilitySnapshot | None = None
    failed_refreshes: int = 0
    last_error: str | None = None

    def apply_refresh_result(self, snapshot: VolatilitySnapshot | None) -> bool:
        if is_valid_dvol(snapshot):
            self.current = snapshot
            self.failed_refreshes = 0
            self.last_error = None
            return True
        self.failed_refreshes += 1
        self.last_error = "invalid_dvol"
        return False


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


def _float_tuple(value: Any, default: tuple[float, ...]) -> tuple[float, ...]:
    if value is None:
        return default
    if isinstance(value, (list, tuple)):
        raw_values = value
    elif isinstance(value, str):
        raw_values = [item.strip() for item in value.strip("[]").split(",") if item.strip()]
    else:
        raw_values = [value]
    try:
        parsed = tuple(float(item) for item in raw_values)
    except (TypeError, ValueError):
        return default
    return parsed or default


def load_bot_config(path: Path) -> BotConfig:
    raw = _load_yaml(path)
    strategy_mode = str(_deep_get(raw, ("strategy", "strategy_mode"), _deep_get(raw, ("strategy_mode",), "poly_single_source")))
    if strategy_mode != "poly_single_source":
        raise ValueError(f"unsupported strategy_mode: {strategy_mode}")
    poly = PolySourceConfig(
        entry_start_age_sec=float(_deep_get(raw, ("poly_source", "entry_start_age_sec"), 100.0)),
        entry_end_age_sec=float(_deep_get(raw, ("poly_source", "entry_end_age_sec"), 240.0)),
        final_no_entry_remaining_sec=float(_deep_get(raw, ("poly_source", "final_no_entry_remaining_sec"), 30.0)),
        early_to_core_age_sec=float(_deep_get(raw, ("poly_source", "early_to_core_age_sec"), 120.0)),
        core_to_late_age_sec=float(_deep_get(raw, ("poly_source", "core_to_late_age_sec"), 240.0)),
        max_entries_per_market=int(_deep_get(raw, ("poly_source", "max_entries_per_market"), 1)),
        max_book_age_ms=float(_deep_get(raw, ("poly_source", "max_book_age_ms"), 1000.0)),
        poly_reference_distance_bps=float(_deep_get(raw, ("poly_source", "poly_reference_distance_bps"), 0.5)),
        max_poly_reference_distance_bps=float(_deep_get(raw, ("poly_source", "max_poly_reference_distance_bps"), 0.0)),
        poly_trend_lookback_sec=float(_deep_get(raw, ("poly_source", "poly_trend_lookback_sec"), 3.0)),
        poly_return_bps=float(_deep_get(raw, ("poly_source", "poly_return_bps"), 0.3)),
        max_entry_ask=float(_deep_get(raw, ("poly_source", "max_entry_ask"), 0.65)),
        max_entry_fill_price=float(_deep_get(raw, ("poly_source", "max_entry_fill_price"), 0.0)),
        min_poly_entry_score=float(_deep_get(raw, ("poly_source", "min_poly_entry_score"), 0.0)),
        min_poly_hold_score=float(_deep_get(raw, ("poly_source", "min_poly_hold_score"), 0.0)),
        poly_score_component_logs=str(_deep_get(raw, ("poly_source", "poly_score_component_logs"), "compact")),
        entry_tick_size=float(_deep_get(raw, ("poly_source", "entry_tick_size"), 0.01)),
        buy_price_buffer_ticks=float(_deep_get(raw, ("poly_source", "buy_price_buffer_ticks"), 2.0)),
        reference_distance_exit_remaining_sec=_float_tuple(
            _deep_get(raw, ("poly_source", "reference_distance_exit_remaining_sec"), None),
            (120.0, 90.0, 70.0, 45.0, 30.0),
        ),
        reference_distance_exit_min_bps=_float_tuple(
            _deep_get(raw, ("poly_source", "reference_distance_exit_min_bps"), None),
            (-2.0, -1.0, 0.25, 0.75, 1.0),
        ),
        exit_min_hold_sec=float(_deep_get(raw, ("poly_source", "exit_min_hold_sec"), 3.0)),
        hold_to_settlement_enabled=bool(_deep_get(raw, ("poly_source", "hold_to_settlement_enabled"), True)),
        hold_to_settlement_min_profit_ratio=float(_deep_get(raw, ("poly_source", "hold_to_settlement_min_profit_ratio"), 0.50)),
        hold_to_settlement_min_bid_avg=float(_deep_get(raw, ("poly_source", "hold_to_settlement_min_bid_avg"), 0.80)),
        hold_to_settlement_min_bid_limit=float(_deep_get(raw, ("poly_source", "hold_to_settlement_min_bid_limit"), 0.75)),
        hold_to_settlement_min_reference_distance_bps=float(_deep_get(raw, ("poly_source", "hold_to_settlement_min_reference_distance_bps"), 1.0)),
        hold_to_settlement_min_poly_return_bps=float(_deep_get(raw, ("poly_source", "hold_to_settlement_min_poly_return_bps"), 0.0)),
    )
    execution_raw = ExecutionConfig(
        paper_latency_sec=float(_deep_get(raw, ("execution", "paper_latency_sec"), 0.0)),
        depth_notional=float(_deep_get(raw, ("execution", "depth_notional"), 5.0)),
        max_book_age_sec=float(_deep_get(raw, ("execution", "max_book_age_sec"), 1.0)),
        retry_count=int(_deep_get(raw, ("execution", "retry_count"), 1)),
        retry_interval_sec=float(_deep_get(raw, ("execution", "retry_interval_sec"), 0.0)),
        buy_price_buffer_ticks=float(_deep_get(raw, ("execution", "buy_price_buffer_ticks"), 2.0)),
        buy_retry_price_buffer_ticks=float(_deep_get(raw, ("execution", "buy_retry_price_buffer_ticks"), 4.0)),
        buy_dynamic_buffer_enabled=bool(_deep_get(raw, ("execution", "buy_dynamic_buffer_enabled"), True)),
        buy_dynamic_buffer_attempt1_max_ticks=float(_deep_get(raw, ("execution", "buy_dynamic_buffer_attempt1_max_ticks"), 5.0)),
        buy_dynamic_buffer_attempt2_max_ticks=float(_deep_get(raw, ("execution", "buy_dynamic_buffer_attempt2_max_ticks"), 8.0)),
        sell_price_buffer_ticks=float(_deep_get(raw, ("execution", "sell_price_buffer_ticks"), 5.0)),
        sell_retry_price_buffer_ticks=float(_deep_get(raw, ("execution", "sell_retry_price_buffer_ticks"), 8.0)),
        sell_dynamic_buffer_enabled=bool(_deep_get(raw, ("execution", "sell_dynamic_buffer_enabled"), True)),
        sell_profit_exit_buffer_ticks=float(_deep_get(raw, ("execution", "sell_profit_exit_buffer_ticks"), 5.0)),
        sell_profit_exit_retry_buffer_ticks=float(_deep_get(raw, ("execution", "sell_profit_exit_retry_buffer_ticks"), 8.0)),
        sell_risk_exit_buffer_ticks=float(_deep_get(raw, ("execution", "sell_risk_exit_buffer_ticks"), 8.0)),
        sell_risk_exit_retry_buffer_ticks=float(_deep_get(raw, ("execution", "sell_risk_exit_retry_buffer_ticks"), 12.0)),
        sell_force_exit_buffer_ticks=float(_deep_get(raw, ("execution", "sell_force_exit_buffer_ticks"), 10.0)),
        sell_force_exit_retry_buffer_ticks=float(_deep_get(raw, ("execution", "sell_force_exit_retry_buffer_ticks"), 15.0)),
        batch_exit_enabled=bool(_deep_get(raw, ("execution", "batch_exit_enabled"), False)),
        batch_exit_min_shares=float(_deep_get(raw, ("execution", "batch_exit_min_shares"), 20.0)),
        batch_exit_min_notional_usd=float(_deep_get(raw, ("execution", "batch_exit_min_notional_usd"), 5.0)),
        batch_exit_slices=_float_tuple(_deep_get(raw, ("execution", "batch_exit_slices"), None), (0.4, 0.3, 1.0)),
        batch_exit_extra_buffer_ticks=_float_tuple(_deep_get(raw, ("execution", "batch_exit_extra_buffer_ticks"), None), (0.0, 3.0, 6.0)),
        live_min_sell_shares=float(_deep_get(raw, ("execution", "live_min_sell_shares"), 0.01)),
        live_min_sell_notional_usd=float(_deep_get(raw, ("execution", "live_min_sell_notional_usd"), 0.0)),
    )
    execution_warnings = execution_raw.normalization_warnings()
    execution = execution_raw.normalized()
    amount_usd = float(_deep_get(raw, ("execution", "amount_usd"), 5.0))
    return BotConfig(
        strategy_mode=strategy_mode,
        poly_source=poly,
        execution=execution,
        risk=RiskConfig(
            consecutive_loss_limit=max(0, int(_deep_get(raw, ("risk", "consecutive_loss_limit"), 5))),
            loss_pause_windows=max(0, int(_deep_get(raw, ("risk", "loss_pause_windows"), 3))),
            stop_on_live_insufficient_cash_balance=bool(_deep_get(
                raw,
                ("risk", "stop_on_live_insufficient_cash_balance"),
                _deep_get(raw, ("risk", "stop_on_live_no_sellable_balance"), True),
            )),
        ),
        amount_usd=amount_usd,
        interval_sec=float(_deep_get(raw, ("runtime", "interval_sec"), 0.5)),
        warmup_timeout_sec=float(_deep_get(raw, ("runtime", "warmup_timeout_sec"), 8.0)),
        volatility_source=str(_deep_get(raw, ("runtime", "volatility_source"), "binance_rv")),
        rv_refresh_sec=float(_deep_get(raw, ("runtime", "rv_refresh_sec"), 60.0)),
        rv_lookback_minutes=max(2, int(_deep_get(raw, ("runtime", "rv_lookback_minutes"), 60))),
        rv_ewma_half_life_minutes=float(_deep_get(raw, ("runtime", "rv_ewma_half_life_minutes"), 10.0)),
        rv_floor_annual=float(_deep_get(raw, ("runtime", "rv_floor_annual"), 0.20)),
        rv_cap_annual=float(_deep_get(raw, ("runtime", "rv_cap_annual"), 2.50)),
        dvol_fallback_enabled=bool(_deep_get(raw, ("runtime", "dvol_fallback_enabled"), True)),
        dvol_refresh_sec=float(_deep_get(raw, ("runtime", "dvol_refresh_sec"), 300.0)),
        max_dvol_age_sec=float(_deep_get(raw, ("runtime", "max_dvol_age_sec"), 900.0)),
        dvol_retry_interval_sec=float(_deep_get(raw, ("runtime", "dvol_retry_interval_sec"), 5.0)),
        dvol_retry_attempts=int(_deep_get(raw, ("runtime", "dvol_retry_attempts"), 10)),
        settlement_boundary_usd=float(_deep_get(raw, ("runtime", "settlement_boundary_usd"), 5.0)),
        coinbase_enabled=bool(_deep_get(raw, ("market_data", "coinbase_enabled"), False)),
        polymarket_price_enabled=bool(_deep_get(raw, ("market_data", "polymarket_price_enabled"), True)),
        max_polymarket_price_age_sec=float(_deep_get(raw, ("market_data", "max_polymarket_price_age_sec"), 4.0)),
        polymarket_stale_reconnect_sec=float(_deep_get(raw, ("market_data", "polymarket_stale_reconnect_sec"), 5.0)),
        polymarket_unhealthy_log_after_sec=float(_deep_get(
            raw,
            ("market_data", "polymarket_unhealthy_log_after_sec"),
            10.0,
        )),
        post_exit_observation_enabled=bool(_deep_get(raw, ("runtime", "post_exit_observation_enabled"), False)),
        post_exit_observation_interval_sec=max(1.0, float(_deep_get(
            raw,
            ("runtime", "post_exit_observation_interval_sec"),
            10.0,
        ))),
        config_warnings=execution_warnings,
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
    parser.add_argument("--dynamic-params", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--dynamic-config", type=Path, default=DEFAULT_DYNAMIC_CONFIG, help=argparse.SUPPRESS)
    parser.add_argument("--dynamic-state", type=Path, default=DEFAULT_DYNAMIC_STATE, help=argparse.SUPPRESS)
    parser.add_argument("--log-retention-hours", type=float, default=24.0, help="Prune JSONL rows older than this many hours; <=0 disables pruning")
    parser.add_argument("--log-prune-every-windows", type=int, default=5, help="Run JSONL retention pruning every N completed windows")
    parser.add_argument("--post-exit-observation", dest="post_exit_observation_enabled", action="store_true", default=None)
    parser.add_argument("--no-post-exit-observation", dest="post_exit_observation_enabled", action="store_false")
    parser.add_argument("--post-exit-observation-interval-sec", type=float)
    parser.add_argument("--coinbase", dest="coinbase_enabled", action="store_true", default=None)
    parser.add_argument("--no-coinbase", dest="coinbase_enabled", action="store_false")
    parser.add_argument("--polymarket-price", dest="polymarket_price_enabled", action="store_true", default=None)
    parser.add_argument("--no-polymarket-price", dest="polymarket_price_enabled", action="store_false")
    parser.add_argument("--polymarket-stale-reconnect-sec", type=float)
    parser.add_argument("--polymarket-unhealthy-log-after-sec", type=float)
    parser.add_argument("--consecutive-loss-limit", type=int)
    parser.add_argument("--loss-pause-windows", type=int)
    parser.add_argument("--stop-on-live-insufficient-cash-balance", dest="stop_on_live_insufficient_cash_balance", action="store_true", default=None)
    parser.add_argument("--no-stop-on-live-insufficient-cash-balance", dest="stop_on_live_insufficient_cash_balance", action="store_false")
    parser.add_argument("--stop-on-live-no-sellable-balance", dest="stop_on_live_insufficient_cash_balance", action="store_true")
    parser.add_argument("--no-stop-on-live-no-sellable-balance", dest="stop_on_live_insufficient_cash_balance", action="store_false")
    return parser


def build_runtime_options(args: argparse.Namespace) -> RuntimeOptions:
    cfg = load_bot_config(args.config)
    if args.amount_usd is not None:
        amount_usd = float(args.amount_usd)
        cfg = replace(cfg, execution=replace(cfg.execution, depth_notional=amount_usd), amount_usd=amount_usd)
    if args.interval_sec is not None:
        cfg = replace(cfg, interval_sec=float(args.interval_sec))
    if args.coinbase_enabled is not None:
        cfg = replace(cfg, coinbase_enabled=bool(args.coinbase_enabled))
    if args.polymarket_price_enabled is not None:
        cfg = replace(cfg, polymarket_price_enabled=bool(args.polymarket_price_enabled))
    if args.polymarket_stale_reconnect_sec is not None:
        cfg = replace(cfg, polymarket_stale_reconnect_sec=max(1.0, float(args.polymarket_stale_reconnect_sec)))
    if args.polymarket_unhealthy_log_after_sec is not None:
        cfg = replace(cfg, polymarket_unhealthy_log_after_sec=max(0.0, float(args.polymarket_unhealthy_log_after_sec)))
    if args.consecutive_loss_limit is not None:
        cfg = replace(cfg, risk=replace(cfg.risk, consecutive_loss_limit=max(0, int(args.consecutive_loss_limit))))
    if args.loss_pause_windows is not None:
        cfg = replace(cfg, risk=replace(cfg.risk, loss_pause_windows=max(0, int(args.loss_pause_windows))))
    if args.stop_on_live_insufficient_cash_balance is not None:
        cfg = replace(cfg, risk=replace(
            cfg.risk,
            stop_on_live_insufficient_cash_balance=bool(args.stop_on_live_insufficient_cash_balance),
        ))
    if args.mode == "live" and not args.i_understand_live_risk:
        raise ValueError("live mode requires --i-understand-live-risk")
    if args.dynamic_params:
        raise ValueError("--dynamic-params was removed with the old dual-source strategy")
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
        post_exit_observation_enabled=(
            bool(args.post_exit_observation_enabled)
            if args.post_exit_observation_enabled is not None
            else bool(cfg.post_exit_observation_enabled and args.mode == "paper")
        ),
        post_exit_observation_interval_sec=max(
            1.0,
            float(
                args.post_exit_observation_interval_sec
                if args.post_exit_observation_interval_sec is not None
                else cfg.post_exit_observation_interval_sec
            ),
        ),
    )


def _config_log_row(options: RuntimeOptions) -> dict[str, Any]:
    cfg = options.config
    row = {
        "ts": dt.datetime.now().astimezone().isoformat(),
        "event": "config",
        "mode": options.mode,
        "analysis_logs": options.analysis_logs,
        "strategy_mode": cfg.strategy_mode,
        "coinbase_enabled": cfg.coinbase_enabled,
        "polymarket_price_enabled": cfg.polymarket_price_enabled,
        "max_polymarket_price_age_sec": cfg.max_polymarket_price_age_sec,
        "polymarket_stale_reconnect_sec": cfg.polymarket_stale_reconnect_sec,
        "polymarket_unhealthy_log_after_sec": cfg.polymarket_unhealthy_log_after_sec,
        "windows": options.windows,
        "once": options.once,
        "poly_source": asdict(cfg.poly_source),
        "execution": {
            **asdict(cfg.execution),
            "amount_usd": cfg.amount_usd,
        },
        "risk": asdict(cfg.risk),
        "runtime": {
            "interval_sec": cfg.interval_sec,
            "warmup_timeout_sec": cfg.warmup_timeout_sec,
            "volatility_source": cfg.volatility_source,
            "rv_refresh_sec": cfg.rv_refresh_sec,
            "rv_lookback_minutes": cfg.rv_lookback_minutes,
            "rv_ewma_half_life_minutes": cfg.rv_ewma_half_life_minutes,
            "rv_floor_annual": cfg.rv_floor_annual,
            "rv_cap_annual": cfg.rv_cap_annual,
            "dvol_fallback_enabled": cfg.dvol_fallback_enabled,
            "dvol_refresh_sec": cfg.dvol_refresh_sec,
            "max_dvol_age_sec": cfg.max_dvol_age_sec,
            "dvol_retry_interval_sec": cfg.dvol_retry_interval_sec,
            "dvol_retry_attempts": cfg.dvol_retry_attempts,
            "settlement_boundary_usd": cfg.settlement_boundary_usd,
            "post_exit_observation_enabled": options.post_exit_observation_enabled,
            "post_exit_observation_interval_sec": options.post_exit_observation_interval_sec,
        },
        "dynamic_params": {
            "enabled": options.dynamic_params,
            "config": str(options.dynamic_config) if options.dynamic_params else None,
            "state": str(options.dynamic_state) if options.dynamic_params else None,
        },
        "log_retention_hours": options.log_retention_hours,
        "log_prune_every_windows": options.log_prune_every_windows,
    }
    if cfg.config_warnings:
        row["config_warnings"] = list(cfg.config_warnings)
    return row


def _dynamic_health_payload(last_check_result: dict[str, Any]) -> dict[str, Any] | None:
    value = last_check_result.get("health")
    return value if isinstance(value, dict) else None


def _dynamic_candidate_payload(last_check_result: dict[str, Any]) -> list[Any]:
    value = last_check_result.get("candidate_results")
    return value if isinstance(value, list) else []


PRICE_RUNTIME_FIELDS = {"price_source", "s_price", "k_price", "basis_bps"}
PRICE_ANALYSIS_FIELDS = {
    "binance_price",
    "coinbase_price",
    "proxy_price",
    "binance_open_price",
    "binance_open_source",
    "coinbase_open_price",
    "coinbase_open_source",
    "polymarket_price",
    "polymarket_price_age_sec",
    "polymarket_open_price",
    "polymarket_open_source",
    "proxy_open_price",
    "source_spread_usd",
    "source_spread_bps",
    "lead_binance_vs_polymarket_usd",
    "lead_binance_vs_polymarket_bps",
    "polymarket_divergence_bps",
    "lead_coinbase_vs_polymarket_usd",
    "lead_coinbase_vs_polymarket_bps",
    "lead_proxy_vs_polymarket_usd",
    "lead_proxy_vs_polymarket_bps",
    "lead_binance_return_1s_bps",
    "lead_binance_return_3s_bps",
    "lead_binance_return_5s_bps",
    "lead_coinbase_return_1s_bps",
    "lead_coinbase_return_3s_bps",
    "lead_coinbase_return_5s_bps",
    "lead_polymarket_return_1s_bps",
    "lead_polymarket_return_3s_bps",
    "lead_polymarket_return_5s_bps",
    "lead_polymarket_return_10s_bps",
    "lead_polymarket_return_15s_bps",
    "poly_return_since_entry_start_bps",
    "lead_binance_side",
    "lead_coinbase_side",
    "lead_polymarket_side",
    "lead_proxy_side",
    "lead_binance_side_disagrees_with_polymarket",
    "lead_coinbase_side_disagrees_with_polymarket",
    "lead_proxy_side_disagrees_with_polymarket",
    "clob_ws",
}


def _runtime_log_meta(meta: dict[str, Any], *, strategy_mode: str = "prob_edge") -> dict[str, Any]:
    if strategy_mode == "poly_single_source":
        return {
            key: value
            for key, value in meta.items()
            if key not in PRICE_ANALYSIS_FIELDS
            and key not in {"price_source", "s_price", "basis_bps"}
        }
    return {key: value for key, value in meta.items() if key not in PRICE_ANALYSIS_FIELDS}


def _price_analysis(meta: dict[str, Any], *, strategy_mode: str = "prob_edge") -> dict[str, Any]:
    if strategy_mode == "poly_single_source":
        fields = (
            "k_price",
            "polymarket_price",
            "polymarket_price_age_sec",
            "polymarket_open_price",
            "polymarket_open_source",
            "lead_polymarket_return_1s_bps",
            "lead_polymarket_return_3s_bps",
            "lead_polymarket_return_5s_bps",
            "lead_polymarket_return_10s_bps",
            "lead_polymarket_return_15s_bps",
            "poly_return_since_entry_start_bps",
            "lead_polymarket_side",
        )
        row = {
            key: value
            for key in fields
            if key in meta and (value := meta.get(key)) is not None and value != "missing"
        }
        return {"strategy_price_source": "polymarket_reference", **row}

    source = str(meta.get("price_source") or "")
    base_fields = ("price_source", "s_price", "k_price", "basis_bps")
    if source.startswith("proxy_"):
        fields = base_fields + (
            "polymarket_price",
            "polymarket_price_age_sec",
            "proxy_price",
            "proxy_open_price",
            "binance_price",
            "binance_open_price",
            "binance_open_source",
            "coinbase_price",
            "coinbase_open_price",
            "coinbase_open_source",
            "source_spread_usd",
            "source_spread_bps",
            "lead_binance_vs_polymarket_usd",
            "lead_binance_vs_polymarket_bps",
            "polymarket_divergence_bps",
            "lead_binance_return_3s_bps",
            "lead_polymarket_return_3s_bps",
            "lead_polymarket_return_10s_bps",
            "lead_polymarket_return_15s_bps",
            "poly_return_since_entry_start_bps",
            "lead_binance_side",
            "lead_polymarket_side",
            "lead_binance_side_disagrees_with_polymarket",
        )
    else:
        fields = base_fields + (
            "polymarket_price",
            "polymarket_price_age_sec",
            "proxy_price",
            "binance_price",
            "coinbase_price",
        )
    return {
        key: value
        for key in fields
        if key in meta and (value := meta.get(key)) is not None and value != "missing"
    }


def _should_attach_reference_meta(
    reference_meta: dict[str, Any],
    *,
    analysis_logs: bool,
    has_position: bool,
    decision: StrategyDecision | None,
) -> bool:
    if not reference_meta:
        return False
    if analysis_logs or has_position:
        return True
    return decision is not None and decision.action == "exit"


def _reference_meta(meta: dict[str, Any], *, strategy_mode: str = "prob_edge") -> dict[str, Any]:
    if strategy_mode == "poly_single_source":
        fields = (
            "polymarket_price",
            "polymarket_price_age_sec",
            "lead_polymarket_return_1s_bps",
            "lead_polymarket_return_3s_bps",
            "lead_polymarket_return_5s_bps",
            "lead_polymarket_return_10s_bps",
            "lead_polymarket_return_15s_bps",
            "poly_return_since_entry_start_bps",
            "lead_polymarket_side",
        )
        return {
            key: value
            for key in fields
            if key in meta and (value := meta.get(key)) is not None and value != "missing"
        }

    fields = (
        "polymarket_price",
        "polymarket_price_age_sec",
        "lead_binance_vs_polymarket_usd",
        "lead_binance_vs_polymarket_bps",
        "polymarket_divergence_bps",
        "lead_binance_return_3s_bps",
        "lead_polymarket_return_3s_bps",
        "lead_polymarket_return_10s_bps",
        "lead_polymarket_return_15s_bps",
        "poly_return_since_entry_start_bps",
        "lead_binance_side",
        "lead_polymarket_side",
        "lead_binance_side_disagrees_with_polymarket",
    )
    return {
        key: value
        for key in fields
        if key in meta and (value := meta.get(key)) is not None and value != "missing"
    }


def _warmup_warning_row(*, now: dt.datetime, mode: str, market_slug: str, unhealthy_log_after_sec: float) -> dict[str, Any]:
    return {
        "ts": now.astimezone().isoformat(),
        "event": "warning",
        "mode": mode,
        "market_slug": market_slug,
        "warning": "binance_ws_warmup_no_tick",
        "message": "Binance WS warmup expired without first tick",
        "polymarket_reference_check_after_sec": float(unhealthy_log_after_sec),
    }


def _polymarket_reference_unhealthy_row(
    *,
    now: dt.datetime,
    mode: str,
    market_slug: str,
    unhealthy_for_sec: float,
    coinbase_started: bool,
) -> dict[str, Any]:
    return {
        "ts": now.astimezone().isoformat(),
        "event": "polymarket_reference_unhealthy",
        "mode": mode,
        "market_slug": market_slug,
        "trigger": "polymarket_unhealthy_for_seconds",
        "unhealthy_for_sec": _compact(unhealthy_for_sec, 3),
        "coinbase_started": bool(coinbase_started),
    }


def _polymarket_reference_recovered_row(
    *,
    now: dt.datetime,
    mode: str,
    market_slug: str,
) -> dict[str, Any]:
    return {
        "ts": now.astimezone().isoformat(),
        "event": "polymarket_reference_recovered",
        "mode": mode,
        "market_slug": market_slug,
    }


def _should_write_row(row: dict[str, Any], seen_repetitive_skips: set[tuple[str, str]], *, analysis_logs: bool = True) -> bool:
    decision = row.get("decision")
    if not isinstance(decision, dict):
        return True
    if row.get("event") != "tick":
        return True
    if row.get("mode") == "live" and not analysis_logs:
        return False
    reason = decision.get("reason")
    one_per_window_reasons = {"outside_entry_time", "max_entries", "final_no_entry"}
    one_per_window_phase_reasons = {
        "edge_too_small",
        "gap_direction_conflict",
        "model_prob_too_low",
        "reference_not_confirmed",
        "weak_sk_distance",
    }
    one_per_window_side_reasons = {
        "poly_ask_too_high",
        "poly_reference_distance_too_high",
        "poly_reference_not_confirmed",
        "poly_score_too_low",
        "poly_trend_not_confirmed",
    }
    if (
        decision.get("action") != "skip"
        or (
            reason not in one_per_window_reasons
            and reason not in one_per_window_phase_reasons
            and reason not in one_per_window_side_reasons
        )
    ):
        return True
    phase_suffix = f":{decision.get('phase')}" if reason in one_per_window_phase_reasons else ""
    side_suffix = f":{decision.get('side')}" if reason in one_per_window_side_reasons else ""
    key = (str(row.get("market_slug") or ""), f"{reason}{phase_suffix}{side_suffix}")
    if key in seen_repetitive_skips:
        return False
    seen_repetitive_skips.add(key)
    return True


async def _noop_price_update(_update) -> None:
    return None


def volatility_refresh_interval_sec(cfg: BotConfig) -> float:
    if cfg.volatility_source == "binance_rv":
        return max(1.0, cfg.rv_refresh_sec)
    return max(1.0, cfg.dvol_refresh_sec)


def make_volatility_fetcher(cfg: BotConfig) -> Callable[[], VolatilitySnapshot]:
    if cfg.volatility_source == "binance_rv":
        def fetch() -> VolatilitySnapshot:
            snapshot = fetch_binance_rv_snapshot(
                lookback_minutes=cfg.rv_lookback_minutes,
                ewma_half_life_minutes=cfg.rv_ewma_half_life_minutes,
                floor_annual=cfg.rv_floor_annual,
                cap_annual=cfg.rv_cap_annual,
            )
            if is_valid_dvol(snapshot) or not cfg.dvol_fallback_enabled:
                return snapshot
            return fetch_dvol_snapshot()
        return fetch
    return fetch_dvol_snapshot


def is_dvol_stale(volatility: VolatilitySnapshot | None, *, now_wall: float, max_age_sec: float) -> bool:
    return volatility is None or now_wall - volatility.fetched_at > max_age_sec


def is_valid_dvol(volatility: VolatilitySnapshot | None) -> bool:
    return volatility is not None and volatility.sigma is not None and volatility.sigma > 0


async def fetch_valid_dvol_with_retries(
    *,
    fetcher: Callable[[], VolatilitySnapshot] = fetch_dvol_snapshot,
    retry_interval_sec: float = 5.0,
    max_retries: int = 10,
    sleep: Callable[[float], Any] = asyncio.sleep,
    on_retry: Callable[[int, VolatilitySnapshot | None, str | None], None] | None = None,
) -> VolatilitySnapshot | None:
    retries = max(0, int(max_retries))
    for attempt in range(retries + 1):
        snapshot: VolatilitySnapshot | None = None
        error: str | None = None
        try:
            snapshot = await asyncio.to_thread(fetcher)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        if is_valid_dvol(snapshot):
            return snapshot
        if attempt >= retries:
            return None
        if on_retry is not None:
            on_retry(attempt + 1, snapshot, error)
        await sleep(max(0.0, float(retry_interval_sec)))
    return None


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
        "settlement_source": "multi_source_proxy",
        "settlement_price": latest_proxy_price,
        "settlement_uncertain": abs(latest_proxy_price - prices.k_price) < boundary_usd,
    }


def _backtest_base_config(cfg: BotConfig) -> BacktestConfig:
    return BacktestConfig(
        amount_usd=cfg.amount_usd,
        entry_start_age_sec=cfg.poly_source.entry_start_age_sec,
        entry_end_age_sec=cfg.poly_source.entry_end_age_sec,
        final_no_entry_remaining_sec=cfg.poly_source.final_no_entry_remaining_sec,
        max_book_age_ms=cfg.poly_source.max_book_age_ms,
        max_entries_per_market=cfg.poly_source.max_entries_per_market,
        buy_slippage_ticks=0.0,
        sell_slippage_ticks=0.0,
        sell_price_buffer_ticks=cfg.execution.sell_price_buffer_ticks,
        sell_retry_price_buffer_ticks=cfg.execution.sell_retry_price_buffer_ticks,
        settlement_boundary_usd=cfg.settlement_boundary_usd,
        entry_tick_size=cfg.poly_source.entry_tick_size,
        hold_to_settlement_enabled=cfg.poly_source.hold_to_settlement_enabled,
        hold_to_settlement_min_profit_ratio=cfg.poly_source.hold_to_settlement_min_profit_ratio,
        hold_to_settlement_min_bid_avg=cfg.poly_source.hold_to_settlement_min_bid_avg,
        hold_to_settlement_min_bid_limit=cfg.poly_source.hold_to_settlement_min_bid_limit,
        poly_reference_distance_bps=cfg.poly_source.poly_reference_distance_bps,
        max_poly_reference_distance_bps=cfg.poly_source.max_poly_reference_distance_bps,
        poly_trend_lookback_sec=cfg.poly_source.poly_trend_lookback_sec,
        poly_return_bps=cfg.poly_source.poly_return_bps,
        max_entry_ask=cfg.poly_source.max_entry_ask,
        max_entry_fill_price=cfg.poly_source.max_entry_fill_price,
        min_poly_entry_score=cfg.poly_source.min_poly_entry_score,
        min_poly_hold_score=cfg.poly_source.min_poly_hold_score,
        poly_score_component_logs=cfg.poly_source.poly_score_component_logs,
        poly_buy_price_buffer_ticks=cfg.poly_source.buy_price_buffer_ticks,
        reference_distance_exit_remaining_sec=cfg.poly_source.reference_distance_exit_remaining_sec,
        reference_distance_exit_min_bps=cfg.poly_source.reference_distance_exit_min_bps,
        poly_exit_min_hold_sec=cfg.poly_source.exit_min_hold_sec,
        poly_hold_to_settlement_min_reference_distance_bps=cfg.poly_source.hold_to_settlement_min_reference_distance_bps,
        poly_hold_to_settlement_min_poly_return_bps=cfg.poly_source.hold_to_settlement_min_poly_return_bps,
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


def _snapshot(
    window,
    prices: WindowPrices,
    feed: BinancePriceFeed | None,
    coinbase_feed: CoinbaseBtcPriceFeed | None,
    polymarket_feed: PolymarketChainlinkBtcPriceFeed | None,
    stream: PriceStream,
    cfg: BotConfig,
    sigma_eff: float | None,
) -> tuple[MarketSnapshot, dict[str, Any]]:
    now = dt.datetime.now(dt.timezone.utc)
    age_sec = (now - window.start_time).total_seconds()
    remaining_sec = (window.end_time - now).total_seconds()
    price = effective_price(
        feed,
        coinbase_feed,
        prices,
        coinbase_enabled=cfg.coinbase_enabled,
        polymarket_feed=polymarket_feed,
        polymarket_enabled=cfg.polymarket_price_enabled,
    )
    price_source, s_price, basis_bps = price.source, price.effective, price.basis_bps
    now_ts = now.timestamp()
    raw_binance_price = feed.latest_price if feed is not None else None
    raw_coinbase_price = coinbase_feed.latest_price if cfg.coinbase_enabled and coinbase_feed is not None else None
    raw_proxy_values = [value for value in (raw_binance_price, raw_coinbase_price) if value is not None]
    raw_proxy_price = sum(raw_proxy_values) / len(raw_proxy_values) if raw_proxy_values else None
    raw_source_spread_usd = abs(raw_binance_price - raw_coinbase_price) if raw_binance_price is not None and raw_coinbase_price is not None else None
    raw_source_spread_bps = (raw_source_spread_usd / raw_proxy_price) * 10_000.0 if raw_source_spread_usd is not None and raw_proxy_price else None
    lead_binance_usd, lead_binance_bps = lead_delta(raw_binance_price, price.polymarket)
    lead_coinbase_usd, lead_coinbase_bps = lead_delta(raw_coinbase_price, price.polymarket)
    lead_proxy_usd, lead_proxy_bps = lead_delta(raw_proxy_price, price.polymarket)
    lead_binance_side = side_vs_k(raw_binance_price, prices.k_price)
    lead_coinbase_side = side_vs_k(raw_coinbase_price, prices.k_price)
    lead_proxy_side = side_vs_k(raw_proxy_price, prices.k_price)
    lead_polymarket_side = side_vs_k(price.polymarket, prices.k_price)
    polymarket_price_is_fresh = (
        price.polymarket is not None
        and (
            price.polymarket_age_sec is None
            or price.polymarket_age_sec <= cfg.max_polymarket_price_age_sec
        )
    )
    fresh_polymarket_price = price.polymarket if polymarket_price_is_fresh else None
    poly_entry_start_age = cfg.poly_source.entry_start_age_sec
    poly_since_entry_start = (
        price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=age_sec - poly_entry_start_age)
        if age_sec > poly_entry_start_age
        else None
    )
    up = token_state(
        stream,
        window.up_token,
        cfg.amount_usd,
        top_max_age_sec=cfg.execution.max_book_age_sec,
        include_ask_safety=False,
    )
    down = token_state(
        stream,
        window.down_token,
        cfg.amount_usd,
        top_max_age_sec=cfg.execution.max_book_age_sec,
        include_ask_safety=False,
    )
    clob_ws = stream.diagnostics(reset_counts=True) if hasattr(stream, "diagnostics") else {}
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
        up_best_ask=up["ask"],
        down_best_ask=down["ask"],
        up_bid_avg=up["bid_avg"],
        down_bid_avg=down["bid_avg"],
        up_bid_limit=up["bid_limit"],
        down_bid_limit=down["bid_limit"],
        up_bid_depth_ok=bool(up["bid_depth_ok"]),
        down_bid_depth_ok=bool(down["bid_depth_ok"]),
        up_book_age_ms=up["book_age_ms"],
        down_book_age_ms=down["book_age_ms"],
        up_bid_age_ms=up.get("bid_age_ms"),
        down_bid_age_ms=down.get("bid_age_ms"),
        source_spread_bps=price.spread_bps,
        polymarket_divergence_bps=lead_proxy_bps if cfg.coinbase_enabled and lead_proxy_bps is not None else lead_binance_bps,
        polymarket_price=fresh_polymarket_price,
        polymarket_price_age_sec=price.polymarket_age_sec,
        polymarket_return_1s_bps=price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=1.0),
        polymarket_return_3s_bps=price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=3.0),
        polymarket_return_5s_bps=price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=5.0),
        polymarket_return_10s_bps=price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=10.0),
        polymarket_return_15s_bps=price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=15.0),
        poly_return_since_entry_start_bps=poly_since_entry_start,
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
        "binance_price": _compact(raw_binance_price, 2),
        "coinbase_price": _compact(raw_coinbase_price, 2),
        "polymarket_price": _compact(price.polymarket, 2),
        "polymarket_price_age_sec": _compact(price.polymarket_age_sec, 3),
        "proxy_price": _compact(price.proxy, 2),
        "polymarket_open_price": _compact(prices.polymarket_open_price, 2),
        "polymarket_open_source": prices.polymarket_open_source,
        "binance_open_price": _compact(prices.binance_open_price, 2),
        "binance_open_source": prices.binance_open_source,
        "coinbase_open_price": _compact(prices.coinbase_open_price, 2),
        "coinbase_open_source": prices.coinbase_open_source,
        "proxy_open_price": _compact(price.proxy_open, 2),
        "source_spread_usd": _compact(raw_source_spread_usd if raw_source_spread_usd is not None else price.spread_usd, 2),
        "source_spread_bps": _compact(raw_source_spread_bps if raw_source_spread_bps is not None else price.spread_bps, 3),
        "lead_binance_vs_polymarket_usd": _compact(lead_binance_usd, 2),
        "lead_binance_vs_polymarket_bps": _compact(lead_binance_bps, 3),
        "polymarket_divergence_bps": _compact(lead_proxy_bps if cfg.coinbase_enabled and lead_proxy_bps is not None else lead_binance_bps, 3),
        "lead_coinbase_vs_polymarket_usd": _compact(lead_coinbase_usd, 2),
        "lead_coinbase_vs_polymarket_bps": _compact(lead_coinbase_bps, 3),
        "lead_proxy_vs_polymarket_usd": _compact(lead_proxy_usd, 2),
        "lead_proxy_vs_polymarket_bps": _compact(lead_proxy_bps, 3),
        "lead_binance_return_1s_bps": _compact(price_return_bps(feed, now_ts=now_ts, lookback_sec=1.0), 3),
        "lead_binance_return_3s_bps": _compact(price_return_bps(feed, now_ts=now_ts, lookback_sec=3.0), 3),
        "lead_binance_return_5s_bps": _compact(price_return_bps(feed, now_ts=now_ts, lookback_sec=5.0), 3),
        "lead_coinbase_return_1s_bps": _compact(price_return_bps(coinbase_feed, now_ts=now_ts, lookback_sec=1.0), 3),
        "lead_coinbase_return_3s_bps": _compact(price_return_bps(coinbase_feed, now_ts=now_ts, lookback_sec=3.0), 3),
        "lead_coinbase_return_5s_bps": _compact(price_return_bps(coinbase_feed, now_ts=now_ts, lookback_sec=5.0), 3),
        "lead_polymarket_return_1s_bps": _compact(price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=1.0), 3),
        "lead_polymarket_return_3s_bps": _compact(price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=3.0), 3),
        "lead_polymarket_return_5s_bps": _compact(price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=5.0), 3),
        "lead_polymarket_return_10s_bps": _compact(price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=10.0), 3),
        "lead_polymarket_return_15s_bps": _compact(price_return_bps(polymarket_feed, now_ts=now_ts, lookback_sec=15.0), 3),
        "poly_return_since_entry_start_bps": _compact(poly_since_entry_start, 3),
        "lead_binance_side": lead_binance_side,
        "lead_coinbase_side": lead_coinbase_side,
        "lead_proxy_side": lead_proxy_side,
        "lead_polymarket_side": lead_polymarket_side,
        "lead_binance_side_disagrees_with_polymarket": (
            lead_binance_side != lead_polymarket_side
            if lead_binance_side is not None and lead_polymarket_side is not None
            else None
        ),
        "lead_coinbase_side_disagrees_with_polymarket": (
            lead_coinbase_side != lead_polymarket_side
            if lead_coinbase_side is not None and lead_polymarket_side is not None
            else None
        ),
        "lead_proxy_side_disagrees_with_polymarket": (
            lead_proxy_side != lead_polymarket_side
            if lead_proxy_side is not None and lead_polymarket_side is not None
            else None
        ),
        "clob_ws": clob_ws,
        "up": up,
        "down": down,
    }
    return snap, meta


async def _refresh_exit_retry_params(
    *,
    window,
    prices: WindowPrices,
    feed: BinancePriceFeed | None,
    coinbase_feed: CoinbaseBtcPriceFeed | None,
    polymarket_feed: PolymarketChainlinkBtcPriceFeed | None,
    stream: PriceStream,
    cfg: BotConfig,
    sigma_eff: float | None,
    state: StrategyState,
    position: PositionSnapshot,
    exit_reason: str | None = None,
) -> SellRetryParams | None:
    snap, _meta = _snapshot(window, prices, feed, coinbase_feed, polymarket_feed, stream, cfg, sigma_eff)
    decision = evaluate_poly_exit(snap, position, cfg.poly_source, state)
    if decision.action == "exit" and decision.limit_price is not None:
        return SellRetryParams(min_price=decision.limit_price, exit_reason=decision.reason)
    if position.token_side == "up":
        min_price = snap.up_bid_limit if snap.up_bid_depth_ok else None
    else:
        min_price = snap.down_bid_limit if snap.down_bid_depth_ok else None
    if min_price is None:
        return None
    return SellRetryParams(min_price=min_price, exit_reason=exit_reason or decision.reason)


async def _refresh_entry_retry_params(
    *,
    stream: PriceStream,
    token_id: str,
    max_price: float | None,
    cfg: BotConfig,
) -> BuyRetryParams | None:
    best_ask = stream.get_latest_best_ask(token_id, max_age_sec=cfg.execution.max_book_age_sec)
    if best_ask is None:
        return None
    if max_price is not None and best_ask > max_price:
        return None
    return BuyRetryParams(best_ask=best_ask, price_hint_base=best_ask, max_price=max_price)
