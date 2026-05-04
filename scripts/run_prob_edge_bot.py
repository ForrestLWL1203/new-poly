#!/usr/bin/env python3
"""Run the BTC 5m probability-edge strategy bot."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import sys
import time
from dataclasses import dataclass
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
from new_poly.strategy.prob_edge import EdgeConfig, MarketSnapshot, evaluate_entry, evaluate_exit
from new_poly.strategy.state import PositionSnapshot, StrategyState
from new_poly.trading.clob_client import prefetch_order_params
from new_poly.trading.execution import ExecutionConfig, LiveFakExecutionGateway, PaperExecutionGateway

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


@dataclass(frozen=True)
class BotConfig:
    edge: EdgeConfig
    execution: ExecutionConfig
    amount_usd: float
    interval_sec: float
    warmup_timeout_sec: float
    paired_buffer: float
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


class JsonlLogger:
    def __init__(self, path: Path | None) -> None:
        self.handle = None
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
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
    )
    execution = ExecutionConfig(
        paper_latency_sec=float(_deep_get(raw, ("execution", "paper_latency_sec"), 0.4)),
        depth_notional=float(_deep_get(raw, ("execution", "depth_notional"), 5.0)),
        max_book_age_sec=float(_deep_get(raw, ("execution", "max_book_age_sec"), 1.0)),
        retry_count=int(_deep_get(raw, ("execution", "retry_count"), 1)),
        retry_interval_sec=float(_deep_get(raw, ("execution", "retry_interval_sec"), 0.2)),
    )
    amount_usd = float(_deep_get(raw, ("execution", "amount_usd"), 5.0))
    return BotConfig(
        edge=edge,
        execution=execution,
        amount_usd=amount_usd,
        interval_sec=float(_deep_get(raw, ("runtime", "interval_sec"), 1.0)),
        warmup_timeout_sec=float(_deep_get(raw, ("runtime", "warmup_timeout_sec"), 8.0)),
        paired_buffer=float(_deep_get(raw, ("runtime", "paired_buffer"), 0.01)),
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
    return parser


def build_runtime_options(args: argparse.Namespace) -> RuntimeOptions:
    cfg = load_bot_config(args.config)
    if args.amount_usd is not None:
        execution = ExecutionConfig(
            paper_latency_sec=cfg.execution.paper_latency_sec,
            depth_notional=float(args.amount_usd),
            max_book_age_sec=cfg.execution.max_book_age_sec,
            retry_count=cfg.execution.retry_count,
            retry_interval_sec=cfg.execution.retry_interval_sec,
        )
        cfg = BotConfig(cfg.edge, execution, float(args.amount_usd), cfg.interval_sec, cfg.warmup_timeout_sec, cfg.paired_buffer, cfg.dvol_refresh_sec, cfg.max_dvol_age_sec, cfg.settlement_boundary_usd)
    if args.interval_sec is not None:
        cfg = BotConfig(cfg.edge, cfg.execution, cfg.amount_usd, float(args.interval_sec), cfg.warmup_timeout_sec, cfg.paired_buffer, cfg.dvol_refresh_sec, cfg.max_dvol_age_sec, cfg.settlement_boundary_usd)
    if args.mode == "live" and not args.i_understand_live_risk:
        raise ValueError("live mode requires --i-understand-live-risk")
    return RuntimeOptions(
        mode=args.mode,
        windows=args.windows,
        once=args.once,
        jsonl=args.jsonl,
        config=cfg,
        live_risk_ack=args.i_understand_live_risk,
    )


def _compact(value: float | None, digits: int = 6) -> float | None:
    return round(float(value), digits) if value is not None else None


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


def _snapshot(window, prices: WindowPrices, feed: BinancePriceFeed, stream: PriceStream, cfg: BotConfig, sigma_eff: float | None) -> tuple[MarketSnapshot, dict[str, Any]]:
    now = dt.datetime.now(dt.timezone.utc)
    age_sec = (now - window.start_time).total_seconds()
    remaining_sec = (window.end_time - now).total_seconds()
    price_source, s_price, basis_bps = effective_price(feed, prices)
    up = token_state(stream, window.up_token, cfg.amount_usd)
    down = token_state(stream, window.down_token, cfg.amount_usd)
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


async def run(options: RuntimeOptions) -> int:
    cfg = options.config
    logger = JsonlLogger(options.jsonl)
    feed = BinancePriceFeed("btcusdt")
    series = MarketSeries.from_known("btc-updown-5m")
    stream = PriceStream(on_price=lambda _update: asyncio.sleep(0))
    volatility: DvolSnapshot | None = None
    try:
        volatility = await asyncio.to_thread(fetch_dvol_snapshot)
    except Exception:
        volatility = None
    next_dvol_refresh = time.monotonic() + cfg.dvol_refresh_sec
    state = StrategyState()
    completed_windows = 0

    gateway = (
        LiveFakExecutionGateway(
            live_risk_ack=options.live_risk_ack,
            retry_count=cfg.execution.retry_count,
            retry_interval_sec=cfg.execution.retry_interval_sec,
        )
        if options.mode == "live"
        else PaperExecutionGateway(stream=stream, config=cfg.execution)
    )

    try:
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
                    state.record_model_prob(snap.age_sec, decision.model_prob)
                if decision.action == "exit":
                    result = await gateway.sell(state.open_position.token_id, state.open_position.filled_shares, min_price=decision.limit_price)
                    row["order"] = result.__dict__
                    if result.success:
                        pnl = state.record_exit(result.avg_price, decision.reason)
                        row["event"] = "exit"
                        row["exit_pnl"] = _compact(pnl, 4)
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
                    )
                    row["order"] = result.__dict__
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
                if options.windows is not None and completed_windows >= options.windows:
                    return 0
                window = find_following_window(window, series)
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
