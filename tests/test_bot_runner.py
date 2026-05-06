from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import replace

from new_poly.bot_loop import DvolRuntime, FeedContext
from new_poly.bot_runner import BotRunner, StartedContext, StartupContext
from new_poly.bot_runtime import DvolRefreshState, WindowPrices, build_arg_parser, build_runtime_options
from new_poly.market.deribit import DvolSnapshot
from new_poly.market.market import MarketWindow
from new_poly.strategy.state import PositionSnapshot, StrategyState
from new_poly.strategy.prob_edge import MarketSnapshot
from new_poly.strategy.dynamic_params import DynamicConfig, DynamicState, SignalProfile


class DummyLogger:
    def __init__(self) -> None:
        self.rows = []
        self.prune_result = 0
        self.prune_calls = 0

    def write(self, row) -> None:
        self.rows.append(row)

    def prune(self) -> int:
        self.prune_calls += 1
        return self.prune_result

    def close(self) -> None:
        pass


class DummyStream:
    def __init__(self) -> None:
        self.switched = []

    async def switch_tokens(self, token_ids):
        self.switched.append(list(token_ids))


def _window(slug: str, *, offset: int = 0) -> MarketWindow:
    start = dt.datetime(2026, 5, 6, 0, 0, tzinfo=dt.timezone.utc) + dt.timedelta(minutes=offset)
    return MarketWindow(
        question=f"{slug}?",
        up_token=f"{slug}-up",
        down_token=f"{slug}-down",
        start_time=start,
        end_time=start + dt.timedelta(minutes=5),
        slug=slug,
        resolution_source="test",
    )


def test_roll_window_updates_context_and_dynamic_state(monkeypatch) -> None:
    async def scenario() -> None:
        options = build_runtime_options(build_arg_parser().parse_args(["--once"]))
        runner = BotRunner(options)
        runner.logger = DummyLogger()
        stream = DummyStream()
        feeds = FeedContext(binance=None, coinbase=None, polymarket=None, stream=stream)
        gateway = object()
        first = _window("m1")
        next_window = _window("m2", offset=5)
        initial_prices = WindowPrices(k_price=100.0)
        next_prices = WindowPrices(k_price=101.0)
        new_cfg = replace(options.config, amount_usd=2.0)
        dynamic_state = DynamicState(active_profile="aggressive")
        dynamic_task = asyncio.create_task(asyncio.sleep(0))

        async def fake_handle_window_close(**kwargs):
            assert kwargs["window"] is first
            assert kwargs["prices"] is initial_prices
            assert kwargs["feeds"] is feeds
            assert kwargs["dynamic_cfg"] is runner.dynamic.cfg
            assert kwargs["dynamic_state"] is runner.dynamic.state
            assert kwargs["dynamic_task"] is runner.dynamic.task
            return next_window, next_prices, new_cfg, dynamic_state, dynamic_task, False

        monkeypatch.setattr("new_poly.bot_runner._handle_window_close", fake_handle_window_close)

        runner.startup_context = StartupContext(feeds=feeds, gateway=gateway)
        runner.context = StartedContext(feeds=feeds, gateway=gateway, window=first, prices=initial_prices)
        runner.dynamic.cfg = DynamicConfig(
            profiles=[
                SignalProfile(
                    name="aggressive",
                    entry_start_age_sec=100,
                    entry_end_age_sec=240,
                    early_required_edge=0.14,
                    core_required_edge=0.12,
                    max_entries_per_market=4,
                    min_candidate_trades=12,
                    risk_rank=0,
                )
            ]
        )
        runner.dynamic.state = DynamicState(active_profile="aggressive", pending_profile="aggressive")
        initial_dynamic_task = asyncio.create_task(asyncio.sleep(0))
        runner.dynamic.task = initial_dynamic_task
        runner.state.entry_count = 3
        runner.state.current_market_slug = first.slug

        should_stop = await runner.roll_window()

        assert should_stop is False
        assert runner.active.window is next_window
        assert runner.active.prices is next_prices
        assert runner.cfg is new_cfg
        assert runner.dynamic.state is dynamic_state
        assert runner.dynamic.task is dynamic_task
        assert runner.state.current_market_slug == next_window.slug
        assert runner.state.entry_count == 0
        assert runner.state.open_position is None
        assert runner.active.feeds is feeds
        assert runner.active.gateway is gateway

        await asyncio.gather(initial_dynamic_task, dynamic_task, runner.dynamic.task, return_exceptions=True)

    asyncio.run(scenario())


def test_prepare_tick_context_collects_snapshot_and_row(monkeypatch) -> None:
    async def scenario() -> None:
        options = build_runtime_options(build_arg_parser().parse_args(["--once"]))
        runner = BotRunner(options)
        runner.logger = DummyLogger()
        feeds = FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream())
        window = _window("m1")
        runner.startup_context = StartupContext(feeds=feeds, gateway=object())
        runner.context = StartedContext(feeds=feeds, gateway=object(), window=window, prices=WindowPrices(k_price=100.0))
        runner.dvol = DvolRuntime(
            state=DvolRefreshState(DvolSnapshot(
                source="test_dvol",
                currency="BTC",
                dvol=60.0,
                sigma=0.6,
                timestamp_ms=1,
                fetched_at=1.0,
            )),
            refresh_task=None,
            refresh_market_slug=None,
            next_refresh=999999999.0,
        )
        snap = MarketSnapshot(
            market_slug=window.slug,
            age_sec=1.0,
            remaining_sec=299.0,
            s_price=101.0,
            k_price=100.0,
            sigma_eff=0.6,
        )
        meta = {
            "ts": "2026-05-06T00:00:01+00:00",
            "market_slug": window.slug,
            "age_sec": 1.0,
            "remaining_sec": 299.0,
            "s_price": 101.0,
            "k_price": 100.0,
        }

        async def fake_refresh_window_inputs():
            runner.active.prices.k_price = 100.0

        async def fake_advance_dvol():
            return 0.6, False

        monkeypatch.setattr(runner, "refresh_window_inputs", fake_refresh_window_inputs)
        monkeypatch.setattr(runner, "advance_dvol", fake_advance_dvol)
        monkeypatch.setattr(runner, "build_snapshot", lambda sigma_eff: (snap, meta))

        tick = await runner.prepare_tick_context()

        assert tick.snap is snap
        assert tick.meta is meta
        assert tick.sigma_eff == 0.6
        assert tick.dvol_stale is False
        assert tick.row["event"] == "tick"
        assert tick.row["market_slug"] == window.slug
        assert tick.row["sigma_source"] == "test_dvol"
        assert tick.price_analysis == {"s_price": 101.0, "k_price": 100.0}
        assert tick.reference_meta == {}

    asyncio.run(scenario())


def test_settle_open_position_writes_settlement_row(monkeypatch) -> None:
    options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
    logger = DummyLogger()
    feeds = FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream())
    window = _window("m1")
    prices = WindowPrices(k_price=100.0)
    state = StrategyState()
    state.record_entry(PositionSnapshot(
        market_slug=window.slug,
        token_side="up",
        token_id=window.up_token,
        entry_time=120.0,
        entry_avg_price=0.40,
        filled_shares=2.0,
        entry_model_prob=0.70,
        entry_edge=0.30,
    ))

    monkeypatch.setattr("new_poly.bot_loop.effective_price", lambda *args, **kwargs: type(
        "Price",
        (),
        {"effective": 105.0},
    )())

    from new_poly.bot_loop import _settle_open_position_if_needed

    _settle_open_position_if_needed(
        window=window,
        prices=prices,
        cfg=options.config,
        options=options,
        feeds=feeds,
        state=state,
        logger=logger,
    )

    assert state.open_position is None
    assert state.realized_pnl == 1.2
    assert len(logger.rows) == 1
    row = logger.rows[0]
    assert row["event"] == "settlement"
    assert row["market_slug"] == window.slug
    assert row["winning_side"] == "up"
    assert row["settlement_proxy_price"] == 105.0
    assert row["settlement_pnl"] == 1.2


def test_prune_logs_after_window_writes_retention_row() -> None:
    from new_poly.bot_loop import LoopRuntime, _prune_logs_after_window_if_needed

    options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
    logger = DummyLogger()
    logger.prune_result = 7
    loop = LoopRuntime(completed_windows=10)

    _prune_logs_after_window_if_needed(loop=loop, logger=logger, options=options)

    assert logger.prune_calls == 1
    assert len(logger.rows) == 1
    row = logger.rows[0]
    assert row["event"] == "log_retention"
    assert row["mode"] == "paper"
    assert row["retention_hours"] == options.log_retention_hours
    assert row["prune_every_windows"] == options.log_prune_every_windows
    assert row["removed_rows"] == 7
