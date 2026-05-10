from __future__ import annotations

import asyncio
import datetime as dt
import time
from dataclasses import replace

from new_poly.bot_loop import DvolRuntime, FeedContext, WindowCloseResult, _advance_dvol_refresh
from new_poly.bot_runner import BotRunner, StartedContext, StartupContext
from new_poly.bot_runtime import DvolRefreshState, WindowPrices, build_arg_parser, build_runtime_options
from new_poly.market.deribit import DvolSnapshot
from new_poly.market.market import MarketWindow
from new_poly.strategy.state import PositionSnapshot, StrategyState
from new_poly.strategy.prob_edge import MarketSnapshot
from new_poly.strategy.prob_edge import StrategyDecision
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


class DummyGateway:
    async def buy(self, *args, **kwargs):
        return kwargs.get("result")

    async def sell(self, *args, **kwargs):
        return kwargs.get("result")


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


def test_entry_writes_order_intent_before_gateway_returns(monkeypatch) -> None:
    async def scenario() -> None:
        from new_poly.bot_execution_flow import handle_flat_tick
        from new_poly.trading.execution import ExecutionResult

        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
        logger = DummyLogger()
        row = {"event": "tick", "market_slug": "m1"}
        snap = MarketSnapshot(
            market_slug="m1",
            age_sec=130.0,
            remaining_sec=170.0,
            s_price=99.0,
            k_price=100.0,
            sigma_eff=0.6,
        )
        window = _window("m1")
        state = StrategyState(current_market_slug="m1")
        decision = StrategyDecision(
            action="enter",
            reason="edge",
            side="down",
            model_prob=0.70,
            price=0.37,
            limit_price=0.56,
            depth_limit_price=0.37,
            best_ask=0.37,
            edge=0.33,
            phase="core",
            required_edge=0.14,
        )
        gateway_started = asyncio.Event()
        release_gateway = asyncio.Event()

        class BlockingGateway:
            async def buy(self, *args, **kwargs):
                gateway_started.set()
                await release_gateway.wait()
                return ExecutionResult(True, filled_size=2.0, avg_price=0.37, message="MATCHED")

        monkeypatch.setattr("new_poly.bot_execution_flow.evaluate_entry", lambda *_args, **_kwargs: decision)

        task = asyncio.create_task(handle_flat_tick(
            row=row,
            snap=snap,
            window=window,
            prices=WindowPrices(k_price=100.0),
            feeds=FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream()),
            cfg=options.config,
            options=options,
            gateway=BlockingGateway(),
            state=state,
            sigma_eff=0.6,
            price_analysis={},
            logger=logger,
        ))
        await gateway_started.wait()

        assert logger.rows
        assert logger.rows[0]["event"] == "order_intent"
        assert logger.rows[0]["order_intent"] == "entry"
        assert logger.rows[0]["entry_side"] == "down"
        assert logger.rows[0]["limit_price"] == 0.56

        release_gateway.set()
        await task

    asyncio.run(scenario())


def test_exit_writes_exit_intent_before_gateway_returns(monkeypatch) -> None:
    async def scenario() -> None:
        from new_poly.bot_execution_flow import handle_open_position_tick
        from new_poly.trading.execution import ExecutionResult

        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
        logger = DummyLogger()
        row = {"event": "tick", "market_slug": "m1"}
        snap = MarketSnapshot(
            market_slug="m1",
            age_sec=150.0,
            remaining_sec=150.0,
            s_price=99.0,
            k_price=100.0,
            sigma_eff=0.6,
        )
        window = _window("m1")
        state = StrategyState(current_market_slug="m1")
        state.record_entry(PositionSnapshot(
            market_slug="m1",
            token_side="down",
            token_id=window.down_token,
            entry_time=130.0,
            entry_avg_price=0.37,
            filled_shares=2.0,
            entry_model_prob=0.70,
            entry_edge=0.33,
        ))
        decision = StrategyDecision(
            action="exit",
            reason="logic_decay_exit",
            side="down",
            model_prob=0.30,
            price=0.31,
            limit_price=0.26,
            profit_now=-0.06,
        )
        gateway_started = asyncio.Event()
        release_gateway = asyncio.Event()

        class BlockingGateway:
            async def sell(self, *args, **kwargs):
                gateway_started.set()
                await release_gateway.wait()
                return ExecutionResult(False, message="UNMATCHED")

        monkeypatch.setattr("new_poly.bot_execution_flow.evaluate_exit", lambda *_args, **_kwargs: decision)

        task = asyncio.create_task(handle_open_position_tick(
            row=row,
            snap=snap,
            window=window,
            prices=WindowPrices(k_price=100.0),
            feeds=FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream()),
            cfg=options.config,
            options=options,
            gateway=BlockingGateway(),
            state=state,
            sigma_eff=0.6,
            price_analysis={},
            logger=logger,
        ))
        await gateway_started.wait()

        assert logger.rows
        assert logger.rows[0]["event"] == "exit_intent"
        assert logger.rows[0]["exit_intent"] == "exit"
        assert logger.rows[0]["exit_side"] == "down"
        assert logger.rows[0]["exit_reason"] == "logic_decay_exit"
        assert logger.rows[0]["shares"] == 2.0

        release_gateway.set()
        await task

    asyncio.run(scenario())


def test_dust_sell_result_closes_residual_without_crashing(monkeypatch) -> None:
    async def scenario() -> None:
        from new_poly.bot_execution_flow import handle_open_position_tick
        from new_poly.trading.execution import ExecutionResult

        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper", "--analysis-logs"]))
        row = {"event": "tick", "market_slug": "m1"}
        snap = MarketSnapshot(
            market_slug="m1",
            age_sec=150.0,
            remaining_sec=150.0,
            s_price=99.0,
            k_price=100.0,
            sigma_eff=0.6,
        )
        window = _window("m1")
        state = StrategyState(current_market_slug="m1")
        state.record_entry(PositionSnapshot(
            market_slug="m1",
            token_side="down",
            token_id=window.down_token,
            entry_time=130.0,
            entry_avg_price=0.76,
            filled_shares=0.005787,
            entry_model_prob=0.70,
            entry_edge=0.33,
        ))
        decision = StrategyDecision(
            action="exit",
            reason="logic_decay_exit",
            side="down",
            model_prob=0.30,
            price=0.64,
            limit_price=0.64,
        )

        class DustGateway:
            async def sell(self, *args, **kwargs):
                return ExecutionResult(
                    False,
                    message="live dust sell skipped: shares below minimum",
                    mode="live",
                    attempt=0,
                    total_latency_ms=0,
                    timing={"dust_shares": 0.005787, "min_live_sell_shares": 0.01},
                )

        monkeypatch.setattr("new_poly.bot_execution_flow.evaluate_exit", lambda *_args, **_kwargs: decision)

        await handle_open_position_tick(
            row=row,
            snap=snap,
            window=window,
            prices=WindowPrices(k_price=100.0),
            feeds=FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream()),
            cfg=options.config,
            options=options,
            gateway=DustGateway(),
            state=state,
            sigma_eff=0.6,
            price_analysis={},
            logger=DummyLogger(),
        )

        assert row["event"] == "dust_position"
        assert row["exit_reason"] == "dust_position"
        assert state.open_position is None
        assert row["exit_pnl"] == -0.0044

    asyncio.run(scenario())


def test_intentional_safe_sell_residual_logs_position_reduce(monkeypatch) -> None:
    async def scenario() -> None:
        from new_poly.bot_execution_flow import handle_open_position_tick
        from new_poly.trading.execution import ExecutionResult

        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper", "--analysis-logs"]))
        row = {"event": "tick", "market_slug": "m1"}
        snap = MarketSnapshot(
            market_slug="m1",
            age_sec=150.0,
            remaining_sec=150.0,
            s_price=99.0,
            k_price=100.0,
            sigma_eff=0.6,
        )
        window = _window("m1")
        state = StrategyState(current_market_slug="m1")
        state.record_entry(PositionSnapshot(
            market_slug="m1",
            token_side="down",
            token_id=window.down_token,
            entry_time=130.0,
            entry_avg_price=0.37,
            filled_shares=2.0,
            entry_model_prob=0.70,
            entry_edge=0.33,
        ))
        decision = StrategyDecision(
            action="exit",
            reason="logic_decay_exit",
            side="down",
            model_prob=0.30,
            price=0.31,
            limit_price=0.26,
            profit_now=-0.06,
        )

        class ResidualGateway:
            async def sell(self, *args, **kwargs):
                return ExecutionResult(True, filled_size=1.99, avg_price=0.30, message="matched")

        monkeypatch.setattr("new_poly.bot_execution_flow.evaluate_exit", lambda *_args, **_kwargs: decision)

        await handle_open_position_tick(
            row=row,
            snap=snap,
            window=window,
            prices=WindowPrices(k_price=100.0),
            feeds=FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream()),
            cfg=options.config,
            options=options,
            gateway=ResidualGateway(),
            state=state,
            sigma_eff=0.6,
            price_analysis={},
            logger=DummyLogger(),
        )

        assert row["event"] == "position_reduce"
        assert row["exit_status"] == "residual_open"
        assert row["remaining_shares"] == 0.01
        assert state.open_position is not None
        assert abs(state.open_position.filled_shares - 0.01) < 1e-9

    asyncio.run(scenario())


def test_roll_window_updates_context_and_dynamic_state(monkeypatch) -> None:
    async def scenario() -> None:
        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
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
            return WindowCloseResult(next_window, next_prices, new_cfg, dynamic_state, dynamic_task, False)

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


def test_normal_volatility_refresh_is_silent() -> None:
    async def scenario() -> None:
        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
        logger = DummyLogger()
        now = time.time()
        old = DvolSnapshot(
            source="test_dvol",
            currency="BTC",
            dvol=60.0,
            sigma=0.6,
            timestamp_ms=1,
            fetched_at=now,
        )
        new = DvolSnapshot(
            source="test_dvol",
            currency="BTC",
            dvol=61.0,
            sigma=0.61,
            timestamp_ms=2,
            fetched_at=now,
        )
        task = asyncio.create_task(asyncio.sleep(0, result=new))
        await task
        dvol = DvolRuntime(
            state=DvolRefreshState(old),
            refresh_task=task,
            refresh_market_slug="m1",
            next_refresh=999999999.0,
        )

        sigma_eff, stale = await _advance_dvol_refresh(
            dvol=dvol,
            cfg=options.config,
            logger=logger,
            options=options,
            window_slug="m1",
        )

        assert sigma_eff == 0.61
        assert stale is False
        assert logger.rows == []

    asyncio.run(scenario())


def test_start_first_window_logs_live_prefetch_failures(monkeypatch) -> None:
    async def scenario() -> None:
        options = build_runtime_options(build_arg_parser().parse_args([
            "--mode",
            "live",
            "--i-understand-live-risk",
        ]))
        runner = BotRunner(options)
        runner.logger = DummyLogger()
        feeds = FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream())
        runner.startup_context = StartupContext(feeds=feeds, gateway=object())
        window = _window("m1")
        monkeypatch.setattr("new_poly.bot_runner.find_initial_window", lambda _series: window)

        async def fake_start_market_feeds(**_kwargs):
            return None

        async def fake_warmup_binance(**_kwargs):
            return None

        async def fake_prefetch_live_order_params(**kwargs):
            kwargs["logger"].write({
                "event": "clob_prefetch_failed",
                "market_slug": kwargs["market_slug"],
                "token_side": kwargs["token_side"],
                "failed_operation": "get_neg_risk",
                "action": "continue_without_prefetch",
            })
            return {"ok": False}

        monkeypatch.setattr("new_poly.bot_runner.start_market_feeds", fake_start_market_feeds)
        monkeypatch.setattr("new_poly.bot_runner.warmup_binance", fake_warmup_binance)
        monkeypatch.setattr("new_poly.bot_runner._prefetch_live_order_params", fake_prefetch_live_order_params)

        await runner.start_first_window()

        failures = [row for row in runner.logger.rows if row.get("event") == "clob_prefetch_failed"]
        assert [row["token_side"] for row in failures] == ["up", "down"]
        assert [row for row in runner.logger.rows if row.get("event") == "clob_prefetch_ready"] == []
        assert runner.active.window is window

    asyncio.run(scenario())


def test_start_first_window_writes_operational_lifecycle_rows_without_analysis_logs(monkeypatch) -> None:
    async def scenario() -> None:
        options = build_runtime_options(build_arg_parser().parse_args([
            "--mode",
            "live",
            "--i-understand-live-risk",
            "--no-analysis-logs",
        ]))
        runner = BotRunner(options)
        runner.logger = DummyLogger()
        feeds = FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream())
        runner.startup_context = StartupContext(feeds=feeds, gateway=object())
        window = _window("m1")
        monkeypatch.setattr("new_poly.bot_runner.find_initial_window", lambda _series: window)

        async def fake_start_market_feeds(**_kwargs):
            return None

        async def fake_warmup_binance(**_kwargs):
            return None

        async def fake_prefetch_live_order_params(**kwargs):
            return {"ok": True, "token_id": kwargs["token_id"], "cached": False}

        monkeypatch.setattr("new_poly.bot_runner.start_market_feeds", fake_start_market_feeds)
        monkeypatch.setattr("new_poly.bot_runner.warmup_binance", fake_warmup_binance)
        monkeypatch.setattr("new_poly.bot_runner._prefetch_live_order_params", fake_prefetch_live_order_params)

        await runner.start_first_window()

        events = [row["event"] for row in runner.logger.rows]
        assert "window_selected" in events
        assert "market_feeds_started" in events
        assert events.count("clob_prefetch_started") == 2
        assert events.count("clob_prefetch_ready") == 2
        assert "binance_warmup_started" in events
        assert "binance_warmup_ready" in events

    asyncio.run(scenario())


def test_run_tick_stops_after_fatal_stop(monkeypatch) -> None:
    async def scenario() -> None:
        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
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
        tick = type("Tick", (), {
            "row": {"event": "fatal_stop", "market_slug": window.slug},
            "snap": object(),
            "sigma_eff": 0.6,
            "price_analysis": {},
            "reference_meta": {},
        })()

        async def fake_prepare_tick_context():
            return tick

        async def fake_handle_strategy_tick(**kwargs):
            runner.state.fatal_stop_reason = "live_no_sellable_balance"
            return StrategyDecision(action="skip", reason="fatal_stop")

        monkeypatch.setattr(runner, "prepare_tick_context", fake_prepare_tick_context)
        monkeypatch.setattr(runner, "handle_strategy_tick", fake_handle_strategy_tick)

        should_stop = await runner.run_tick()

        assert should_stop is True

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


def test_switch_to_next_window_resets_state_and_stream(monkeypatch) -> None:
    async def scenario() -> None:
        from new_poly.bot_loop import LoopRuntime, _switch_to_next_window

        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
        stream = DummyStream()
        feeds = FeedContext(binance=None, coinbase=None, polymarket=None, stream=stream)
        first = _window("m1")
        next_window = _window("m2", offset=5)
        state = StrategyState(current_market_slug=first.slug, entry_count=3)
        loop = LoopRuntime(seen_repetitive_skips={(first.slug, "edge_too_small")})

        monkeypatch.setattr("new_poly.bot_loop.find_following_window", lambda window, series: next_window)

        logger = DummyLogger()
        window, prices = await _switch_to_next_window(
            window=first,
            series=object(),
            feeds=feeds,
            state=state,
            loop=loop,
            options=options,
            logger=logger,
        )

        assert window is next_window
        assert isinstance(prices, WindowPrices)
        assert state.current_market_slug == next_window.slug
        assert state.entry_count == 0
        assert loop.seen_repetitive_skips == set()
        assert stream.switched == [[next_window.up_token, next_window.down_token]]
        assert len(logger.rows) == 1
        row = logger.rows[0]
        assert row["event"] == "window_selected"
        assert row["mode"] == "paper"
        assert row["market_slug"] == next_window.slug
        assert row["window_start"] == next_window.start_time.isoformat()
        assert row["window_end"] == next_window.end_time.isoformat()
        assert row["completed_windows"] == 0
        assert row["ts"]

    asyncio.run(scenario())
