from __future__ import annotations

import asyncio
import datetime as dt
import time
from dataclasses import replace

from new_poly.bot_loop import DvolRuntime, FeedContext, WindowCloseResult, WindowContext, _advance_dvol_refresh
from new_poly import bot_loop
from new_poly.bot_runner import BotRunner, StartedContext, StartupContext, TickContext
from new_poly.bot_runtime import DvolRefreshState, WindowPrices, build_arg_parser, build_runtime_options
from new_poly.market.deribit import DvolSnapshot
from new_poly.market.market import MarketWindow
from new_poly.strategy.state import PositionSnapshot, StrategyState, UnknownEntryOrder
from new_poly.strategy.prob_edge import MarketSnapshot
from new_poly.strategy.prob_edge import StrategyDecision
from new_poly.strategy.dynamic_params import DynamicState


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


def test_window_close_settlement_helpers_are_async() -> None:
    assert asyncio.iscoroutinefunction(bot_loop._settle_open_position_if_needed)
    assert asyncio.iscoroutinefunction(bot_loop._write_window_settlement_row)
    assert asyncio.iscoroutinefunction(bot_loop._crypto_close_settlement)


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

        monkeypatch.setattr("new_poly.bot_execution_flow.evaluate_poly_entry", lambda *_args, **_kwargs: decision)

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


def test_compact_trade_events_keep_position_snapshots_without_analysis_logs(monkeypatch) -> None:
    async def scenario() -> None:
        from new_poly.bot_execution_flow import handle_flat_tick, handle_open_position_tick
        from new_poly.trading.execution import ExecutionResult

        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper", "--no-analysis-logs"]))
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
        entry_decision = StrategyDecision(
            action="enter",
            reason="poly_edge",
            side="up",
            price=0.42,
            limit_price=0.50,
            depth_limit_price=0.42,
            best_ask=0.42,
            edge=4.0,
        )

        class BuyGateway:
            async def buy(self, *args, **kwargs):
                return ExecutionResult(True, filled_size=2.5, avg_price=0.42, message="MATCHED")

        monkeypatch.setattr("new_poly.bot_execution_flow.evaluate_poly_entry", lambda *_args, **_kwargs: entry_decision)

        await handle_flat_tick(
            row=row,
            snap=snap,
            window=window,
            prices=WindowPrices(k_price=100.0),
            feeds=FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream()),
            cfg=options.config,
            options=options,
            gateway=BuyGateway(),
            state=state,
            sigma_eff=0.6,
            price_analysis={},
            logger=DummyLogger(),
        )

        assert "analysis" not in row
        assert row["position_after_entry"]["token_side"] == "up"
        assert row["position_after_entry"]["entry_avg_price"] == 0.42

        exit_row = {"event": "tick", "market_slug": "m1"}
        exit_decision = StrategyDecision(
            action="exit",
            reason="poly_hold_score_exit",
            side="up",
            price=0.61,
            limit_price=0.60,
        )

        class SellGateway:
            async def sell(self, *args, **kwargs):
                return ExecutionResult(True, filled_size=2.5, avg_price=0.61, message="MATCHED")

        monkeypatch.setattr("new_poly.bot_execution_flow.evaluate_poly_exit", lambda *_args, **_kwargs: exit_decision)

        await handle_open_position_tick(
            row=exit_row,
            snap=replace(snap, age_sec=160.0, remaining_sec=140.0),
            window=window,
            prices=WindowPrices(k_price=100.0),
            feeds=FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream()),
            cfg=options.config,
            options=options,
            gateway=SellGateway(),
            state=state,
            sigma_eff=0.6,
            price_analysis={},
            logger=DummyLogger(),
        )

        assert "analysis" not in exit_row
        assert exit_row["position_before_exit"]["token_side"] == "up"
        assert exit_row["position_after_exit"] is None

    asyncio.run(scenario())


def test_poly_source_order_intent_omits_empty_legacy_probability_fields() -> None:
    from new_poly.bot_execution_flow import _order_intent_row

    options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
    decision = StrategyDecision(
        action="enter",
        reason="poly_edge",
        side="down",
        price=0.37,
        limit_price=0.75,
        edge=5.2,
        poly_reference_distance_bps=2.0,
        poly_return_bps=1.0,
        poly_trend_lookback_sec=10.0,
        poly_entry_score=5.2,
    )

    row = _order_intent_row(
        row={"ts": "2026-05-13T00:00:00+00:00", "market_slug": "m1", "age_sec": 130, "remaining_sec": 170},
        intent="entry",
        token_id="token",
        decision=decision,
        price_analysis={},
        options=options,
        extra={"amount_usd": 1.0},
    )

    assert row["event"] == "order_intent"
    assert row["reason"] == "poly_edge"
    assert row["edge"] == 5.2
    assert "model_prob" not in row
    assert "required_edge" not in row
    assert "phase" not in row


def test_order_intent_keeps_structural_null_fields() -> None:
    from new_poly.bot_execution_flow import _order_intent_row

    options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
    decision = StrategyDecision(action="exit", reason="missing_exit_depth")

    row = _order_intent_row(
        row={"ts": "2026-05-13T00:00:00+00:00", "market_slug": "m1", "age_sec": 130, "remaining_sec": 170},
        intent="exit",
        token_id="token",
        decision=decision,
        price_analysis={},
        options=options,
    )

    assert row["event"] == "exit_intent"
    assert row["side"] is None
    assert row["exit_side"] is None
    assert row["signal_price"] is None
    assert row["limit_price"] is None
    assert "model_prob" not in row


def test_live_unknown_buy_safety_check_uses_poly_entry_end_age() -> None:
    from new_poly.bot_execution_flow import _unknown_buy_needs_safety_check

    options = build_runtime_options(build_arg_parser().parse_args([
        "--config",
        "configs/prob_poly_single_source.yaml",
    ]))
    options = replace(options, mode="live")
    window = _window("m1")
    state = StrategyState(current_market_slug="m1")
    state.unresolved_unknown_entry = UnknownEntryOrder(
        market_slug="m1",
        token_side="up",
        token_id=window.up_token,
        amount_usd=1.0,
        entry_time=200.0,
        entry_avg_price=0.29,
        entry_model_prob=0.0,
        entry_edge=5.0,
    )
    snap = MarketSnapshot(
        market_slug="m1",
        age_sec=240.0,
        remaining_sec=100.0,
        s_price=101.0,
        k_price=100.0,
        sigma_eff=0.6,
    )

    assert options.config.poly_source is not None
    assert options.config.poly_source.entry_end_age_sec == 220.0
    assert _unknown_buy_needs_safety_check(state=state, snap=snap, window=window, cfg=options.config, options=options)


def test_post_exit_observation_ticks_are_throttled_and_keep_poly_context() -> None:
    options = build_runtime_options(build_arg_parser().parse_args([
        "--config",
        "configs/prob_poly_single_source.yaml",
        "--mode",
        "paper",
    ]))
    options = replace(options, post_exit_observation_interval_sec=10.0)
    runner = BotRunner(options)
    runner.logger = DummyLogger()
    runner.state = StrategyState(
        current_market_slug="m1",
        entry_count=1,
        last_exit_reason="poly_hold_score_exit",
        last_exit_side="up",
        last_exit_age_sec=120.0,
    )
    base_snap = MarketSnapshot(
        market_slug="m1",
        age_sec=125.0,
        remaining_sec=175.0,
        s_price=None,
        k_price=100000.0,
        sigma_eff=0.6,
        polymarket_price=100020.0,
        polymarket_return_3s_bps=0.4,
    )
    base_tick = TickContext(
        sigma_eff=0.6,
        dvol_stale=False,
        snap=base_snap,
        meta={},
        price_analysis={"strategy_price_source": "polymarket_reference", "polymarket_price": 100020.0},
        reference_meta={"polymarket_price": 100020.0, "lead_polymarket_return_3s_bps": 0.4},
        row={
            "ts": "2026-05-12T00:00:00+00:00",
            "event": "tick",
            "mode": "paper",
            "market_slug": "m1",
            "age_sec": 125,
            "remaining_sec": 175,
            "k_price": 100000.0,
            "up": {"ask": 0.70, "bid": 0.65},
            "down": {"ask": 0.35, "bid": 0.30},
        },
    )

    runner.write_post_exit_observation_if_due(base_tick)
    assert runner.logger.rows == []

    due_tick = replace(base_tick, snap=replace(base_snap, age_sec=130.0, remaining_sec=170.0), row={**base_tick.row, "age_sec": 130, "remaining_sec": 170})
    runner.write_post_exit_observation_if_due(due_tick)
    assert len(runner.logger.rows) == 1
    row = runner.logger.rows[0]
    assert row["event"] == "post_exit_observation"
    assert row["last_exit_reason"] == "poly_hold_score_exit"
    assert row["last_exit_side"] == "up"
    assert row["last_exit_age_sec"] == 120.0
    assert row["decision"] == {"action": "observe", "reason": "post_exit_observation"}
    assert row["polymarket_price"] == 100020.0
    assert row["lead_polymarket_return_3s_bps"] == 0.4
    assert "reference" not in row
    assert "analysis" not in row

    runner.write_post_exit_observation_if_due(replace(due_tick, snap=replace(base_snap, age_sec=135.0, remaining_sec=165.0), row={**base_tick.row, "age_sec": 135, "remaining_sec": 165}))
    assert len(runner.logger.rows) == 1

    runner.write_post_exit_observation_if_due(replace(due_tick, snap=replace(base_snap, age_sec=140.0, remaining_sec=160.0), row={**base_tick.row, "age_sec": 140, "remaining_sec": 160}))
    assert len(runner.logger.rows) == 2


def test_poly_source_tick_logs_keep_backtest_fields_and_drop_repeated_analysis() -> None:
    from new_poly.bot_logging import write_tick_row

    options = build_runtime_options(build_arg_parser().parse_args([
        "--config",
        "configs/prob_poly_single_source.yaml",
        "--mode",
        "paper",
    ]))
    logger = DummyLogger()
    loop = bot_loop.LoopRuntime()
    state = StrategyState(open_position=PositionSnapshot(
        market_slug="m1",
        token_side="up",
        token_id="up-token",
        entry_time=130.0,
        entry_avg_price=0.42,
        filled_shares=2.0,
        entry_model_prob=0.0,
        entry_edge=4.0,
    ))
    row = {
        "ts": "2026-05-12T00:00:00+00:00",
        "event": "tick",
        "mode": "paper",
        "market_slug": "m1",
        "window_start": "2026-05-12T00:00:00+00:00",
        "window_end": "2026-05-12T00:05:00+00:00",
        "age_sec": 150.0,
        "remaining_sec": 150.0,
        "k_price": 100000.0,
        "sigma_source": "not_used",
        "sigma_eff": None,
        "volatility_stale": False,
        "realized_pnl": 0.0,
        "position": {"token_side": "up", "entry_avg_price": 0.42},
        "decision": {"action": "skip", "reason": "poly_score_too_low", "poly_entry_score": 5.5},
        "up": {
            "ask": 0.43,
            "bid": 0.41,
            "ask_avg": 0.431,
            "ask_limit": 0.44,
            "bid_avg": 0.409,
            "bid_limit": 0.40,
            "bid_depth_ok": True,
            "book_age_ms": 80,
            "bid_age_ms": 75,
            "ask_age_ms": 70,
            "stable_depth_usd": 100.0,
        },
        "down": {
            "ask": 0.59,
            "bid": 0.57,
            "ask_avg": 0.591,
            "ask_limit": 0.60,
            "bid_avg": 0.569,
            "bid_limit": 0.56,
            "bid_depth_ok": True,
            "book_age_ms": 90,
            "bid_age_ms": 85,
            "stable_depth_usd": 100.0,
        },
    }

    write_tick_row(
        logger=logger,
        loop=loop,
        options=options,
        state=state,
        row=row,
        reference_meta={
            "polymarket_price": 100050.0,
            "polymarket_price_age_sec": 0.2,
            "lead_polymarket_return_3s_bps": 0.7,
            "lead_polymarket_return_10s_bps": 1.4,
        },
        decision=StrategyDecision(action="skip", reason="poly_score_too_low"),
    )

    assert len(logger.rows) == 1
    compact = logger.rows[0]
    assert compact["event"] == "tick"
    assert compact["market_slug"] == "m1"
    assert compact["k_price"] == 100000.0
    assert compact["polymarket_price"] == 100050.0
    assert compact["lead_polymarket_return_10s_bps"] == 1.4
    assert compact["up"] == {
        "ask": 0.43,
        "bid_avg": 0.409,
        "bid_limit": 0.40,
        "bid_depth_ok": True,
        "book_age_ms": 80,
    }
    assert "decision" not in compact
    assert "position" not in compact
    assert "reference" not in compact
    assert "mode" not in compact
    assert "window_start" not in compact
    assert "stable_depth_usd" not in compact["up"]
    assert "bid" not in compact["up"]
    assert "ask_limit" not in compact["up"]
    assert "bid_age_ms" not in compact["up"]


def test_poly_source_live_tick_logs_are_not_compacted() -> None:
    from new_poly.bot_logging import compact_high_frequency_row

    options = build_runtime_options(build_arg_parser().parse_args([
        "--config",
        "configs/prob_poly_single_source.yaml",
        "--mode",
        "live",
        "--i-understand-live-risk",
    ]))
    row = {
        "event": "tick",
        "mode": "live",
        "market_slug": "m1",
        "decision": {"action": "skip", "reason": "test"},
        "position": {"token_side": "up"},
        "clob_ws": {"last_depth_update_age_ms": 1500.0},
        "up": {"ask": 0.51, "ask_avg": 0.52, "bid": 0.49, "stable_depth_usd": 10.0},
    }

    assert compact_high_frequency_row(row, options=options) is row


def test_non_poly_source_tick_logs_are_not_compacted() -> None:
    from new_poly.bot_logging import compact_high_frequency_row

    options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
    options = replace(options, config=replace(options.config, strategy_mode="prob_edge"))
    row = {"event": "tick", "mode": "paper", "decision": {"action": "skip"}, "position": {"token_side": "up"}}

    assert compact_high_frequency_row(row, options=options) is row


def test_poly_source_compact_tick_keeps_clob_ws_and_prefers_top_level_analysis() -> None:
    from new_poly.bot_logging import compact_high_frequency_row

    options = build_runtime_options(build_arg_parser().parse_args([
        "--config",
        "configs/prob_poly_single_source.yaml",
        "--mode",
        "paper",
    ]))
    row = {
        "event": "tick",
        "market_slug": "m1",
        "age_sec": 150.0,
        "remaining_sec": 150.0,
        "k_price": 100000.0,
        "clob_ws": {"last_depth_update_age_ms": 1500.0},
        "analysis": {
            "polymarket_price": 100020.0,
            "price_sources": {"polymarket_price": 100010.0, "lead_polymarket_return_10s_bps": 0.4},
        },
        "up": {"ask": 0.43, "bid_avg": 0.41, "bid_limit": 0.40, "bid_depth_ok": True, "book_age_ms": 80},
    }

    compact = compact_high_frequency_row(row, options=options)

    assert compact["clob_ws"] == {"last_depth_update_age_ms": 1500.0}
    assert compact["polymarket_price"] == 100020.0
    assert compact["lead_polymarket_return_10s_bps"] == 0.4


def test_window_context_reset_clears_post_exit_observation_throttle() -> None:
    options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
    runner = BotRunner(options)
    runner.startup_context = StartupContext(
        feeds=FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream()),
        gateway=DummyGateway(),
    )
    runner.context = StartedContext(
        feeds=runner.startup_context.feeds,
        gateway=runner.startup_context.gateway,
        window=_window("m1"),
        prices=WindowPrices(k_price=100.0),
    )
    runner.loop.post_exit_observation_last_age_sec = 250.0
    runner.loop.post_exit_observation_market_slug = "m1"

    runner.set_window_context(WindowContext(window=_window("m2", offset=5), prices=WindowPrices(k_price=101.0)))

    assert runner.loop.post_exit_observation_last_age_sec is None
    assert runner.loop.post_exit_observation_market_slug is None


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
            reason="poly_hold_score_exit",
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

        monkeypatch.setattr("new_poly.bot_execution_flow.evaluate_poly_exit", lambda *_args, **_kwargs: decision)

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
        assert logger.rows[0]["exit_reason"] == "poly_hold_score_exit"
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
            reason="poly_hold_score_exit",
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

        monkeypatch.setattr("new_poly.bot_execution_flow.evaluate_poly_exit", lambda *_args, **_kwargs: decision)

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
            reason="poly_hold_score_exit",
            side="down",
            model_prob=0.30,
            price=0.31,
            limit_price=0.26,
            profit_now=-0.06,
        )

        class ResidualGateway:
            async def sell(self, *args, **kwargs):
                return ExecutionResult(True, filled_size=1.99, avg_price=0.30, message="matched")

        monkeypatch.setattr("new_poly.bot_execution_flow.evaluate_poly_exit", lambda *_args, **_kwargs: decision)

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


def test_roll_window_waits_long_enough_for_unknown_buy_reconcile() -> None:
    from new_poly.bot_runner import PENDING_EXECUTION_WINDOW_CLOSE_TIMEOUT_SEC
    from new_poly.trading.execution import UNKNOWN_BUY_RECONCILE_DELAYS_SEC

    assert PENDING_EXECUTION_WINDOW_CLOSE_TIMEOUT_SEC >= max(UNKNOWN_BUY_RECONCILE_DELAYS_SEC) + 3.0


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
        assert tick.price_analysis == {"strategy_price_source": "polymarket_reference", "k_price": 100.0}
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
    monkeypatch.setattr("new_poly.bot_loop.fetch_crypto_price_api", lambda window: None)

    from new_poly.bot_loop import _settle_open_position_if_needed

    asyncio.run(_settle_open_position_if_needed(
        window=window,
        prices=prices,
        cfg=options.config,
        options=options,
        feeds=feeds,
        state=state,
        logger=logger,
    ))

    assert state.open_position is None
    assert state.realized_pnl == 1.2
    assert len(logger.rows) == 1
    row = logger.rows[0]
    assert row["event"] == "settlement"
    assert row["market_slug"] == window.slug
    assert row["winning_side"] == "up"
    assert row["settlement_proxy_price"] == 105.0
    assert row["settlement_pnl"] == 1.2


def test_settle_open_position_prefers_polymarket_close_price(monkeypatch) -> None:
    options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
    logger = DummyLogger()
    feeds = FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream())
    window = _window("m1")
    prices = WindowPrices(k_price=100.0)
    state = StrategyState()
    state.record_entry(PositionSnapshot(
        market_slug=window.slug,
        token_side="down",
        token_id=window.down_token,
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
    monkeypatch.setattr("new_poly.bot_loop.fetch_crypto_price_api", lambda window: {
        "openPrice": 100.0,
        "closePrice": 99.5,
        "completed": True,
        "incomplete": False,
        "cached": True,
    })

    from new_poly.bot_loop import _settle_open_position_if_needed

    asyncio.run(_settle_open_position_if_needed(
        window=window,
        prices=prices,
        cfg=options.config,
        options=options,
        feeds=feeds,
        state=state,
        logger=logger,
    ))

    row = logger.rows[0]
    assert row["winning_side"] == "down"
    assert row["settlement_source"] == "polymarket_crypto_price_api_close"
    assert row["settlement_price"] == 99.5
    assert row["settlement_close_price"] == 99.5
    assert row["settlement_open_price"] == 100.0
    assert row["settlement_proxy_price"] == 105.0
    assert row["settlement_pnl"] == 1.2


def test_window_close_price_row_records_polymarket_close(monkeypatch) -> None:
    options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper"]))
    logger = DummyLogger()
    window = _window("m1")
    monkeypatch.setattr("new_poly.bot_loop.fetch_crypto_price_api", lambda window: {
        "openPrice": 100.0,
        "closePrice": 101.0,
        "completed": True,
        "incomplete": False,
        "cached": True,
    })

    from new_poly.bot_loop import _write_window_settlement_row

    asyncio.run(_write_window_settlement_row(window=window, cfg=options.config, options=options, logger=logger))

    assert len(logger.rows) == 1
    row = logger.rows[0]
    assert row["event"] == "window_settlement"
    assert row["market_slug"] == window.slug
    assert row["winning_side"] == "up"
    assert row["settlement_source"] == "polymarket_crypto_price_api_close"
    assert row["settlement_open_price"] == 100.0
    assert row["settlement_close_price"] == 101.0
    assert row["settlement_price"] == 101.0


def test_window_close_without_position_defers_settlement_until_next_window(monkeypatch) -> None:
    async def scenario() -> None:
        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper", "--windows", "2"]))
        logger = DummyLogger()
        loop = bot_loop.LoopRuntime()
        stream = DummyStream()
        calls = []
        first = _window("m1", offset=0)
        second = _window("m2", offset=5)

        monkeypatch.setattr("new_poly.bot_loop.find_following_window", lambda window, series: second)
        monkeypatch.setattr("new_poly.bot_loop.fetch_crypto_price_api", lambda window: calls.append(window.slug) or {
            "openPrice": 100.0,
            "closePrice": 101.0,
            "completed": True,
            "incomplete": False,
            "cached": True,
        })

        result = await bot_loop._handle_window_close(
            window=first,
            prices=WindowPrices(k_price=100.0),
            cfg=options.config,
            options=options,
            feeds=FeedContext(binance=None, coinbase=None, polymarket=None, stream=stream),
            state=StrategyState(),
            loop=loop,
            logger=logger,
            series=object(),
            dynamic_state=None,
            dynamic_task=None,
        )

        assert result.should_stop is False
        assert result.window is second
        assert calls == []
        assert [row["event"] for row in logger.rows] == ["window_selected"]

        await bot_loop._write_pending_window_settlement_if_due(
            loop=loop,
            logger=logger,
            now=second.start_time + dt.timedelta(seconds=89),
        )
        assert calls == []

        await bot_loop._write_pending_window_settlement_if_due(
            loop=loop,
            logger=logger,
            now=second.start_time + dt.timedelta(seconds=90),
        )
        assert calls == ["m1"]
        assert logger.rows[-1]["event"] == "window_settlement"
        assert logger.rows[-1]["market_slug"] == "m1"
        assert logger.rows[-1]["settlement_close_price"] == 101.0

    asyncio.run(scenario())


def test_window_close_with_open_position_defers_position_settlement_until_close_price_is_ready(monkeypatch) -> None:
    async def scenario() -> None:
        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper", "--windows", "2"]))
        logger = DummyLogger()
        loop = bot_loop.LoopRuntime()
        stream = DummyStream()
        calls = []
        first = _window("m1", offset=0)
        second = _window("m2", offset=5)
        state = StrategyState(current_market_slug=first.slug)
        state.record_entry(PositionSnapshot(
            market_slug=first.slug,
            token_side="up",
            token_id=first.up_token,
            entry_time=120.0,
            entry_avg_price=0.40,
            filled_shares=2.0,
            entry_model_prob=0.70,
            entry_edge=0.30,
        ))

        monkeypatch.setattr("new_poly.bot_loop.find_following_window", lambda window, series: second)
        monkeypatch.setattr("new_poly.bot_loop.fetch_crypto_price_api", lambda window: calls.append(window.slug) or {
            "openPrice": 100.0,
            "closePrice": 101.0,
            "completed": True,
            "incomplete": False,
            "cached": True,
        })

        result = await bot_loop._handle_window_close(
            window=first,
            prices=WindowPrices(k_price=100.0),
            cfg=options.config,
            options=options,
            feeds=FeedContext(binance=None, coinbase=None, polymarket=None, stream=stream),
            state=state,
            loop=loop,
            logger=logger,
            series=object(),
            dynamic_state=None,
            dynamic_task=None,
        )

        assert result.should_stop is False
        assert result.window is second
        assert calls == []
        assert state.open_position is None
        assert state.realized_pnl == 0.0
        assert [row["event"] for row in logger.rows] == ["window_selected"]

        await bot_loop._write_pending_window_settlement_if_due(
            loop=loop,
            logger=logger,
            now=second.start_time + dt.timedelta(seconds=90),
        )

        assert calls == ["m1"]
        assert state.realized_pnl == 1.2
        assert [row["event"] for row in logger.rows] == ["window_selected", "settlement"]
        row = logger.rows[-1]
        assert row["market_slug"] == first.slug
        assert row["winning_side"] == "up"
        assert row["settlement_source"] == "polymarket_crypto_price_api_close"
        assert row["settlement_pnl"] == 1.2
        assert row["realized_pnl"] == 1.2

    asyncio.run(scenario())


def test_final_window_close_without_position_waits_then_writes_settlement(monkeypatch) -> None:
    async def scenario() -> None:
        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper", "--windows", "1"]))
        logger = DummyLogger()
        loop = bot_loop.LoopRuntime()
        calls = []
        sleeps = []
        monkeypatch.setattr("new_poly.bot_loop.fetch_crypto_price_api", lambda window: calls.append(window.slug) or {
            "openPrice": 100.0,
            "closePrice": 99.0,
            "completed": True,
            "incomplete": False,
            "cached": True,
        })

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        monkeypatch.setattr("new_poly.bot_loop.asyncio.sleep", fake_sleep)

        result = await bot_loop._handle_window_close(
            window=_window("m1"),
            prices=WindowPrices(k_price=100.0),
            cfg=options.config,
            options=options,
            feeds=FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream()),
            state=StrategyState(),
            loop=loop,
            logger=logger,
            series=object(),
            dynamic_state=None,
            dynamic_task=None,
        )

        assert result.should_stop is True
        assert sleeps == [90.0]
        assert calls == ["m1"]
        assert logger.rows[-1]["event"] == "window_settlement"
        assert logger.rows[-1]["winning_side"] == "down"
        assert logger.rows[-1]["settlement_close_price"] == 99.0

    asyncio.run(scenario())


def test_final_window_close_with_open_position_waits_then_writes_position_settlement(monkeypatch) -> None:
    async def scenario() -> None:
        options = build_runtime_options(build_arg_parser().parse_args(["--mode", "paper", "--windows", "1"]))
        logger = DummyLogger()
        loop = bot_loop.LoopRuntime()
        state = StrategyState(current_market_slug="m1")
        state.record_entry(PositionSnapshot(
            market_slug="m1",
            token_side="down",
            token_id="m1-down",
            entry_time=120.0,
            entry_avg_price=0.25,
            filled_shares=4.0,
            entry_model_prob=0.70,
            entry_edge=0.30,
        ))
        calls = []
        sleeps = []
        monkeypatch.setattr("new_poly.bot_loop.fetch_crypto_price_api", lambda window: calls.append(window.slug) or {
            "openPrice": 100.0,
            "closePrice": 99.0,
            "completed": True,
            "incomplete": False,
            "cached": True,
        })

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        monkeypatch.setattr("new_poly.bot_loop.asyncio.sleep", fake_sleep)

        result = await bot_loop._handle_window_close(
            window=_window("m1"),
            prices=WindowPrices(k_price=100.0),
            cfg=options.config,
            options=options,
            feeds=FeedContext(binance=None, coinbase=None, polymarket=None, stream=DummyStream()),
            state=state,
            loop=loop,
            logger=logger,
            series=object(),
            dynamic_state=None,
            dynamic_task=None,
        )

        assert result.should_stop is True
        assert sleeps == [90.0]
        assert calls == ["m1"]
        assert logger.rows[-1]["event"] == "settlement"
        assert logger.rows[-1]["winning_side"] == "down"
        assert logger.rows[-1]["settlement_pnl"] == 3.0
        assert logger.rows[-1]["realized_pnl"] == 3.0
        assert state.realized_pnl == 3.0

    asyncio.run(scenario())


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


def test_reference_distance_cap_skip_log_is_deduped_per_window_side() -> None:
    from new_poly.bot_runtime import _should_write_row

    seen: set[tuple[str, str]] = set()
    row = {
        "event": "tick",
        "market_slug": "m1",
        "decision": {
            "action": "skip",
            "reason": "poly_reference_distance_too_high",
            "side": "up",
        },
    }
    same_side = {
        "event": "tick",
        "market_slug": "m1",
        "decision": {
            "action": "skip",
            "reason": "poly_reference_distance_too_high",
            "side": "up",
        },
    }
    other_side = {
        "event": "tick",
        "market_slug": "m1",
        "decision": {
            "action": "skip",
            "reason": "poly_reference_distance_too_high",
            "side": "down",
        },
    }

    assert _should_write_row(row, seen) is True
    assert _should_write_row(same_side, seen) is False
    assert _should_write_row(other_side, seen) is True
