from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.trading import fak_quotes
from new_poly.trading.execution import (
    BuyRetryParams,
    ExecutionConfig,
    ExecutionResult,
    LiveFakExecutionGateway,
    PaperExecutionGateway,
    SellRetryParams,
)


class FakeStream:
    def __init__(self) -> None:
        self.asks = {"up": [(0.50, 10.0)], "down": [(0.45, 10.0)]}
        self.bids = {"up": [(0.48, 10.0)], "down": [(0.43, 10.0)]}

    def get_latest_ask_levels_with_size(self, token_id, max_age_sec=None):
        return self.asks.get(token_id, [])

    def get_latest_bid_levels_with_size(self, token_id, max_age_sec=None):
        return self.bids.get(token_id, [])

    def get_latest_best_ask(self, token_id, max_age_sec=None, level=1):
        levels = self.asks.get(token_id, [])
        return levels[level - 1][0] if len(levels) >= level else None

    def get_latest_best_bid(self, token_id, max_age_sec=None, level=1):
        levels = self.bids.get(token_id, [])
        return levels[level - 1][0] if len(levels) >= level else None

    def get_latest_best_ask_age(self, token_id, level=1):
        return 0.01

    def get_latest_best_bid_age(self, token_id, level=1):
        return 0.01


class SequencedLiveGateway(LiveFakExecutionGateway):
    def __init__(self, responses, *, retry_interval_sec=0.0, **kwargs):
        super().__init__(live_risk_ack=True, retry_interval_sec=retry_interval_sec, **kwargs)
        self.responses = list(responses)
        self.calls = []

    def _post(self, token_id, amount, side, price_hint):
        self.calls.append((token_id, amount, side, price_hint))
        return self.responses.pop(0)


class BatchClient:
    def __init__(self):
        self.market_orders = []
        self.posted_batches = []

    def create_market_order(self, args, options=None):
        self.market_orders.append(args)
        return {"amount": args.amount, "price": args.price}

    def post_orders(self, batch):
        self.posted_batches.append(batch)
        return [
            {
                "orderID": f"ord-{index}",
                "success": True,
                "status": "matched",
                "makingAmount": str(order["order"]["amount"]),
                "takingAmount": str(order["order"]["amount"] * order["order"]["price"]),
            }
            for index, order in enumerate(batch)
        ]


def test_paper_buy_and_sell_use_depth_after_delay() -> None:
    async def scenario() -> None:
        stream = FakeStream()

        async def mutate_after_signal():
            stream.asks["up"] = [(0.55, 10.0)]
            stream.bids["up"] = [(0.51, 10.0)]

        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(paper_latency_sec=0.0, depth_notional=5.0),
            before_fill=mutate_after_signal,
        )

        buy = await gateway.buy("up", amount_usd=5.0, max_price=0.60)
        assert buy.success is True
        assert buy.avg_price == 0.55
        assert round(buy.filled_size, 6) == round(5.0 / 0.55, 6)

        sell = await gateway.sell("up", shares=buy.filled_size, min_price=0.20)
        assert sell.success is True
        assert sell.avg_price == 0.51

    asyncio.run(scenario())


def test_paper_depth_shortfall_no_fill() -> None:
    async def scenario() -> None:
        stream = FakeStream()
        stream.asks["up"] = [(0.5, 1.0)]
        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(paper_latency_sec=0.0, depth_notional=5.0),
        )

        result = await gateway.buy("up", amount_usd=5.0, max_price=0.60)
        assert result.success is False
        assert result.filled_size == 0.0

    asyncio.run(scenario())


def test_paper_sell_uses_live_style_exit_floor(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)

    async def scenario() -> None:
        stream = FakeStream()
        stream.bids["up"] = [(0.36, 10.0)]
        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(paper_latency_sec=0.0, sell_price_buffer_ticks=4, sell_retry_price_buffer_ticks=5),
        )

        result = await gateway.sell("up", shares=10.0, min_price=0.40, exit_reason="logic_decay_exit")

        assert result.success is True
        assert result.avg_price == 0.36

    asyncio.run(scenario())


def test_live_gateway_requires_explicit_risk_ack() -> None:
    with pytest.raises(ValueError, match="i-understand-live-risk"):
        LiveFakExecutionGateway(live_risk_ack=False)


def test_buy_hint_uses_clob_tick_size(monkeypatch) -> None:
    monkeypatch.setattr(fak_quotes, "get_tick_size", lambda token_id: 0.01)

    hint = fak_quotes.buffer_buy_price_hint("up", best_ask=0.50, buffer_ticks=1, max_price=0.55)

    assert hint == 0.51


def test_live_buy_hint_buffers_best_ask_but_caps_at_depth_limit(monkeypatch) -> None:
    monkeypatch.setattr(fak_quotes, "get_tick_size", lambda token_id: 0.01)
    captured = {}

    class Gateway(LiveFakExecutionGateway):
        def _post(self, token_id, amount, side, price_hint):
            captured["price_hint"] = price_hint
            return ExecutionResult(True, filled_size=10.0, avg_price=price_hint, message="posted", mode="live")

    gateway = Gateway(live_risk_ack=True, buy_dynamic_buffer_enabled=False)
    result = asyncio.run(gateway.buy("up", amount_usd=5.0, max_price=0.55, best_ask=0.50))

    assert result.success is True
    assert captured["price_hint"] == 0.52


def test_live_buy_hint_buffers_depth_limit_when_provided(monkeypatch) -> None:
    monkeypatch.setattr(fak_quotes, "get_tick_size", lambda token_id: 0.01)
    captured = {}

    class Gateway(LiveFakExecutionGateway):
        def _post(self, token_id, amount, side, price_hint):
            captured["price_hint"] = price_hint
            return ExecutionResult(True, filled_size=10.0, avg_price=price_hint, message="posted", mode="live")

    gateway = Gateway(live_risk_ack=True, buy_dynamic_buffer_enabled=False)
    result = asyncio.run(
        gateway.buy("up", amount_usd=5.0, max_price=0.56, best_ask=0.50, price_hint_base=0.54)
    )

    assert result.success is True
    assert captured["price_hint"] == 0.56


def test_live_buy_hint_never_exceeds_depth_limit(monkeypatch) -> None:
    monkeypatch.setattr(fak_quotes, "get_tick_size", lambda token_id: 0.01)
    captured = {}

    class Gateway(LiveFakExecutionGateway):
        def _post(self, token_id, amount, side, price_hint):
            captured["price_hint"] = price_hint
            return ExecutionResult(True, filled_size=10.0, avg_price=price_hint, message="posted", mode="live")

    gateway = Gateway(live_risk_ack=True, buy_dynamic_buffer_enabled=False)
    result = asyncio.run(gateway.buy("up", amount_usd=5.0, max_price=0.55, best_ask=0.55))

    assert result.success is True
    assert captured["price_hint"] == 0.55


def test_live_buy_dynamic_buffer_uses_fair_room_without_spending_reserved_edge(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(False, message="UNMATCHED", mode="live"),
        ExecutionResult(True, filled_size=10.0, avg_price=0.248, message="MATCHED", mode="live"),
    ])

    result = asyncio.run(gateway.buy("down", amount_usd=1.0, max_price=0.29, best_ask=0.17))

    assert result.success is True
    assert gateway.calls[0][3] == pytest.approx(0.22)
    assert gateway.calls[1][3] == pytest.approx(0.248)


def test_live_buy_dynamic_buffer_clamps_when_fair_room_is_tight(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(True, filled_size=10.0, avg_price=0.485355, message="MATCHED", mode="live"),
    ])

    result = asyncio.run(gateway.buy("down", amount_usd=1.0, max_price=0.485355, best_ask=0.47))

    assert result.success is True
    assert gateway.calls[0][3] == pytest.approx(0.47)


def test_live_buy_retry_refreshes_signal_before_second_post(monkeypatch) -> None:
    monkeypatch.setattr(fak_quotes, "get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(False, message="UNMATCHED", mode="live"),
        ExecutionResult(True, filled_size=10.0, avg_price=0.55, message="MATCHED", mode="live"),
    ], buy_dynamic_buffer_enabled=False)

    async def refresh_retry(attempt):
        assert attempt == 1
        return BuyRetryParams(max_price=0.57, best_ask=0.54, price_hint_base=0.55)

    result = asyncio.run(
        gateway.buy(
            "up",
            amount_usd=5.0,
            max_price=0.60,
            best_ask=0.50,
            price_hint_base=0.50,
            retry_refresh=refresh_retry,
        )
    )

    assert result.success is True
    assert len(gateway.calls) == 2
    assert gateway.calls[0][3] == 0.52
    assert gateway.calls[1][3] == 0.57
    assert result.attempt == 2
    assert result.total_latency_ms is not None


def test_live_buy_retry_skips_when_signal_refresh_fails(monkeypatch) -> None:
    monkeypatch.setattr(fak_quotes, "get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(False, message="UNMATCHED", mode="live"),
    ])

    async def refresh_retry(attempt):
        return None

    result = asyncio.run(
        gateway.buy(
            "up",
            amount_usd=5.0,
            max_price=0.60,
            best_ask=0.50,
            price_hint_base=0.50,
            retry_refresh=refresh_retry,
        )
    )

    assert result.success is False
    assert len(gateway.calls) == 1
    assert result.attempt == 1
    assert "retry skipped" in result.message


def test_live_buy_retry_can_use_configured_four_tick_buffer(monkeypatch) -> None:
    monkeypatch.setattr(fak_quotes, "get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(False, message="UNMATCHED", mode="live"),
        ExecutionResult(True, filled_size=10.0, avg_price=0.58, message="MATCHED", mode="live"),
    ], buy_dynamic_buffer_enabled=False)
    gateway.buy_price_buffer_ticks = 2.0
    gateway.buy_retry_price_buffer_ticks = 4.0

    result = asyncio.run(
        gateway.buy("up", amount_usd=5.0, max_price=0.60, best_ask=0.50, price_hint_base=0.54)
    )

    assert result.success is True
    assert gateway.calls[0][3] == 0.56
    assert gateway.calls[1][3] == 0.58


def test_live_sell_retry_reposts_same_floor_price(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_token_balance", lambda token_id, safe=True: 10.0)
    monkeypatch.setattr(fak_quotes, "get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(False, message="UNMATCHED", mode="live"),
        ExecutionResult(True, filled_size=10.0, avg_price=0.40, message="MATCHED", mode="live"),
    ])

    result = asyncio.run(gateway.sell("up", shares=10.0, min_price=0.40))

    assert result.success is True
    assert len(gateway.calls) == 2
    assert gateway.calls[0][3] == 0.40
    assert gateway.calls[1][3] == 0.40
    assert result.attempt == 2
    assert result.total_latency_ms is not None


def test_live_sell_profit_exit_uses_configured_aggressive_retry(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_token_balance", lambda token_id, safe=True: 10.0)
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(False, message="UNMATCHED", mode="live"),
        ExecutionResult(True, filled_size=10.0, avg_price=0.39, message="MATCHED", mode="live"),
    ])

    result = asyncio.run(gateway.sell("up", shares=10.0, min_price=0.40, exit_reason="defensive_take_profit"))

    assert result.success is True
    assert gateway.calls[0][3] == 0.35
    assert gateway.calls[1][3] == 0.34


def test_live_sell_logic_decay_starts_below_bid_limit(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_token_balance", lambda token_id, safe=True: 10.0)
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(False, message="UNMATCHED", mode="live"),
        ExecutionResult(True, filled_size=10.0, avg_price=0.36, message="MATCHED", mode="live"),
    ])

    result = asyncio.run(gateway.sell("up", shares=10.0, min_price=0.40, exit_reason="logic_decay_exit"))

    assert result.success is True
    assert gateway.calls[0][3] == 0.35
    assert gateway.calls[1][3] == 0.34


def test_live_sell_polymarket_divergence_uses_configured_aggressive_retry(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_token_balance", lambda token_id, safe=True: 10.0)
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(False, message="UNMATCHED", mode="live"),
        ExecutionResult(True, filled_size=10.0, avg_price=0.36, message="MATCHED", mode="live"),
    ])

    result = asyncio.run(gateway.sell("up", shares=10.0, min_price=0.40, exit_reason="polymarket_divergence_exit"))

    assert result.success is True
    assert gateway.calls[0][3] == 0.35
    assert gateway.calls[1][3] == 0.34


def test_live_sell_retry_refreshes_exit_floor_before_second_post(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_token_balance", lambda token_id, safe=True: 10.0)
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(False, message="UNMATCHED", mode="live"),
        ExecutionResult(True, filled_size=10.0, avg_price=0.45, message="MATCHED", mode="live"),
    ])

    async def refresh_retry(attempt):
        assert attempt == 1
        return SellRetryParams(min_price=0.50, exit_reason="logic_decay_exit")

    result = asyncio.run(
        gateway.sell(
            "up",
            shares=10.0,
            min_price=0.40,
            exit_reason="logic_decay_exit",
            retry_refresh=refresh_retry,
        )
    )

    assert result.success is True
    assert gateway.calls[0][3] == 0.35
    assert gateway.calls[1][3] == 0.44


def test_live_batch_sell_posts_multiple_fak_slices(monkeypatch) -> None:
    client = BatchClient()
    monkeypatch.setattr("new_poly.trading.execution.get_client", lambda: client)
    monkeypatch.setattr("new_poly.trading.execution.get_order_options", lambda token_id: None)
    monkeypatch.setattr("new_poly.trading.execution.get_token_balance", lambda token_id, safe=True: 100.0)
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)
    gateway = LiveFakExecutionGateway(
        live_risk_ack=True,
        batch_exit_enabled=True,
        batch_exit_min_shares=20.0,
        batch_exit_slices=(0.4, 0.3, 1.0),
        batch_exit_extra_buffer_ticks=(0.0, 3.0, 6.0),
    )

    result = asyncio.run(gateway.sell("up", shares=100.0, min_price=0.38, exit_reason="logic_decay_exit"))

    assert result.success is True
    assert result.filled_size == pytest.approx(100.0)
    assert result.avg_price == pytest.approx((40 * 0.33 + 30 * 0.30 + 30 * 0.27) / 100)
    assert len(client.posted_batches) == 1
    assert [order.amount for order in client.market_orders] == [40.0, 30.0, 30.0]
    assert [order.price for order in client.market_orders] == [0.33, 0.30, 0.27]


def test_live_sell_final_force_uses_emergency_ladder(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_token_balance", lambda token_id, safe=True: 10.0)
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(False, message="UNMATCHED", mode="live"),
        ExecutionResult(True, filled_size=10.0, avg_price=0.30, message="MATCHED", mode="live"),
    ])

    result = asyncio.run(gateway.sell("up", shares=10.0, min_price=0.40, exit_reason="final_force_exit"))

    assert result.success is True
    assert gateway.calls[0][3] == 0.35
    assert gateway.calls[1][3] == 0.30


def test_live_sell_price_hint_never_goes_below_one_tick(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_token_balance", lambda token_id, safe=True: 10.0)
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)
    gateway = SequencedLiveGateway([
        ExecutionResult(False, message="UNMATCHED", mode="live"),
        ExecutionResult(False, message="UNMATCHED", mode="live"),
    ])

    result = asyncio.run(gateway.sell("up", shares=10.0, min_price=0.03, exit_reason="final_force_exit"))

    assert result.success is False
    assert gateway.calls[0][3] == 0.01
    assert gateway.calls[1][3] == 0.01


def test_live_sell_no_balance_is_not_an_order_attempt(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_token_balance", lambda token_id, safe=True: 0.0)
    gateway = SequencedLiveGateway([])

    result = asyncio.run(gateway.sell("up", shares=10.0, min_price=0.40))

    assert result.success is False
    assert result.attempt == 0
    assert result.total_latency_ms == 0
    assert result.fatal_stop_reason == "live_no_sellable_balance"
    assert gateway.calls == []


def test_live_sell_dust_shares_are_not_posted(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_token_balance", lambda token_id, safe=True: 0.005787)
    gateway = SequencedLiveGateway([], live_min_sell_shares=0.01)

    result = asyncio.run(gateway.sell("up", shares=0.005787, min_price=0.64, exit_reason="logic_decay_exit"))

    assert result.success is False
    assert result.attempt == 0
    assert result.message == "live dust sell skipped: shares below minimum"
    assert result.timing["dust_shares"] == pytest.approx(0.005787)
    assert result.timing["min_live_sell_shares"] == pytest.approx(0.01)
    assert gateway.calls == []


def test_live_post_matched_without_fill_fields_is_success(monkeypatch) -> None:
    class Client:
        def create_market_order(self, args, options=None):
            return {"signed": True}

        def post_order(self, signed, order_type):
            return {
                "orderID": "ord-matched",
                "success": True,
                "status": "MATCHED",
                "takingAmount": "1.6667",
                "makingAmount": "1.0",
            }

    monkeypatch.setattr("new_poly.trading.execution.get_client", lambda: Client())
    monkeypatch.setattr("new_poly.trading.execution.get_order_options", lambda token_id: None)
    gateway = LiveFakExecutionGateway(live_risk_ack=True)

    result = gateway._post("up", 1.0, "BUY", 0.60)

    assert result.success is True
    assert result.order_id == "ord-matched"
    assert result.filled_size == pytest.approx(1.6667)
    assert result.avg_price == pytest.approx(1.0 / 1.6667)


def test_live_post_success_status_is_not_treated_as_matched(monkeypatch) -> None:
    class Client:
        def create_market_order(self, args, options=None):
            return {"signed": True}

        def post_order(self, signed, order_type):
            return {
                "orderID": "ord-success",
                "success": True,
                "status": "SUCCESS",
            }

    monkeypatch.setattr("new_poly.trading.execution.get_client", lambda: Client())
    monkeypatch.setattr("new_poly.trading.execution.get_order_options", lambda token_id: None)
    gateway = LiveFakExecutionGateway(live_risk_ack=True)

    result = gateway._post("up", 1.0, "BUY", 0.50)

    assert result.success is False
    assert result.order_id == "ord-success"
    assert result.filled_size == 0.0
    assert result.avg_price == 0.0


def test_live_post_does_not_treat_fill_fields_as_success_without_matched_status(monkeypatch) -> None:
    class Client:
        def create_market_order(self, args, options=None):
            return {"signed": True}

        def post_order(self, signed, order_type):
            return {
                "orderID": "ord-unmatched",
                "success": False,
                "status": "UNMATCHED",
                "sizeFilled": "1.0",
                "avgPrice": "0.50",
            }

    monkeypatch.setattr("new_poly.trading.execution.get_client", lambda: Client())
    monkeypatch.setattr("new_poly.trading.execution.get_order_options", lambda token_id: None)
    gateway = LiveFakExecutionGateway(live_risk_ack=True)

    result = gateway._post("up", 1.0, "BUY", 0.50)

    assert result.success is False
    assert result.filled_size == pytest.approx(1.0)
    assert result.avg_price == pytest.approx(0.50)


def test_live_post_no_match_exception_is_no_fill_with_latency(monkeypatch) -> None:
    class Client:
        def create_market_order(self, args, options=None):
            return {"signed": True}

        def post_order(self, signed, order_type):
            raise RuntimeError(
                "PolyApiException[status_code=400, error_message={'error': "
                "'no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found.', "
                "'orderID': '0xabc'}]"
            )

    monkeypatch.setattr("new_poly.trading.execution.get_client", lambda: Client())
    monkeypatch.setattr("new_poly.trading.execution.get_order_options", lambda token_id: None)
    gateway = LiveFakExecutionGateway(live_risk_ack=True)

    result = gateway._post("up", 1.0, "BUY", 0.50)

    assert result.success is False
    assert result.order_id == "0xabc"
    assert "no match" in result.message
    assert result.latency_ms is not None
    assert result.total_latency_ms == result.latency_ms


def test_paper_buy_uses_depth_limit_not_average() -> None:
    async def scenario() -> None:
        stream = FakeStream()
        stream.asks["up"] = [(0.50, 5.0), (0.55, 10.0)]
        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(paper_latency_sec=0.0, depth_notional=5.0),
        )

        result = await gateway.buy("up", amount_usd=5.0, max_price=0.55)
        assert result.success is True
        assert round(result.avg_price, 6) == 0.52381

    asyncio.run(scenario())


def test_paper_buy_records_compact_timing_telemetry() -> None:
    async def scenario() -> None:
        stream = FakeStream()
        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(paper_latency_sec=0.0, retry_interval_sec=0.0),
        )

        result = await gateway.buy("up", amount_usd=5.0, max_price=0.60)

        assert result.success is True
        assert result.timing["paper_configured_latency_ms"] == 0
        assert "paper_actual_sleep_ms" in result.timing
        assert "book_read_ms" in result.timing
        assert result.timing["attempts"] == 1
        assert result.timing["total_latency_ms"] == result.total_latency_ms

    asyncio.run(scenario())


def test_live_post_records_detailed_timing(monkeypatch) -> None:
    class Client:
        def create_market_order(self, args, options=None):
            return {"signed": True}

        def post_order(self, signed, order_type):
            return {
                "orderID": "ord-matched",
                "success": True,
                "status": "MATCHED",
                "takingAmount": "1.6667",
                "makingAmount": "1.0",
            }

    monkeypatch.setattr("new_poly.trading.execution.get_client", lambda: Client())
    monkeypatch.setattr("new_poly.trading.execution.get_order_options", lambda token_id: None)
    gateway = LiveFakExecutionGateway(live_risk_ack=True)

    result = gateway._post("up", 1.0, "BUY", 0.60)

    assert result.success is True
    assert result.timing["create_order_ms"] >= 0
    assert result.timing["post_order_ms"] >= 0
    assert result.timing["wall_latency_ms"] >= 0
    assert result.timing["response_at_epoch_ms"] >= result.timing["sent_at_epoch_ms"]


def test_paper_retry_uses_one_latency_plus_retry_interval() -> None:
    async def scenario() -> None:
        stream = FakeStream()
        stream.asks["up"] = [(0.70, 10.0)]
        calls = 0

        async def mutate_after_signal():
            nonlocal calls
            calls += 1
            if calls == 2:
                stream.asks["up"] = [(0.55, 10.0)]

        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(
                paper_latency_sec=0.01,
                retry_interval_sec=0.01,
                retry_count=1,
                depth_notional=5.0,
            ),
            before_fill=mutate_after_signal,
        )

        result = await gateway.buy("up", amount_usd=5.0, max_price=0.60)
        assert result.success is True
        assert result.attempt == 2
        assert result.total_latency_ms is not None
        assert result.total_latency_ms < 35

    asyncio.run(scenario())


def test_paper_buy_retry_uses_refreshed_signal_params() -> None:
    async def scenario() -> None:
        stream = FakeStream()
        stream.asks["up"] = [(0.58, 10.0)]
        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(paper_latency_sec=0.0, retry_interval_sec=0.0, retry_count=1),
        )

        async def refresh_retry(attempt):
            assert attempt == 1
            return BuyRetryParams(max_price=0.60, best_ask=0.58, price_hint_base=0.58)

        result = await gateway.buy("up", amount_usd=5.0, max_price=0.55, retry_refresh=refresh_retry)

        assert result.success is True
        assert result.attempt == 2
        assert result.avg_price == 0.58

    asyncio.run(scenario())


def test_paper_buy_retry_skips_when_signal_refresh_fails() -> None:
    async def scenario() -> None:
        stream = FakeStream()
        stream.asks["up"] = [(0.58, 10.0)]
        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(paper_latency_sec=0.0, retry_interval_sec=0.0, retry_count=1),
        )

        async def refresh_retry(attempt):
            return None

        result = await gateway.buy("up", amount_usd=5.0, max_price=0.55, retry_refresh=refresh_retry)

        assert result.success is False
        assert result.attempt == 1
        assert "retry skipped" in result.message

    asyncio.run(scenario())


def test_paper_sell_retry_uses_refreshed_exit_floor(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)

    async def scenario() -> None:
        stream = FakeStream()
        stream.bids["up"] = [(0.45, 10.0)]
        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(paper_latency_sec=0.0, retry_interval_sec=0.0, retry_count=1),
        )

        async def refresh_retry(attempt):
            assert attempt == 1
            return SellRetryParams(min_price=0.50, exit_reason="logic_decay_exit")

        result = await gateway.sell(
            "up",
            shares=10.0,
            min_price=0.60,
            exit_reason="logic_decay_exit",
            retry_refresh=refresh_retry,
        )

        assert result.success is True
        assert result.attempt == 2
        assert result.avg_price == 0.45

    asyncio.run(scenario())


def test_paper_sell_uses_local_tick_without_clob_lookup(monkeypatch) -> None:
    def fail_tick_lookup(token_id):
        raise AssertionError("paper sell should not query live CLOB tick size")

    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", fail_tick_lookup)

    async def scenario() -> None:
        stream = FakeStream()
        stream.bids["up"] = [(0.45, 10.0)]
        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(paper_latency_sec=0.0, retry_interval_sec=0.0, retry_count=0),
        )

        result = await gateway.sell("up", shares=10.0, min_price=0.50, exit_reason="logic_decay_exit")

        assert result.success is True
        assert result.avg_price == 0.45
        assert result.total_latency_ms is not None
        assert result.total_latency_ms < 50

    asyncio.run(scenario())


def test_paper_batch_sell_can_partially_exit_large_share_position(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)

    async def scenario() -> None:
        stream = FakeStream()
        stream.bids["up"] = [(0.38, 40.0), (0.32, 30.0), (0.28, 30.0)]
        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(
                paper_latency_sec=0.0,
                retry_count=0,
                batch_exit_enabled=True,
                batch_exit_min_shares=20.0,
                batch_exit_slices=(0.4, 0.3, 1.0),
                batch_exit_extra_buffer_ticks=(0.0, 3.0, 6.0),
            ),
        )

        result = await gateway.sell("up", shares=100.0, min_price=0.38, exit_reason="logic_decay_exit")

        assert result.success is True
        assert result.filled_size == pytest.approx(100.0)
        assert result.avg_price == pytest.approx(0.332)

    asyncio.run(scenario())


def test_paper_sell_retry_skips_when_exit_refresh_fails(monkeypatch) -> None:
    monkeypatch.setattr("new_poly.trading.execution.get_tick_size", lambda token_id: 0.01)

    async def scenario() -> None:
        stream = FakeStream()
        stream.bids["up"] = [(0.45, 10.0)]
        gateway = PaperExecutionGateway(
            stream=stream,
            config=ExecutionConfig(paper_latency_sec=0.0, retry_interval_sec=0.0, retry_count=1),
        )

        async def refresh_retry(attempt):
            return None

        result = await gateway.sell(
            "up",
            shares=10.0,
            min_price=0.60,
            exit_reason="logic_decay_exit",
            retry_refresh=refresh_retry,
        )

        assert result.success is False
        assert result.attempt == 1
        assert "retry skipped" in result.message

    asyncio.run(scenario())
