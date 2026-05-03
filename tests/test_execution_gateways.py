from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from new_poly.trading.execution import (
    ExecutionConfig,
    LiveFakExecutionGateway,
    PaperExecutionGateway,
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


def test_live_gateway_requires_explicit_risk_ack() -> None:
    with pytest.raises(ValueError, match="i-understand-live-risk"):
        LiveFakExecutionGateway(live_risk_ack=False)


def test_live_buy_hint_buffers_depth_limit_price(monkeypatch) -> None:
    captured = {}

    class Gateway(LiveFakExecutionGateway):
        def _post(self, token_id, amount, side, price_hint):
            captured["price_hint"] = price_hint
            return "posted"

    gateway = Gateway(live_risk_ack=True)
    result = asyncio.run(gateway.buy("up", amount_usd=5.0, max_price=0.55, best_ask=0.50))

    assert result == "posted"
    assert captured["price_hint"] == 0.551


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
