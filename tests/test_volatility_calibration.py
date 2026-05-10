from __future__ import annotations

import pytest

from new_poly.backtest.volatility_calibration import (
    CalibrationConfig,
    evaluate_lookbacks,
)


BASE_MS = 1_778_371_200_000


def _kline(open_time_ms: int, price: float, *, high: float | None = None, low: float | None = None) -> list:
    return [
        open_time_ms,
        str(price),
        str(high if high is not None else price),
        str(low if low is not None else price),
        str(price),
        "0",
        open_time_ms + 59_999,
    ]


def _row(slug: str, ts: str, age: int, s_price: float, k_price: float) -> dict:
    return {
        "market_slug": slug,
        "ts": ts,
        "age_sec": age,
        "remaining_sec": 300 - age,
        "s_price": s_price,
        "k_price": k_price,
    }


def test_evaluate_lookbacks_recomputes_probability_metrics_from_past_klines() -> None:
    rows = [
        _row("m1", "2026-05-10T00:04:00+00:00", 240, 101.0, 100.0),
        _row("m2", "2026-05-10T00:09:00+00:00", 240, 99.0, 100.0),
    ]
    settlement_by_market = {"m1": 101.5, "m2": 100.5}
    klines = [
        _kline(BASE_MS + 0, 100.0),
        _kline(BASE_MS + 60_000, 101.0),
        _kline(BASE_MS + 120_000, 99.0),
        _kline(BASE_MS + 180_000, 101.0),
        _kline(BASE_MS + 240_000, 101.5),
        _kline(BASE_MS + 300_000, 101.0),
        _kline(BASE_MS + 360_000, 100.0),
        _kline(BASE_MS + 420_000, 99.0),
        _kline(BASE_MS + 480_000, 98.5),
        _kline(BASE_MS + 540_000, 100.5),
    ]

    results = evaluate_lookbacks(
        rows,
        klines,
        [1, 3],
        config=CalibrationConfig(
            ewma_half_life_minutes=2.0,
            floor_annual=0.01,
            cap_annual=10.0,
            probability_floor=1e-6,
            buckets=((0.0, 0.5), (0.5, 1.0)),
        ),
        settlement_by_market=settlement_by_market,
    )

    one_min = results[1]
    three_min = results[3]
    assert one_min["samples"] == 2
    assert three_min["samples"] == 2
    assert one_min["brier"] != pytest.approx(three_min["brier"])
    assert one_min["log_loss"] != pytest.approx(three_min["log_loss"])
    assert sum(bucket["samples"] for bucket in one_min["buckets"]) == 2
    assert one_min["high_confidence"]["0.60"]["samples"] >= 1


def test_evaluate_lookbacks_uses_last_window_row_as_settlement_when_missing_map() -> None:
    rows = [
        _row("m1", "2026-05-10T00:04:00+00:00", 240, 101.0, 100.0),
        _row("m1", "2026-05-10T00:04:59+00:00", 299, 99.0, 100.0),
    ]
    klines = [_kline(BASE_MS + i * 60_000, 100.0 + i) for i in range(6)]

    results = evaluate_lookbacks(
        rows,
        klines,
        [3],
        config=CalibrationConfig(floor_annual=0.01, cap_annual=10.0),
    )

    assert results[3]["samples"] == 2
    assert results[3]["outcome_up_rate"] == 0.0
