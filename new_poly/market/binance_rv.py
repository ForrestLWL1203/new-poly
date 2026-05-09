"""Binance short-horizon realized volatility helpers."""

from __future__ import annotations

import json
import math
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Sequence


BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
MINUTES_PER_YEAR = 365 * 24 * 60


@dataclass(frozen=True)
class BinanceRvSnapshot:
    source: str
    symbol: str
    sigma: float | None
    close_sigma_annual: float | None
    parkinson_sigma_annual: float | None
    lookback_minutes: int
    candles: int
    timestamp_ms: int | None
    fetched_at: float

    @property
    def age_sec(self) -> float | None:
        if self.timestamp_ms is None:
            return None
        return max(0.0, self.fetched_at - self.timestamp_ms / 1000.0)

    def to_json(self) -> dict[str, float | int | str | None]:
        return {
            "source": self.source,
            "symbol": self.symbol,
            "sigma": round(self.sigma, 6) if self.sigma is not None else None,
            "close_sigma_annual": round(self.close_sigma_annual, 6) if self.close_sigma_annual is not None else None,
            "parkinson_sigma_annual": round(self.parkinson_sigma_annual, 6) if self.parkinson_sigma_annual is not None else None,
            "lookback_minutes": self.lookback_minutes,
            "candles": self.candles,
            "timestamp_ms": self.timestamp_ms,
            "age_sec": round(self.age_sec, 3) if self.age_sec is not None else None,
        }


def _float_at(row: Sequence[Any], index: int) -> float | None:
    try:
        value = float(row[index])
    except (TypeError, ValueError, IndexError):
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return value


def _annualize_1m_sigma(value: float | None) -> float | None:
    if value is None:
        return None
    return value * math.sqrt(MINUTES_PER_YEAR)


def _ewma(values: Sequence[float], *, half_life_minutes: float) -> float | None:
    if not values:
        return None
    half_life = max(1e-9, float(half_life_minutes))
    decay = math.log(2.0) / half_life
    # Values are oldest -> newest; newest gets the largest weight.
    weighted = 0.0
    total_weight = 0.0
    count = len(values)
    for idx, value in enumerate(values):
        age = count - idx - 1
        weight = math.exp(-decay * age)
        weighted += weight * value
        total_weight += weight
    if total_weight <= 0:
        return None
    return weighted / total_weight


def _clamp_sigma(value: float | None, *, floor_annual: float, cap_annual: float) -> float | None:
    if value is None or not math.isfinite(value) or value <= 0:
        return None
    floor = max(0.0, float(floor_annual))
    cap = max(floor, float(cap_annual))
    return min(cap, max(floor, value))


def compute_binance_rv_sigma_from_klines(
    klines: Sequence[Sequence[Any]],
    *,
    ewma_half_life_minutes: float = 10.0,
    floor_annual: float = 0.20,
    cap_annual: float = 2.50,
    symbol: str = "BTCUSDT",
    fetched_at: float | None = None,
) -> BinanceRvSnapshot:
    fetched = time.time() if fetched_at is None else float(fetched_at)
    closes: list[float] = []
    parkinson_vars: list[float] = []
    last_close_time: int | None = None
    for row in klines:
        high = _float_at(row, 2)
        low = _float_at(row, 3)
        close = _float_at(row, 4)
        if close is not None:
            closes.append(close)
        if high is not None and low is not None and high >= low:
            parkinson_vars.append((math.log(high / low) ** 2) / (4.0 * math.log(2.0)))
        try:
            last_close_time = int(row[6])
        except (TypeError, ValueError, IndexError):
            pass

    returns: list[float] = []
    for previous, current in zip(closes, closes[1:]):
        if previous > 0 and current > 0:
            returns.append(math.log(current / previous))

    close_var = _ewma([value * value for value in returns], half_life_minutes=ewma_half_life_minutes)
    parkinson_var = _ewma(parkinson_vars[-len(returns):], half_life_minutes=ewma_half_life_minutes) if returns else None
    close_sigma = _annualize_1m_sigma(math.sqrt(close_var)) if close_var is not None else None
    parkinson_sigma = _annualize_1m_sigma(math.sqrt(parkinson_var)) if parkinson_var is not None else None
    candidates = [value for value in (close_sigma, parkinson_sigma) if value is not None and math.isfinite(value)]
    sigma = _clamp_sigma(max(candidates) if candidates else None, floor_annual=floor_annual, cap_annual=cap_annual)
    lookback = max(0, len(returns))
    return BinanceRvSnapshot(
        source="binance_1m_rv",
        symbol=symbol.upper(),
        sigma=sigma,
        close_sigma_annual=_clamp_sigma(close_sigma, floor_annual=floor_annual, cap_annual=cap_annual),
        parkinson_sigma_annual=_clamp_sigma(parkinson_sigma, floor_annual=floor_annual, cap_annual=cap_annual),
        lookback_minutes=lookback,
        candles=len(klines),
        timestamp_ms=last_close_time,
        fetched_at=fetched,
    )


def fetch_binance_rv_snapshot(
    *,
    symbol: str = "BTCUSDT",
    lookback_minutes: int = 60,
    ewma_half_life_minutes: float = 10.0,
    floor_annual: float = 0.20,
    cap_annual: float = 2.50,
    timeout_sec: float = 5.0,
) -> BinanceRvSnapshot:
    fetched_at = time.time()
    # Need N+1 closes to produce N close-to-close returns.
    limit = max(2, min(1000, int(lookback_minutes) + 1))
    url = BINANCE_KLINES_URL + "?" + urllib.parse.urlencode({
        "symbol": symbol.upper(),
        "interval": "1m",
        "limit": limit,
    })
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "new-poly/0.1"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not isinstance(payload, list):
            raise ValueError("unexpected Binance kline response")
        return compute_binance_rv_sigma_from_klines(
            payload,
            ewma_half_life_minutes=ewma_half_life_minutes,
            floor_annual=floor_annual,
            cap_annual=cap_annual,
            symbol=symbol,
            fetched_at=fetched_at,
        )
    except Exception:
        return BinanceRvSnapshot(
            source="binance_1m_rv",
            symbol=symbol.upper(),
            sigma=None,
            close_sigma_annual=None,
            parkinson_sigma_annual=None,
            lookback_minutes=0,
            candles=0,
            timestamp_ms=None,
            fetched_at=fetched_at,
        )
