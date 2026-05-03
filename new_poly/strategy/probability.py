"""Binary-option probability helpers for BTC UP/DOWN markets."""

from __future__ import annotations

import math
from dataclasses import dataclass

SECONDS_PER_YEAR = 31_536_000.0


@dataclass(frozen=True)
class BinaryProbabilities:
    up: float
    down: float
    d2: float | None


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def binary_probability(s_price: float, k_price: float, sigma: float, seconds_left: float) -> float:
    """Return the simplified Black-Scholes ``N(d2)`` probability."""
    if seconds_left <= 0:
        return 1.0 if s_price > k_price else 0.0
    if s_price <= 0 or k_price <= 0 or sigma <= 0:
        return 0.5

    t_years = max(seconds_left, 0.1) / SECONDS_PER_YEAR
    denom = sigma * math.sqrt(t_years)
    if denom <= 0:
        return 1.0 if s_price > k_price else 0.0
    d2 = (math.log(s_price / k_price) - 0.5 * sigma * sigma * t_years) / denom
    return max(0.0, min(1.0, normal_cdf(d2)))


def binary_probabilities(s_price: float, k_price: float, sigma: float, seconds_left: float) -> BinaryProbabilities:
    up = binary_probability(s_price, k_price, sigma, seconds_left)
    d2 = None
    if seconds_left > 0 and s_price > 0 and k_price > 0 and sigma > 0:
        t_years = max(seconds_left, 0.1) / SECONDS_PER_YEAR
        d2 = (math.log(s_price / k_price) - 0.5 * sigma * sigma * t_years) / (sigma * math.sqrt(t_years))
    return BinaryProbabilities(up=up, down=1.0 - up, d2=d2)

