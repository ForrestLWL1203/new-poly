"""Probability calibration scans for alternative Binance RV lookbacks."""

from __future__ import annotations

import datetime as dt
import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from new_poly.market.binance_rv import compute_binance_rv_sigma_from_klines
from new_poly.strategy.probability import binary_probability


DEFAULT_BUCKETS = tuple((idx / 10.0, (idx + 1) / 10.0) for idx in range(10))
DEFAULT_HIGH_CONFIDENCE_THRESHOLDS = (0.55, 0.60, 0.65, 0.70, 0.75)


@dataclass(frozen=True)
class CalibrationConfig:
    ewma_half_life_minutes: float = 10.0
    floor_annual: float = 0.20
    cap_annual: float = 2.50
    probability_floor: float = 1e-6
    buckets: tuple[tuple[float, float], ...] = DEFAULT_BUCKETS
    high_confidence_thresholds: tuple[float, ...] = DEFAULT_HIGH_CONFIDENCE_THRESHOLDS
    min_age_sec: float | None = None
    max_age_sec: float | None = None


def parse_row_time(row: Mapping[str, Any]) -> float | None:
    value = row.get("ts")
    if not isinstance(value, str) or not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.timestamp()


def _float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _kline_close_time_ms(row: Sequence[Any]) -> int | None:
    try:
        return int(row[6])
    except (TypeError, ValueError, IndexError):
        return None


def _sorted_klines(klines: Iterable[Sequence[Any]]) -> list[Sequence[Any]]:
    return sorted(
        [row for row in klines if _kline_close_time_ms(row) is not None],
        key=lambda row: _kline_close_time_ms(row) or 0,
    )


def _past_kline_slice(
    klines: Sequence[Sequence[Any]],
    *,
    timestamp_sec: float,
    lookback_minutes: int,
) -> list[Sequence[Any]]:
    cutoff_ms = int(timestamp_sec * 1000)
    eligible = [row for row in klines if (_kline_close_time_ms(row) or 0) <= cutoff_ms]
    needed = max(2, int(lookback_minutes) + 1)
    return eligible[-needed:]


def infer_settlement_by_market(rows: Iterable[Mapping[str, Any]]) -> dict[str, float]:
    latest_by_market: dict[str, tuple[float, float]] = {}
    for row in rows:
        slug = str(row.get("market_slug") or "")
        if not slug:
            continue
        ts = parse_row_time(row)
        age = _float(row.get("age_sec"))
        sort_key = ts if ts is not None else (age if age is not None else 0.0)
        settlement_price = _float(row.get("settlement_price"))
        if settlement_price is None:
            settlement_price = _float(row.get("s_price"))
        if settlement_price is None:
            continue
        previous = latest_by_market.get(slug)
        if previous is None or sort_key >= previous[0]:
            latest_by_market[slug] = (sort_key, settlement_price)
    return {slug: value for slug, (_sort_key, value) in latest_by_market.items()}


def _outcome(row: Mapping[str, Any], settlement_by_market: Mapping[str, float]) -> int | None:
    slug = str(row.get("market_slug") or "")
    k_price = _float(row.get("k_price"))
    settlement_price = settlement_by_market.get(slug)
    if k_price is None or settlement_price is None:
        return None
    return 1 if settlement_price >= k_price else 0


def _empty_bucket(low: float, high: float) -> dict[str, Any]:
    return {
        "range": [low, high],
        "samples": 0,
        "mean_pred": None,
        "actual_up_rate": None,
        "brier": None,
    }


def _finalize_bucket(low: float, high: float, samples: list[tuple[float, int]]) -> dict[str, Any]:
    if not samples:
        return _empty_bucket(low, high)
    count = len(samples)
    mean_pred = sum(pred for pred, _outcome_value in samples) / count
    actual = sum(outcome_value for _pred, outcome_value in samples) / count
    brier = sum((pred - outcome_value) ** 2 for pred, outcome_value in samples) / count
    return {
        "range": [low, high],
        "samples": count,
        "mean_pred": round(mean_pred, 6),
        "actual_up_rate": round(actual, 6),
        "brier": round(brier, 6),
    }


def _bucket_key(probability: float, buckets: Sequence[tuple[float, float]]) -> tuple[float, float] | None:
    for low, high in buckets:
        if probability >= low and (probability < high or (high >= 1.0 and probability <= high)):
            return low, high
    return None


def _finalize_metrics(
    predictions: list[tuple[float, int]],
    *,
    config: CalibrationConfig,
    missing_sigma: int,
    skipped_rows: int,
) -> dict[str, Any]:
    count = len(predictions)
    if count == 0:
        return {
            "samples": 0,
            "missing_sigma": missing_sigma,
            "skipped_rows": skipped_rows,
            "brier": None,
            "log_loss": None,
            "mean_pred": None,
            "outcome_up_rate": None,
            "buckets": [_empty_bucket(low, high) for low, high in config.buckets],
            "high_confidence": {},
        }

    floor = min(0.5, max(1e-12, config.probability_floor))
    brier = sum((pred - outcome_value) ** 2 for pred, outcome_value in predictions) / count
    log_loss = -sum(
        outcome_value * math.log(min(1.0 - floor, max(floor, pred)))
        + (1 - outcome_value) * math.log(min(1.0 - floor, max(floor, 1.0 - pred)))
        for pred, outcome_value in predictions
    ) / count
    mean_pred = sum(pred for pred, _outcome_value in predictions) / count
    outcome_up_rate = sum(outcome_value for _pred, outcome_value in predictions) / count

    bucket_samples: dict[tuple[float, float], list[tuple[float, int]]] = defaultdict(list)
    for pred, outcome_value in predictions:
        key = _bucket_key(pred, config.buckets)
        if key is not None:
            bucket_samples[key].append((pred, outcome_value))

    high_confidence: dict[str, dict[str, Any]] = {}
    for threshold in config.high_confidence_thresholds:
        selected = [
            (pred, outcome_value)
            for pred, outcome_value in predictions
            if max(pred, 1.0 - pred) >= threshold
        ]
        high_confidence[f"{threshold:.2f}"] = {
            "samples": len(selected),
            "accuracy": round(
                sum((pred >= 0.5) == bool(outcome_value) for pred, outcome_value in selected) / len(selected),
                6,
            ) if selected else None,
            "brier": round(
                sum((pred - outcome_value) ** 2 for pred, outcome_value in selected) / len(selected),
                6,
            ) if selected else None,
        }

    return {
        "samples": count,
        "missing_sigma": missing_sigma,
        "skipped_rows": skipped_rows,
        "brier": round(brier, 6),
        "log_loss": round(log_loss, 6),
        "mean_pred": round(mean_pred, 6),
        "outcome_up_rate": round(outcome_up_rate, 6),
        "buckets": [
            _finalize_bucket(low, high, bucket_samples.get((low, high), []))
            for low, high in config.buckets
        ],
        "high_confidence": high_confidence,
    }


def evaluate_lookbacks(
    rows: Iterable[Mapping[str, Any]],
    klines: Iterable[Sequence[Any]],
    lookbacks: Iterable[int],
    *,
    config: CalibrationConfig | None = None,
    settlement_by_market: Mapping[str, float] | None = None,
) -> dict[int, dict[str, Any]]:
    cfg = config or CalibrationConfig()
    row_list = list(rows)
    sorted_klines = _sorted_klines(klines)
    settlements = dict(settlement_by_market or infer_settlement_by_market(row_list))
    lookback_values = [int(value) for value in lookbacks]
    predictions: dict[int, list[tuple[float, int]]] = {lookback: [] for lookback in lookback_values}
    missing_sigma: dict[int, int] = {lookback: 0 for lookback in lookback_values}
    skipped_rows = 0

    for row in row_list:
        timestamp_sec = parse_row_time(row)
        s_price = _float(row.get("s_price"))
        k_price = _float(row.get("k_price"))
        remaining_sec = _float(row.get("remaining_sec"))
        age_sec = _float(row.get("age_sec"))
        outcome_value = _outcome(row, settlements)
        if (
            timestamp_sec is None
            or s_price is None
            or k_price is None
            or remaining_sec is None
            or outcome_value is None
            or (cfg.min_age_sec is not None and age_sec is not None and age_sec < cfg.min_age_sec)
            or (cfg.max_age_sec is not None and age_sec is not None and age_sec > cfg.max_age_sec)
        ):
            skipped_rows += 1
            continue

        for lookback in lookback_values:
            kline_slice = _past_kline_slice(sorted_klines, timestamp_sec=timestamp_sec, lookback_minutes=lookback)
            snapshot = compute_binance_rv_sigma_from_klines(
                kline_slice,
                ewma_half_life_minutes=cfg.ewma_half_life_minutes,
                floor_annual=cfg.floor_annual,
                cap_annual=cfg.cap_annual,
            )
            if snapshot.sigma is None:
                missing_sigma[lookback] += 1
                continue
            prob_up = binary_probability(s_price, k_price, snapshot.sigma, remaining_sec)
            predictions[lookback].append((prob_up, outcome_value))

    return {
        lookback: _finalize_metrics(
            predictions[lookback],
            config=cfg,
            missing_sigma=missing_sigma[lookback],
            skipped_rows=skipped_rows,
        )
        for lookback in lookback_values
    }
