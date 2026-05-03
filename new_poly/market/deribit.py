"""Deribit volatility index helpers."""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass


DERIBIT_VOL_INDEX_URL = "https://www.deribit.com/api/v2/public/get_volatility_index_data"


@dataclass(frozen=True)
class DvolSnapshot:
    source: str
    currency: str
    dvol: float | None
    sigma: float | None
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
            "currency": self.currency,
            "dvol": round(self.dvol, 6) if self.dvol is not None else None,
            "sigma": round(self.sigma, 6) if self.sigma is not None else None,
            "timestamp_ms": self.timestamp_ms,
            "age_sec": round(self.age_sec, 3) if self.age_sec is not None else None,
        }


def fetch_dvol_snapshot(currency: str = "BTC", lookback_sec: int = 600, resolution: str = "60") -> DvolSnapshot:
    fetched_at = time.time()
    end_ms = int(fetched_at * 1000)
    start_ms = end_ms - int(lookback_sec * 1000)
    url = DERIBIT_VOL_INDEX_URL + "?" + urllib.parse.urlencode({
        "currency": currency.upper(),
        "start_timestamp": start_ms,
        "end_timestamp": end_ms,
        "resolution": resolution,
    })
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "new-poly/0.1"})
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        rows = payload.get("result", {}).get("data") or []
        if not rows:
            raise ValueError("empty DVOL response")
        timestamp_ms, *_ohl, close = rows[-1]
        dvol = float(close)
        return DvolSnapshot(
            source="deribit_dvol",
            currency=currency.upper(),
            dvol=dvol,
            sigma=dvol / 100.0,
            timestamp_ms=int(timestamp_ms),
            fetched_at=fetched_at,
        )
    except Exception:
        return DvolSnapshot(
            source="deribit_dvol",
            currency=currency.upper(),
            dvol=None,
            sigma=None,
            timestamp_ms=None,
            fetched_at=fetched_at,
        )
