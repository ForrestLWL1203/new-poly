"""Small structured logging helpers used by migrated infrastructure modules."""

from __future__ import annotations

import logging
from typing import Any

MARKET = "market"
WS = "ws"


def log_event(logger: logging.Logger, level: int, category: str, payload: dict[str, Any]) -> None:
    logger.log(level, "%s %s", category, payload)

