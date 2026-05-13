"""Dynamic-parameter controller for the probability-edge bot."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass

from new_poly.bot_runtime import (
    BotConfig,
    JsonlLogger,
    RuntimeOptions,
)
from new_poly.strategy.dynamic_params import (
    DynamicConfig,
    DynamicDecision,
    DynamicState,
)


@dataclass
class DynamicParamController:
    cfg: DynamicConfig | None = None
    state: DynamicState | None = None
    task: asyncio.Task[tuple[DynamicDecision, DynamicState]] | None = None
    startup_error: str | None = None

    def load(self, *, options: RuntimeOptions, bot_config: BotConfig) -> tuple[RuntimeOptions, BotConfig]:
        if not options.dynamic_params:
            return options, bot_config
        self.cfg = None
        self.state = None
        self.startup_error = "--dynamic-params was removed with the old dual-source strategy"
        return options, bot_config

    def write_startup_error(self, *, logger: JsonlLogger, options: RuntimeOptions) -> None:
        if self.startup_error is None:
            return
        logger.write({
            "ts": dt.datetime.now().astimezone().isoformat(),
            "event": "dynamic_error",
            "mode": options.mode,
            "error_type": "startup",
            "message": self.startup_error,
            "action": "keep_current",
        })

    async def drain(self, *, logger: JsonlLogger, options: RuntimeOptions, window_slug: str) -> None:
        return None

    def update_after_window_close(
        self,
        *,
        state: DynamicState | None,
        task: asyncio.Task[tuple[DynamicDecision, DynamicState]] | None,
    ) -> None:
        self.state = state
        self.task = task

    def trigger_analysis_after_window(
        self,
        *,
        completed_windows: int,
        current_window_id: str,
        realized_drawdown: float,
        cfg: BotConfig,
        logger: JsonlLogger,
        options: RuntimeOptions,
    ) -> asyncio.Task[tuple[DynamicDecision, DynamicState]] | None:
        return self.task

    def apply_pending_profile(
        self,
        *,
        next_window_slug: str,
        cfg: BotConfig,
        logger: JsonlLogger,
        options: RuntimeOptions,
    ) -> tuple[BotConfig, DynamicState | None]:
        return cfg, self.state
