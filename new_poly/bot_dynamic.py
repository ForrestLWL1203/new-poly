"""Dynamic-parameter controller for the probability-edge bot."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass, replace

from new_poly.bot_loop import _drain_dynamic_task
from new_poly.bot_runtime import BotConfig, JsonlLogger, RuntimeOptions, _bot_config_with_edge
from new_poly.strategy.dynamic_params import (
    DynamicConfig,
    DynamicDecision,
    DynamicState,
    load_dynamic_config,
    load_dynamic_state,
    save_dynamic_state,
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
        try:
            self.cfg = load_dynamic_config(options.dynamic_config)
            self.state = load_dynamic_state(options.dynamic_state, default_profile=self.cfg.active_profile)
            if self.state.active_profile not in self.cfg.profile_names():
                self.state = replace(self.state, active_profile=self.cfg.active_profile, pending_profile=None)
            bot_config = _bot_config_with_edge(
                bot_config,
                self.cfg.profile(self.state.active_profile).apply_to(bot_config.edge),
            )
            options = replace(options, config=bot_config)
            save_dynamic_state(options.dynamic_state, self.state)
        except Exception as exc:
            self.cfg = None
            self.state = None
            self.startup_error = str(exc)
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
        self.task, self.state = await _drain_dynamic_task(
            dynamic_task=self.task,
            dynamic_state=self.state,
            logger=logger,
            options=options,
            window_slug=window_slug,
        )

    def update_after_window_close(
        self,
        *,
        state: DynamicState | None,
        task: asyncio.Task[tuple[DynamicDecision, DynamicState]] | None,
    ) -> None:
        self.state = state
        self.task = task
