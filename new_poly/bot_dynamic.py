"""Dynamic-parameter controller for the probability-edge bot."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass, replace

from new_poly.bot_loop import _drain_dynamic_task
from new_poly.bot_runtime import (
    BotConfig,
    JsonlLogger,
    RuntimeOptions,
    _bot_config_with_edge,
    _dynamic_candidate_payload,
    _dynamic_health_payload,
)
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

    def apply_pending_profile(
        self,
        *,
        next_window_slug: str,
        cfg: BotConfig,
        logger: JsonlLogger,
        options: RuntimeOptions,
    ) -> tuple[BotConfig, DynamicState | None]:
        if self.cfg is None or self.state is None or self.state.pending_profile is None:
            return cfg, self.state
        try:
            old_profile = self.state.active_profile
            old_edge = cfg.edge
            profile = self.cfg.profile(self.state.pending_profile)
            new_cfg = _bot_config_with_edge(cfg, profile.apply_to(cfg.edge))
            now_ts = dt.datetime.now(dt.timezone.utc).astimezone().isoformat()
            history = list(self.state.switch_history)
            history.append({
                "from_profile": old_profile,
                "to_profile": profile.name,
                "applied_at_window": next_window_slug,
                "switched_at_ts": now_ts,
                "health_check": self.state.last_check_result,
            })
            self.state = replace(
                self.state,
                active_profile=profile.name,
                pending_profile=None,
                switched_at_window_id=next_window_slug,
                switched_at_ts=now_ts,
                switch_history=history,
            )
            save_dynamic_state(options.dynamic_state, self.state)
            logger.write({
                "ts": now_ts,
                "event": "config_update",
                "mode": options.mode,
                "from_profile": old_profile,
                "to_profile": profile.name,
                "applied_at_window": next_window_slug,
                "reason": "dynamic_params",
                "health_check": _dynamic_health_payload(self.state.last_check_result),
                "candidate_results": _dynamic_candidate_payload(self.state.last_check_result),
                "old_signal_params": {
                    "entry_start_age_sec": old_edge.entry_start_age_sec,
                    "entry_end_age_sec": old_edge.entry_end_age_sec,
                    "early_required_edge": old_edge.early_required_edge,
                    "core_required_edge": old_edge.core_required_edge,
                    "max_entries_per_market": old_edge.max_entries_per_market,
                },
                "new_signal_params": profile.signal_params(),
            })
            return new_cfg, self.state
        except Exception as exc:
            logger.write({
                "ts": dt.datetime.now().astimezone().isoformat(),
                "event": "dynamic_error",
                "mode": options.mode,
                "market_slug": next_window_slug,
                "error_type": type(exc).__name__,
                "message": str(exc),
                "action": "keep_current",
            })
            return cfg, self.state
