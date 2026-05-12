from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
REPO_ROOT = Path(__file__).resolve().parents[1]

from new_poly.bot_runtime import _config_log_row, build_arg_parser, build_runtime_options, load_bot_config


def test_default_mode_is_paper_single_source() -> None:
    opts = build_runtime_options(build_arg_parser().parse_args(["--once"]))

    assert opts.mode == "paper"
    assert opts.analysis_logs is True
    assert opts.config.strategy_mode == "poly_single_source"
    assert opts.config.poly_source.poly_reference_distance_bps == 1.5
    assert not hasattr(opts.config, "edge")


def test_dynamic_params_cli_is_removed() -> None:
    args = build_arg_parser().parse_args(["--once", "--dynamic-params"])

    with pytest.raises(ValueError, match="removed with the old dual-source strategy"):
        build_runtime_options(args)


def test_config_log_row_contains_only_single_source_strategy_config() -> None:
    opts = build_runtime_options(build_arg_parser().parse_args(["--once"]))

    row = _config_log_row(opts)

    assert row["event"] == "config"
    assert "strategy" not in row
    assert row["poly_source"]["poly_reference_distance_bps"] == 1.5
    assert "private_key" not in str(row).lower()


def test_poly_single_source_config_loads() -> None:
    cfg = load_bot_config(REPO_ROOT / "configs" / "prob_poly_single_source.yaml")

    assert cfg.strategy_mode == "poly_single_source"
    assert cfg.poly_source.poly_reference_distance_bps == 1.5
    assert cfg.poly_source.reference_distance_exit_remaining_sec == (120.0, 90.0, 70.0, 45.0, 30.0)
    assert cfg.poly_source.reference_distance_exit_min_bps == (-2.0, -1.0, 0.25, 0.75, 1.0)
    assert cfg.poly_source.hold_to_settlement_min_poly_return_bps == -0.3
