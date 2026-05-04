# Probability Edge Backtest

`scripts/backtest_prob_edge.py` replays data collected by
`scripts/collect_prob_edge_data.py` through the current probability-edge
strategy state machine.

## Run

```bash
python3 scripts/backtest_prob_edge.py \
  --jsonl data/prob-edge-collector-96-20260503T162542Z.kprice-ok.jsonl
```

The script prints JSON with:

- `summary`: default-parameter performance.
- `exit_reasons`: trade count by exit path.
- `entry_phases`: trade count by early/core/late phase.
- `entry_sides`: UP/DOWN count.
- `grid_top`: best parameter combinations from the configured scan.
- `sample_trades`: first few simulated trades for sanity checking.

Current default scan-derived strategy parameters are:

```text
entry_start_age_sec = 90
early_required_edge = 0.12
core_required_edge = 0.08
entry_end_age_sec = 270
late_entry_enabled = false
```

To simulate FAK execution drift, add slippage ticks:

```bash
python3 scripts/backtest_prob_edge.py \
  --jsonl data/prob-edge-collector-96-20260503T162542Z.kprice-ok.jsonl \
  --slippage-ticks 2
```

`--slippage-ticks N` applies both:

- BUY fill = executable ask limit + `N * tick_size`
- SELL fill = executable bid limit - `N * tick_size`

Use `--buy-slippage-ticks` and `--sell-slippage-ticks` when the two sides need
different assumptions. The default `tick_size` is `0.01`.

## Model

The replay uses:

- `s_price`, `k_price`, `remaining_sec`
- `volatility.sigma` as `sigma_eff`
- `up/down.ask_avg` and `up/down.ask_limit` for entry
- `up/down.bid_avg` and `up/down.bid_limit` for exit

Entry decisions still use size-aware `ask_avg` for edge screening, but simulated
fills use the worse executable depth limit. If slippage pushes BUY above the
fair cap (`model_prob - required_edge`), the fill is skipped.

It reuses the production strategy functions:

- `evaluate_entry`
- `evaluate_exit`
- `StrategyState`

If a position remains open after the final available row for a window, it is
settled using Binance proxy direction:

```text
s_price > k_price => UP wins
s_price <= k_price => DOWN wins
```

## Limits

This is not full exchange-level simulation. The collector does not store full
order books, trade queue position, signed order latency, or exact post-order
fill response shape. Use this backtest to compare parameter sets and find
obvious strategy weaknesses. Use a separate full book recorder for precise fill
simulation later.
