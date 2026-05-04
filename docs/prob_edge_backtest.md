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
- `summary.skip_reason_counts`: skip reason distribution while flat.
- `summary.settlement_uncertain`: settlement count where Binance proxy is close
  to K and therefore less reliable.
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

For win-rate-first scans, filter tiny samples and sort by win rate:

```bash
python3 scripts/backtest_prob_edge.py \
  --jsonl data/prob-edge-collector-96-20260503T162542Z.kprice-ok.jsonl \
  --slippage-ticks 3 \
  --grid-min-entries 30 \
  --grid-sort-by win_rate
```

Available grid sort modes:

- `pnl`: total PnL first.
- `win_rate`: win rate first, then entries, then PnL.
- `avg_pnl`: average PnL per trade first.

## Model

The replay uses:

- `s_price`, `k_price`, `remaining_sec`
- `volatility.sigma` as `sigma_eff`
- `up/down.ask_avg` and `up/down.ask_limit` for entry
- `up/down.bid_avg` and `up/down.bid_limit` for exit
- `volatility_stale=true` as missing sigma, matching live behavior

Entry decisions still use size-aware `ask_avg` for edge screening, but simulated
fills use the worse executable depth limit. If slippage pushes BUY above the
fair cap (`model_prob - required_edge`), the fill is skipped.

Trade fields keep both edge definitions:

- `entry_edge`: strategy edge at signal time, matching live logs
  (`model_prob - ask_avg`).
- `entry_edge_at_fill`: edge after simulated fill price and slippage.

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

When `abs(s_price - k_price) < --settlement-boundary-usd` (default `5`), the
trade is still counted but marked `settlement_uncertain=true` so analysis can
bucket or exclude boundary cases.

## Limits

This is not full exchange-level simulation. The collector does not store full
order books, trade queue position, signed order latency, or exact post-order
fill response shape. Use this backtest to compare parameter sets and find
obvious strategy weaknesses. Use a separate full book recorder for precise fill
simulation later.

`tick_size` defaults to `0.01`, which matches observed BTC 5-minute tokens. If a
future market uses a different CLOB tick, pass `--tick-size` explicitly until
the collector records per-token tick sizes.
