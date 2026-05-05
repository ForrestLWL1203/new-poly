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
- SELL fill = executable bid average - `N * tick_size`, clamped by the FAK
  floor

Use `--buy-slippage-ticks` and `--sell-slippage-ticks` when the two sides need
different assumptions. The default `tick_size` is `0.01`.

Replay mirrors paper/live SELL semantics: the strategy floor is the minimum
acceptable FAK price, not the actual fill price. When the collector row says the
position can sell into current bid depth, replay fills from `bid_avg` and only
uses the floor as a lower bound:

```text
normal exits:      max(bid_avg - sell_slippage_ticks, bid_limit - 4 ticks)
final_force_exit:  max(bid_avg - sell_slippage_ticks, bid_limit - 5 ticks)
```

The retry ladder and the 400ms paper latency are not simulated in replay. Use
paper mode when you need to observe retry behavior or post-signal book movement
against the latest local book.

To replay the current live/dry-run entry guard, pass the fair-cap margin used by
the bot:

```bash
python3 scripts/backtest_prob_edge.py \
  --jsonl data/prob-edge-collector-96-20260503T162542Z.kprice-ok.jsonl \
  --amount-usd 1 \
  --entry-start-age-sec 100 \
  --entry-end-age-sec 240 \
  --early-required-edge 0.16 \
  --core-required-edge 0.14 \
  --max-entries-per-market 4 \
  --min-fair-cap-margin-ticks 1 \
  --entry-tick-size 0.01 \
  --min-entry-model-prob 0.35 \
  --prob-drop-exit-window-sec 5 \
  --prob-drop-exit-threshold 0.06 \
  --slippage-ticks 3
```

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
- `source_spread_bps` when present, so `cross_source_max_bps` can reproduce the
  live source-divergence entry skip

Entry decisions still use size-aware `ask_avg` for edge screening, but simulated
fills use the worse executable depth limit. If slippage pushes BUY above the
fair cap (`model_prob - required_edge`), the fill is skipped.

The backtest CLI exposes the live risk gates:

```text
--cross-source-max-bps
--market-disagrees-exit-threshold
--market-disagrees-exit-max-remaining-sec
--market-disagrees-exit-min-loss
--market-disagrees-exit-min-age-sec
--market-disagrees-exit-max-profit
```

Older JSONL without Coinbase fields cannot evaluate cross-source divergence;
those rows behave as if the source-spread gate has no signal.

SELL replay uses the same floor semantics as paper/live. Normal profit and stop
exits default to a `4 tick` first floor, while final-force exits use `5 ticks`.
The fill price comes from `bid_avg` unless explicit sell slippage is requested;
the floor is only a minimum acceptable FAK price. The floor is clamped at one
tick, matching the live executor behavior on very low-priced tokens.

Note: SELL fill semantics changed in May 2026. Older replay builds started from
`bid_limit`; current replay starts from `bid_avg` and uses `bid_limit` only as
the FAK floor base. PnL on identical historical data will generally be higher
than older conservative runs, so re-run grid scans before relying on old
parameter rankings.

When collector rows include `ask_safety_limit`, replay also enforces the current
depth safety check: the safety-depth limit must remain inside the same fair cap.
Older collector files that lack `ask_safety_limit` cannot reconstruct the newer
`depth_safety_multiplier` filter because they do not contain full order-book
levels; those files can still test fair-cap margin and slippage, but not the
extra safety-depth requirement.

Trade fields keep both edge definitions:

- `entry_edge`: strategy edge at signal time, matching live logs
  (`model_prob - ask_avg`).
- `entry_edge_at_fill`: edge after simulated fill price and slippage.

`--min-entry-model-prob` mirrors the live entry-quality gate. It rejects
low-probability, lottery-style candidates even when their `model_prob - ask_avg`
discount is large.

It reuses the production strategy functions:

- `evaluate_entry`
- `evaluate_exit`
- `StrategyState`

Use `--prob-drop-exit-window-sec` and `--prob-drop-exit-threshold` to replay the
fast probability-drop exit guard. The current aggressive profile uses `5` and
`0.06`.

If a position remains open after the final available row for a window, it is
settled using the collected proxy direction. New collector/bot logs use
Binance+Coinbase when both are available and fall back to Binance otherwise:

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
