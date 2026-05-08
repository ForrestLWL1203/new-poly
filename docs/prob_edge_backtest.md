# Probability Edge Backtest

`scripts/backtest_prob_edge.py` replays data collected by
`scripts/collect_prob_edge_data.py` through the current probability-edge
strategy state machine.

## Run

```bash
python3 scripts/backtest_prob_edge.py \
  --jsonl data/prob-edge-collector-96-20260503T162542Z.kprice-ok.jsonl
```

When analyzing JSONL produced by `scripts/run_prob_edge_bot.py` in paper/live
mode, add `--honor-order-events`:

```bash
python3 scripts/backtest_prob_edge.py \
  --jsonl data/remote/prob-edge-paper-coinbase-48w-20260505T101937Z.jsonl \
  --honor-order-events \
  --no-grid
```

This mode replays actual `entry`, `exit`, and `order_no_fill` rows instead of
idealizing fills from every tick. Use it for post-run audit and PnL attribution.
Do not use it for parameter grid searches, because it intentionally follows the
already-executed order path rather than generating a new one from candidate
parameters.

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

Current MVP defaults are:

```text
entry_start_age_sec = 90
early_required_edge = 0.16
core_required_edge = 0.14
entry_end_age_sec = 270
late_entry_enabled = false
```

The current aggressive/live-smoke profile is stricter on timing and allows more
entries:

```text
entry_start_age_sec = 100
entry_end_age_sec = 240
dynamic_entry_enabled = false
strong_move_entry_start_age_sec = 60
strong_move_min_abs_sk_usd = 120
strong_move_required_edge = 0.24
fast_move_entry_start_age_sec = 70
fast_move_min_abs_sk_usd = 80
fast_move_required_edge = 0.22
early_required_edge = 0.16
core_required_edge = 0.14
max_entries_per_market = 2
amount_usd = 1
min_entry_model_prob = 0.40
low_price_extra_edge_threshold = 0.30
low_price_extra_edge = 0.04
buy_cap_relax_enabled = true
model_decay_buffer = 0.03
prob_drop_exit disabled by default
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
normal exits:      max(bid_avg - sell_slippage_ticks, bid_limit - 5 ticks)
final_force_exit:  max(bid_avg - sell_slippage_ticks, bid_limit - 5 ticks)
```

The retry ladder and optional paper fill latency are not simulated in normal
collector replay. Current strategy configs set `paper_latency_sec=0.0` because
the observed CLOB latency is request/response time, not a full pre-match wait
before the order reaches the matching engine. Use paper mode with a manually
configured nonzero latency only when you want a stress test for post-signal book
movement.

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
  --model-decay-buffer 0.03 \
  --max-entries-per-market 2 \
  --min-fair-cap-margin-ticks 1 \
  --entry-tick-size 0.01 \
  --min-entry-model-prob 0.40 \
  --low-price-extra-edge-threshold 0.30 \
  --low-price-extra-edge 0.04 \
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
- `up/down.ask` for entry
- `up/down.bid_avg` and `up/down.bid_limit` for exit
- `volatility_stale=true` as missing sigma, matching live behavior
- `source_spread_bps` when present, so `cross_source_max_bps` can reproduce the
  live source-divergence entry skip
- `polymarket_divergence_bps` or `lead_binance_vs_polymarket_bps`, so
  `polymarket_divergence_exit_bps` can reproduce the live Polymarket-reference
  risk exit

Entry decisions use fresh `ask`/best ask for edge screening. Replay no longer
requires `ask_avg`, `ask_limit`, or `ask_safety_limit` to pass a BUY entry. If
simulated BUY slippage pushes the hinted price above the fair cap
(`model_prob - required_edge`), the fill is skipped.

The backtest CLI exposes the live risk gates:

```text
--cross-source-max-bps
--market-disagrees-exit-threshold
--market-disagrees-exit-max-remaining-sec
--market-disagrees-exit-min-loss
--market-disagrees-exit-min-age-sec
--market-disagrees-exit-max-profit
--polymarket-divergence-exit-bps
--polymarket-divergence-exit-min-age-sec
--low-price-extra-edge-threshold
--low-price-extra-edge
```

Older JSONL without Coinbase fields cannot evaluate cross-source divergence;
those rows behave as if the source-spread gate has no signal.

SELL replay uses the same floor semantics as paper/live. Normal profit and stop
exits default to a `5 tick` first floor, while final-force exits use `5 ticks`.
The fill price comes from `bid_avg` unless explicit sell slippage is requested;
the floor is only a minimum acceptable FAK price. The floor is clamped at one
tick, matching the live executor behavior on very low-priced tokens.

Note: SELL fill semantics changed in May 2026. Older replay builds started from
`bid_limit`; current replay starts from `bid_avg` and uses `bid_limit` only as
the FAK floor base. PnL on identical historical data will generally be higher
than older conservative runs, so re-run grid scans before relying on old
parameter rankings.

Collector rows may still include `ask_avg`, `ask_limit`, and
`ask_safety_limit`, but these are diagnostics after the BBO-based entry change.
They are useful for analyzing how often FAK no-fill might happen, not for
screening entries in replay.

Trade fields keep both edge definitions:

- `entry_edge`: strategy edge at signal time, matching live logs
  (`model_prob - ask`).
- `entry_edge_at_fill`: edge after simulated fill price and slippage.

`--min-entry-model-prob` mirrors the live entry-quality gate. It rejects
low-probability, lottery-style candidates even when their `model_prob - ask`
discount is large.

`--low-price-extra-edge-threshold` and `--low-price-extra-edge` test the softer
low-price guard. When the candidate best ask is below the threshold, replay
adds the extra edge to the current phase edge and also tightens the BUY fair
cap. The default values are `0`, which disables the guard.

It reuses the production strategy functions:

- `evaluate_entry`
- `evaluate_exit`
- `StrategyState`

Use `--prob-drop-exit-window-sec` and `--prob-drop-exit-threshold` to replay the
fast probability-drop exit guard. The current aggressive profile uses `5` and
`0.06`.

If a position remains open after the final available row for a window, it is
settled using the final collected effective `s_price` for that window. Current
collector/bot logs normally use Binance, optionally open-basis-adjusted to the
Polymarket `k_price`, as the effective model price. Polymarket live-data is kept
as a reference field for divergence analysis rather than the default model
source:

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
