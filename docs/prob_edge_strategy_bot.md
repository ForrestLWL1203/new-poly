# Probability Edge Strategy Bot

`scripts/run_prob_edge_bot.py` runs the first BTC 5-minute probability-edge
strategy robot.

It shares one strategy state machine across two execution modes:

- `paper`: default. Simulates FAK fills from the live local CLOB book.
- `live`: posts real FAK orders only when both `--mode live` and
  `--i-understand-live-risk` are provided.

The bot is separate from `scripts/collect_prob_edge_data.py`. The collector
records raw market data only; this script computes probabilities, decisions,
virtual/live fills, position state, and PnL.

## Run

Paper smoke test:

```bash
python3 scripts/run_prob_edge_bot.py --once
```

Paper run for 12 complete windows:

```bash
mkdir -p data
python3 scripts/run_prob_edge_bot.py \
  --mode paper \
  --windows 12 \
  --jsonl data/prob-edge-bot-paper.jsonl
```

Aggressive paper candidate for longer VPS dry-runs:

```bash
mkdir -p data
python3 scripts/run_prob_edge_bot.py \
  --config configs/prob_edge_aggressive.yaml \
  --mode paper \
  --windows 48 \
  --jsonl data/prob-edge-bot-paper-aggressive-48w.jsonl
```

Paper mode enables analysis logs by default. Live mode keeps them off unless
explicitly requested:

```bash
python3 scripts/run_prob_edge_bot.py \
  --mode live \
  --i-understand-live-risk \
  --analysis-logs \
  --windows 1
```

Use `--no-analysis-logs` to keep only compact operational rows during long
paper runs.

Live mode requires the explicit second guard:

```bash
python3 scripts/run_prob_edge_bot.py \
  --mode live \
  --i-understand-live-risk \
  --windows 1
```

## Default Rules

- New entries use phase-specific edge thresholds:
  `90 <= age < 120` requires `early_required_edge = 0.16`;
  `120 <= age < 240` requires `core_required_edge = 0.14`;
  `240 <= age <= 270` is disabled by default with `late_entry_enabled = false`.
- No new entries in the final 30 seconds.
- Default notional is `$5` in the MVP profile. The aggressive/live-smoke
  profile uses `$1`.
- Default max successful entries per market is `2`.
- The default strategy loop interval is `0.5s`, so paper/live decisions run at
  roughly 2Hz. Time-window guards such as `prob_drop_exit_window_sec=5` and
  `prob_stagnation_window_sec=3` are wall-clock windows, not tick counts.
- `sigma_eff` uses Deribit BTC DVOL divided by 100.
- K is the Polymarket UI Price to Beat from the crypto price API.
- Settlement/reporting in paper mode uses proxy direction; the bot does not wait
  for Polymarket `closePrice`.
- S uses Polymarket live-data `crypto_prices_chainlink` by default. This is the
  same live BTC/USD source observed by the Polymarket event UI and matched the
  crypto price API open/close ticks in a three-window probe on 2026-05-06.
- Binance and Coinbase are backup sources, but they are not started while
  Polymarket live-data is healthy. If the Polymarket source is missing or stale
  beyond `market_data.max_polymarket_price_age_sec`, the bot first stays
  fail-closed and does not trade. Only after the source has been continuously
  unhealthy for `market_data.polymarket_backup_after_sec` does it lazily start
  Binance/Coinbase and fall back to the previous basis-adjusted proxy logic.
- The old single `required_edge` field is no longer used.

Config files:

- `configs/prob_edge_mvp.yaml`: baseline/default config.
- `configs/prob_edge_aggressive.yaml`: current optimized aggressive paper
  candidate. It is aggressive by entry count but stricter by entry quality:
  `100-240s` entry timing, `0.16/0.14` early/core edge thresholds,
  `min_entry_model_prob=0.35`, `max_entries_per_market=4`, and `$1`
  notional/depth.
- `configs/prob_edge_dynamic.yaml`: optional dynamic signal-parameter governor
  profile set. It is disabled unless `--dynamic-params` is passed.

Config knobs include:

```text
early_required_edge
core_required_edge
late_entry_enabled
late_required_edge
late_max_spread
defensive_profit_min
protection_profit_min
final_hold_min_prob
final_hold_min_bid_avg
final_hold_min_bid_limit
prob_stagnation_window_sec
prob_stagnation_epsilon
prob_drop_exit_window_sec
prob_drop_exit_threshold
min_entry_model_prob
low_price_extra_edge_threshold
low_price_extra_edge
cross_source_max_bps
market_disagrees_exit_threshold
market_disagrees_exit_max_remaining_sec
market_disagrees_exit_min_loss
market_disagrees_exit_min_age_sec
market_disagrees_exit_max_profit
retry_count
retry_interval_sec
polymarket_price_enabled
max_polymarket_price_age_sec
polymarket_backup_after_sec
coinbase_enabled
```

The current `0.16/0.14` edge thresholds and `0.35` model-probability floor came
from replaying the 120-window collector and dry-run datasets captured on
2026-05-04. They replace the earlier `0.12/0.08` defaults because those looser
thresholds admitted too many low-probability lottery-style entries and larger
`logic_decay_exit` losses.

Coinbase is now a lazy backup source, not the normal strategy baseline. When
the bot is already in fallback mode and both Binance/Coinbase live prices are
available, `cross_source_max_bps` acts as an entry quality gate. If the two
backup sources disagree by more than the configured basis-point threshold, the
bot skips new entries with `source_divergence`. While Polymarket live-data is
fresh, Binance/Coinbase are not needed for trading decisions.

The initial `market_disagrees_exit_threshold` default is `0.30`. A looser
`0.20` threshold caught more bid/model deterioration but over-exited in replay;
`0.30` is the current dry-run candidate.

## FAK Price Logic

The bot keeps two separate prices for each candidate:

- `price`: size-aware average executable price. This is used for edge:
  `edge = model_prob - price`.
- `depth_limit_price`: worst book level needed to fill the target notional.
  For BUY this is the deepest ask level required to fill `amount_usd`; for
  SELL this is the deepest bid level required to sell the held shares.
- `limit_price`: formula-derived hard cap/floor used by execution. For BUY,
  `limit_price = model_prob - required_edge`. For SELL, it remains the
  executable bid floor returned by the exit quote.

For BUY, the formula probability acts as a maximum acceptable token price:

```text
fair_cap = model_prob - required_edge
edge = model_prob - ask_avg
```

An entry is valid only when both are true:

```text
model_prob >= min_entry_model_prob
edge >= required_edge
ask_limit <= fair_cap
```

This prevents an average-cheap book from passing when the deepest required ask
level is already more expensive than the model allows.
`min_entry_model_prob` is a separate quality gate: it rejects low-probability
lottery-style entries even when their price discount is large.

Low-priced tokens can be guarded without banning them outright:

```text
if ask_avg < low_price_extra_edge_threshold:
    effective_required_edge = phase_required_edge + low_price_extra_edge
```

Both values default to `0` in code, which disables the guard. The current
aggressive profile enables a light guard at `<0.30 + 0.02`, based on replaying
the combined 168-window paper sample. This is intentionally softer than a
minimum entry price: recent paper data showed low-priced tokens can contain both
the largest losses and the largest winners, so the current preference is to
require more edge rather than discard the whole bucket.

Live-oriented configs add two extra FAK quality guards:

```text
fair_cap - ask_limit >= min_fair_cap_margin_ticks * tick_size
ask_safety_limit <= fair_cap
```

`ask_safety_limit` is computed from
`amount_usd * depth_safety_multiplier`. The bot still calculates `ask_avg` and
`ask_limit` from the actual order notional, so the safety buffer does not
artificially worsen the edge. It only requires extra executable depth inside the
formula cap. The default aggressive/live-smoke profile uses a `1.5x` depth
safety multiplier and a one-tick fair-cap margin.

Live FAK BUY price hinting then uses the depth limit, not the first ask:

```text
price_hint = min(ask_limit + buffer_ticks * tick_size, fair_cap)
```

Example:

```text
model_prob = 0.62
required_edge = 0.06
fair_cap = 0.56

ask book for target notional:
0.50 covers part of the order
0.54 completes the target notional

ask_limit = 0.54
tick_size = 0.01
price_hint = min(0.54 + 0.01, 0.56) = 0.55
```

Using `best_ask + buffer` would fail to cross the `0.54` level in this case, so
the live order uses `depth_limit_price` as the buffer base.

For SELL, the bot uses `bid_limit`, the lowest bid level needed to sell the
position size, rather than `bid_avg`.

## FAK Retry

Execution retry is intentionally small:

```text
retry_count = 1
retry_interval_sec = 0.0
```

BUY gets at most one retry. After a no-fill, both paper and live immediately
rebuild a fresh strategy snapshot from the current proxy price, current
remaining time, and current in-memory CLOB WS book. The retry is posted only if
the same side still passes `evaluate_entry`; otherwise it is skipped instead of
chasing a stale signal. The default hint ladder is configurable:

```text
attempt 1: min(ask_limit + 2 ticks, fair_cap)
attempt 2: min(ask_limit + 4 ticks, fair_cap)
```

SELL also gets one retry. In CLOB FAK semantics any visible bid at or above the
sell floor can fill, while bids below the floor are rejected. Normal
profit-taking and stop-loss exits use the same configurable aggressive floor:

```text
normal exits:
  market_overprice_exit / defensive_take_profit / profit_protection_exit
  logic_decay_exit / risk_exit
  attempt 1: bid_limit - 4 ticks
  attempt 2: bid_limit - 5 ticks

final_force_exit:
  attempt 1: bid_limit - 5 ticks
  attempt 2: bid_limit - 10 ticks
```

Earlier versions used separate profit/stop floors. Live testing showed that even
profit exits can hit FAK no-match when the local book moves between WS snapshot
and POST, so normal exits now share a `4/5 tick` ladder. `final_force_exit`
keeps its hardcoded `5/10 tick` emergency ladder because the final seconds
prioritize reducing expiry exposure over profile-level tuning. Sell floors are
clamped at one tick; for very low-priced tokens, attempt 1 and attempt 2 may
therefore collapse to the same one-tick floor.

SELL retry is different from BUY retry. Once an exit signal has fired, paper and
live enter an exit-commitment path: the retry still rebuilds a fresh snapshot,
but it does not require `evaluate_exit` to return an exit again. If the open
position still has fresh bid depth, the refreshed `bid_limit` is used as the new
sell floor with the original exit reason. This is based on the 2026-05-05
48-window paper run where 5 exit no-fills were observed and 4/5 became worse
after the retry was skipped as "signal no longer valid".

Live CLOB can return a `400` response such as `no orders found to match with FAK
order`. That is normal FAK behavior when the local WS book has moved before the
POST reaches the matching engine. The live executor records it as
`order_no_fill` with `latency_ms` / `total_latency_ms` and keeps the bot running;
it does not create a position and does not terminate the process.

Paper and live share the same strategy decisions and retry refresh callbacks.
The only intended mode difference is execution: paper simulates local FAK fills
without POSTing orders, while live posts real CLOB FAK orders. Current configs
set `paper_latency_sec=0.0`: the observed CLOB delay is request/response time,
not a full pre-match sleep before the order reaches the matching engine. Keep
`paper_latency_sec` available only for explicit stress tests. Current
live-oriented profiles also set `retry_interval_sec=0`, so retry refresh happens
without an intentional wait.

## Exit Logic

The bot still exits on logic decay and market overpricing, and now adds
late-window profit protection:

- `logic_decay_exit`: model probability falls below entry cost by `0.02`.
- `market_overprice_exit`: executable bid is above model probability by `0.02`.
- `defensive_take_profit`: when `30 < remaining_sec <= 60`, profit is at least
  `defensive_profit_min`, and the held-side model probability has not risen over
  `prob_stagnation_window_sec`.
- `prob_drop_exit`: when enabled, the held-side model probability drops by at
  least `prob_drop_exit_threshold` over `prob_drop_exit_window_sec` and is below
  the entry-time model probability. The current aggressive profile uses a
  `5s / 0.06` guard to cut fast probability collapses before classic
  `logic_decay_exit` fires. The below-entry condition is intentional: positions
  that became strongly favorable and then partially retreated do not panic-exit
  while still better than the original model probability. Because probability
  history is cleared at entry, this guard cannot fire until the position has
  been open for at least `prob_drop_exit_window_sec`.
- `market_disagrees_exit`: when enabled, the Polymarket executable bid becomes
  materially cheaper relative to the model than it was at entry. It is
  intentionally constrained to late, already-losing positions:
  `remaining_sec <= market_disagrees_exit_max_remaining_sec`, loss at least
  `market_disagrees_exit_min_loss`, position age at least
  `market_disagrees_exit_min_age_sec`, and no meaningful current profit. This
  catches cases where the model probability stays high but the market is
  increasingly unwilling to pay for that outcome.
- `profit_protection_exit`: legacy configurable profit guard; the current
  30-second final-force boundary normally supersedes it.
- `final_force_exit`: when `remaining_sec <= final_force_exit_remaining_sec`
  (`30s` in current configs), sell if depth exists unless
  `model_prob`, `bid_avg`, and `bid_limit` all exceed their final-hold
  thresholds.

Exit decisions log `profit_now`, `prob_stagnant`, `prob_delta_3s`,
`prob_drop_delta`, and `market_disagreement` for post-run analysis.

If the probability history is too short to compare against the configured
stagnation window, `prob_stagnant=false` and `defensive_take_profit` does not
trigger.

## Logs

Each JSONL row is compact and non-secret. Rows include:

```text
event
mode
market_slug
age_sec
remaining_sec
s_price
k_price
sigma_eff
up/down book summaries
decision
order
position
realized_pnl
decision.phase
decision.required_edge
decision.profit_now
decision.prob_stagnant
decision.prob_delta_3s
```

Normal long-running logs intentionally keep price diagnostics minimal: only the
effective `price_source`, `s_price`, `k_price`, and `basis_bps` stay in the
runtime row. The full DVOL snapshot is also omitted from normal tick rows; keep
`sigma_eff`, `sigma_source`, and `volatility_stale` for replay compatibility.

When analysis logs are enabled, the bot first writes a `config` row containing
the non-secret strategy, execution, and runtime parameters used for the run.
Rows also include BTC source diagnostics under `analysis.price_sources`, but
only for the active pricing path:

```text
price_source
s_price
k_price
basis_bps
polymarket_price
polymarket_price_age_sec
polymarket_open_price
polymarket_open_source
proxy_price
proxy_open_price
binance_price
binance_open_price
binance_open_source
coinbase_price
coinbase_open_price
coinbase_open_source
source_spread_usd
source_spread_bps
```

Null and `"missing"` analysis values are omitted. Polymarket fields appear when
the Polymarket feed has data; proxy/Binance/Coinbase fields appear only after
lazy backup activation or when running with `--no-polymarket-price`.

Analysis logs are enabled by default in paper mode, disabled by default in live
mode, and can be explicitly toggled with `--analysis-logs` or
`--no-analysis-logs`. Use `--analysis-logs` for short live diagnostics only; it
is intentionally not the quiet long-running live default.

Entry/exit/order-no-fill rows also include strategy execution diagnostics in
the same `analysis` object:

```text
order_intent
entry/exit side
entry/exit model probability
entry signal price
entry fair cap
entry depth limit price
entry signal edge
entry edge at fill
exit min price
exit profit per share
paper/live fill price and shares
order attempt
order total latency
```

This makes paper runs directly usable for parameter analysis while allowing
long-running live mode to keep debug-style fields disabled.

The bot does not log private keys, API secrets, signed order payloads, or full
order books.

By default, strategy JSONL files are pruned by row timestamp:

- `--log-retention-hours 24` is the default.
- Pruning runs when the logger opens and then every
  `--log-prune-every-windows` completed windows. The default runtime cadence is
  `5`.
- Rows without a parseable `ts` are kept to avoid accidental data loss.
- Use `--log-retention-hours 0` to disable pruning for archival runs.
- If dynamic parameters are enabled, keep retention long enough to cover the
  dynamic lookback. Default `24h` retention covers about `288` BTC 5m windows,
  comfortably above the default `50`-window lookback.

## Dynamic Signal Parameters

The bot can optionally run a window-bound dynamic parameter governor:

```bash
python3 scripts/run_prob_edge_bot.py \
  --config configs/prob_edge_aggressive.yaml \
  --mode paper \
  --dynamic-params \
  --dynamic-config configs/prob_edge_dynamic.yaml \
  --dynamic-state data/prob-edge-dynamic-state.json \
  --windows 96 \
  --jsonl data/prob-edge-bot-paper-aggressive-dynamic-96w.jsonl
```

`--dynamic-params` requires `--jsonl`; the strategy log is the analysis input.

This feature is a risk governor, not a profit maximizer. It only changes:

```text
entry_start_age_sec
entry_end_age_sec
early_required_edge
core_required_edge
max_entries_per_market
```

It never changes notional size, depth notional, exit thresholds, FAK retry
settings, or volatility settings.

Default behavior:

- Every 5 completed windows, it launches a background analysis task.
- The analysis uses the last 50 completed windows from the strategy JSONL.
- Health requires at least 20 closed trades, win rate at least 55%, and
  non-negative total PnL.
- Two consecutive failed health checks are required before a profile switch can
  be scheduled.
- Candidate profiles are replayed with 3-tick BUY/SELL slippage. This is an
  intentional robustness bias, not a claim that live FAK normally slips 3 ticks.
- Profile changes are applied only at the next window boundary.
- MVP recovery is disabled: the governor can move to a more conservative
  profile, but it does not automatically move back to aggressive.
- Dynamic adjustment is monotonically de-risking. Once the active profile moves
  to a more conservative tier, returning to a more aggressive tier requires a
  manual reset or edit of `data/prob-edge-dynamic-state.json`.

Dynamic logs:

- `dynamic_check`: health metrics, failed streak, action, and candidate results.
- `config_update`: old/new profile, applied window, health snapshot, and changed
  signal parameters.
- `dynamic_error`: fail-closed error path. The bot keeps the current profile.

Dynamic state:

`--dynamic-state` points at a small JSON file that survives restarts:

```json
{
  "active_profile": "aggressive",
  "pending_profile": null,
  "switched_at_window_id": null,
  "switched_at_ts": null,
  "failed_health_checks": 0,
  "last_check_window_id": null,
  "last_check_result": {},
  "switch_history": []
}
```

The state file is intentionally separate from the main strategy config. The
strategy config remains the startup baseline; the dynamic state records which
profile is currently active, whether a switch is pending for the next window,
and why previous switches happened.

How to read dynamic output:

- `dynamic_check` with `action=no_change`: current profile is healthy.
- `dynamic_check` with `action=wait_for_confirmation`: one health check failed,
  but no switch is allowed yet.
- `dynamic_check` with `action=switch_pending`: a safer profile has been chosen
  and will apply at the next window boundary.
- `config_update`: the pending profile actually became active.
- `dynamic_error`: analysis failed closed; current parameters remain active.

Live mode keeps dynamic parameters off unless `--dynamic-params` is explicitly
provided. Live dynamic switching also enforces a 2-hour switch cooldown and a
drawdown pause threshold from `configs/prob_edge_dynamic.yaml`.

Recommended first test:

```bash
python3 scripts/run_prob_edge_bot.py \
  --config configs/prob_edge_aggressive.yaml \
  --mode paper \
  --dynamic-params \
  --dynamic-config configs/prob_edge_dynamic.yaml \
  --dynamic-state data/prob-edge-dynamic-state.json \
  --windows 120 \
  --jsonl data/prob-edge-bot-paper-aggressive-dynamic-120w.jsonl
```

This is long enough for the first 50-window lookback plus multiple 5-window
health checks. Do not enable dynamic parameters in live mode until this paper
run has been reviewed.
