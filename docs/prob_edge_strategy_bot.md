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
  `90 <= age < 120` requires `early_required_edge = 0.12`;
  `120 <= age < 240` requires `core_required_edge = 0.08`;
  `240 <= age <= 270` is disabled by default with `late_entry_enabled = false`.
- No new entries in the final 30 seconds.
- Default notional is `$5`.
- Default max successful entries per market is `2`.
- `sigma_eff` uses Deribit BTC DVOL divided by 100.
- K is the Polymarket UI Price to Beat from the crypto price API.
- Settlement/reporting in paper mode uses Binance proxy direction; the bot does
  not wait for Polymarket `closePrice`.
- S is Binance proxy price, basis-adjusted once K and Binance open are known.
- The old single `required_edge` field is no longer used.

Config files:

- `configs/prob_edge_mvp.yaml`: baseline/default config.
- `configs/prob_edge_aggressive.yaml`: current optimized aggressive paper
  candidate from the first 96-window replay. It uses `100-240s` entry timing,
  `0.14/0.12` early/core edge thresholds, `max_entries_per_market=4`, and
  `$1` notional/depth.
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
retry_count
retry_interval_sec
```

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
edge >= required_edge
ask_limit <= fair_cap
```

This prevents an average-cheap book from passing when the deepest required ask
level is already more expensive than the model allows.

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
retry_interval_sec = 0.2
```

BUY gets at most one retry. The retry still uses the same formula cap
`fair_cap`, so it cannot chase beyond `model_prob - required_edge`. The retry
only widens the hint by one extra tick:

```text
attempt 1: min(ask_limit + 1 tick, fair_cap)
attempt 2: min(ask_limit + 2 ticks, fair_cap)
```

SELL also gets one retry. The sell hint is the configured `min_price` floor on
both attempts. In CLOB FAK semantics this is already the most aggressive
allowed sell limit: any visible bid at or above `min_price` can fill, while bids
below the floor are rejected. The retry is a second chance against a changed
book, not a price-escalation ladder.

Paper mode mirrors the same retry count and interval against the latest local
book after the simulated latency. Paper applies `paper_latency_sec` once for the
initial order signal, then only `retry_interval_sec` before the retry so retry
simulation does not drift by an extra full tick.

## Exit Logic

The bot still exits on logic decay and market overpricing, and now adds
late-window profit protection:

- `logic_decay_exit`: model probability falls below entry cost by `0.02`.
- `market_overprice_exit`: executable bid is above model probability by `0.02`.
- `defensive_take_profit`: when `30 < remaining_sec <= 60`, profit is at least
  `defensive_profit_min`, and the held-side model probability has not risen over
  `prob_stagnation_window_sec`.
- `profit_protection_exit`: when `15 < remaining_sec <= 30`, profit is at least
  `protection_profit_min`.
- `final_force_exit`: when `remaining_sec <= 15`, sell if depth exists unless
  `model_prob`, `bid_avg`, and `bid_limit` all exceed their final-hold
  thresholds.

Exit decisions log `profit_now`, `prob_stagnant`, and `prob_delta_3s` for
post-run analysis.

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

When analysis logs are enabled, the bot first writes a `config` row containing
the non-secret strategy, execution, and runtime parameters used for the run.
Entry/exit/order-no-fill rows also include an `analysis` object:

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
- Pruning runs when the logger opens and after each completed window.
- Rows without a parseable `ts` are kept to avoid accidental data loss.
- Use `--log-retention-hours 0` to disable pruning for archival runs.

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
