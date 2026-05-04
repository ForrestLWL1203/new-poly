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

Config knobs live in `configs/prob_edge_mvp.yaml`:

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
