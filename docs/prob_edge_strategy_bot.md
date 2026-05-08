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
Both entrypoints share strategy-neutral window, K-refresh, effective-price, and
token-depth helpers from `new_poly/market/prob_edge_data.py`; the bot does not
import implementation details from the collector script.

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
  `entry_start_age_sec <= age < early_to_core_age_sec` requires
  `early_required_edge`;
  `early_to_core_age_sec <= age < core_to_late_age_sec` requires
  `core_required_edge`;
  `core_to_late_age_sec <= age <= entry_end_age_sec` is disabled by default
  with `late_entry_enabled = false`. Current tuned configs use `0.16/0.14`
  edge thresholds and `120s/240s` phase boundaries.
- `dynamic_entry_enabled` can selectively admit earlier entries before
  `entry_start_age_sec`, but it is disabled by default. When enabled, the
  current experimental gates are: `60-70s` requires a strong move
  (`abs(S-K) >= 120`) and `strong_move_required_edge=0.24`; `70-100s` requires
  a fast move (`abs(S-K) >= 80`) and `fast_move_required_edge=0.22`. If the move
  condition is not met, the bot stays outside the entry window. Runtime CLI can
  override the YAML with `--dynamic-entry` or `--no-dynamic-entry`.
- No new entries in the final 30 seconds.
- After a risk exit (`logic_decay_exit`, `polymarket_divergence_exit`,
  `market_disagrees_exit`, or `risk_exit`), the bot blocks same-side re-entry
  for `logic_decay_reentry_cooldown_sec` seconds. Current configs use `30s`.
  The opposite side can still enter if it has fresh edge, and profit exits do
  not trigger this cooldown.
- Global loss protection is separate from same-side cooldown. Current configs
  use `risk.consecutive_loss_limit=5` and `risk.loss_pause_windows=3`: after 5
  consecutive losing closed trades, the bot skips all new entries for the next
  3 completed windows. Open positions still follow normal exit logic.
- Live mode treats "no sellable balance" on an exit as a hard accounting
  failure when `risk.stop_on_live_no_sellable_balance=true`. The bot writes a
  `fatal_stop` row and exits instead of continuing after the position balance
  has become unrecoverable.
- Default notional is `$5` in the MVP profile. The aggressive/live-smoke
  profile uses `$1`.
- Default max successful entries per market is `2`.
- The default strategy loop interval is `0.5s`, so paper/live decisions run at
  roughly 2Hz. Time-window guards such as `prob_stagnation_window_sec=3` are
  wall-clock windows, not tick counts.
- `sigma_eff` uses Deribit BTC DVOL divided by 100.
- Startup requires a valid DVOL snapshot. If the first request fails, the bot
  retries every `runtime.dvol_retry_interval_sec` seconds for
  `runtime.dvol_retry_attempts` retries and exits with `dvol_startup_failed` if
  it still cannot obtain sigma.
- Runtime DVOL refreshes do not overwrite the last valid snapshot with an empty
  one. Failed refreshes retry in the background; while the previous snapshot is
  still younger than `runtime.max_dvol_age_sec`, the strategy keeps using it.
  Once it becomes stale, new entries fail closed with `missing_model_inputs`
  while existing positions can still run exit checks.
- K is the Polymarket UI Price to Beat from the crypto price API.
- Settlement/reporting in paper mode uses the collected effective price
  direction; the bot does not wait for Polymarket `closePrice`.
- S uses Binance BTC/USDT trades as the default model input. This is intentional:
  the current strategy is trying to capture the CEX-to-Polymarket information
  lead, then only trade when the Polymarket CLOB price still offers edge.
- Polymarket live-data `crypto_prices_chainlink` is kept as a settlement-source
  reference and risk diagnostic, not as the normal probability-model `S`. A
  three-window probe on 2026-05-06 showed its boundary ticks matching the crypto
  price API `openPrice`/`closePrice`, so it is useful for detecting when the CEX
  lead is becoming dangerous near settlement.
- The Polymarket reference feed stores only a short rolling history, currently
  about 15 seconds. The bot only needs recent reference ticks for source
  divergence and short-horizon movement diagnostics.
- Coinbase is disabled by default and should stay off unless a specific run is
  testing multi-source backup behavior. With Coinbase disabled, the active model
  source is Binance-only.
- If Polymarket reference data is missing or stale, Binance can still provide
  the model price. The Polymarket feed has its own stale watchdog: after
  `market_data.polymarket_stale_reconnect_sec` without a valid price tick, it
  closes and reconnects the WS. The current default is `5s`.
- CEX-vs-Polymarket lead diagnostics are always computed when both sources have
  data because the cost is tiny. `analysis_logs` only decides whether those
  diagnostics are written to JSONL.
- `market_data.max_polymarket_price_age_sec` defaults to `4.0`; a five-window
  VPS paper run observed occasional healthy Polymarket updates around `3.27s`,
  so the previous `3.0` threshold was slightly too tight.
- The old single `required_edge` field is no longer used.

Config files:

- `configs/prob_edge_mvp.yaml`: baseline/default config.
- `configs/prob_edge_aggressive.yaml`: current optimized aggressive paper
  candidate. It is aggressive by entry count but stricter by entry quality:
  `100-240s` entry timing, `0.16/0.14` early/core edge thresholds,
  `min_entry_model_prob=0.35`, `max_entries_per_market=4`, and `$1`
  notional/depth. Dynamic early entry remains available as an explicit
  experiment via `--dynamic-entry`.
- `configs/prob_edge_dynamic.yaml`: optional dynamic signal-parameter governor
  profile set. It is disabled unless `--dynamic-params` is passed.

Config knobs include:

```text
early_required_edge
core_required_edge
early_to_core_age_sec
core_to_late_age_sec
dynamic_entry_enabled
fast_move_entry_start_age_sec
fast_move_min_abs_sk_usd
fast_move_required_edge
strong_move_entry_start_age_sec
strong_move_min_abs_sk_usd
strong_move_required_edge
late_entry_enabled
late_required_edge
late_max_spread
defensive_profit_min
protection_profit_min
profit_protection_start_remaining_sec
profit_protection_end_remaining_sec
defensive_take_profit_start_remaining_sec
defensive_take_profit_end_remaining_sec
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
polymarket_divergence_exit_bps
polymarket_divergence_exit_min_age_sec
retry_count
retry_interval_sec
buy_dynamic_buffer_enabled
buy_dynamic_buffer_attempt1_room_frac
buy_dynamic_buffer_attempt2_room_frac
buy_dynamic_buffer_attempt1_max_ticks
buy_dynamic_buffer_attempt2_max_ticks
buy_dynamic_buffer_min_reserved_edge
buy_dynamic_buffer_reserved_room_frac
batch_exit_enabled
batch_exit_min_shares
batch_exit_min_notional_usd
batch_exit_slices
batch_exit_extra_buffer_ticks
live_min_sell_shares
live_min_sell_notional_usd
polymarket_price_enabled
max_polymarket_price_age_sec
polymarket_stale_reconnect_sec
polymarket_unhealthy_log_after_sec
dvol_refresh_sec
max_dvol_age_sec
dvol_retry_interval_sec
dvol_retry_attempts
coinbase_enabled
```

The current `0.16/0.14` edge thresholds and `0.35` model-probability floor came
from replaying the 120-window collector and dry-run datasets captured on
2026-05-04. They replace the earlier `0.12/0.08` defaults because those looser
thresholds admitted too many low-probability lottery-style entries and larger
`logic_decay_exit` losses.

Binance is now the normal strategy baseline. Coinbase is disabled by default
and only participates when explicitly enabled. When both Binance/Coinbase live
prices are available, `cross_source_max_bps` can act as an entry quality gate.
If the two CEX sources disagree by more than the configured basis-point
threshold, the bot skips new entries with `source_divergence`. Binance-vs-
Polymarket divergence is diagnostic and risk context, not a hard entry block by
default, because that divergence may be the edge source.

For open positions, that same reference can become a risk exit. The current
default `polymarket_divergence_exit_bps=3.0` exits only when the divergence is
adverse to the held side and the position has been open for at least
`polymarket_divergence_exit_min_age_sec=3.0`. UP exits when
`Binance - Polymarket > 3 bps`; DOWN exits when `Binance - Polymarket < -3 bps`.
Set the threshold to `0` to disable this guard. This is separate from
`market_disagrees_exit`, which measures CLOB bid/model disagreement rather than
Polymarket reference divergence.

The probability helper clamps the pre-expiry `d2` time input to a minimum
`0.1s` only as a numerical guard against unstable `sqrt(T)` behavior. The bot
does not rely on sub-second model probabilities for live risk because new entry
is already closed and `final_force_exit` handles the final seconds.

The current `market_disagrees_exit_threshold` default is `0.25`, active for
positions with `remaining_sec <= 90`. A 12-window dry-run replay favored this
over the older `0.30 / 60s` guard, but it remains a candidate parameter that
needs larger-sample validation.

## FAK Price Logic

The bot keeps two separate prices for each candidate:

- `price`: current fresh best ask for BUY candidates. This is used for edge:
  `edge = model_prob - price`.
- `depth_limit_price`: for BUY this is intentionally the same as `best_ask`.
  The bot no longer pre-accumulates ask depth before posting FAK. For SELL this
  remains the deepest bid level required to sell the held shares.
- `limit_price`: formula-derived hard cap/floor used by execution. For BUY,
  `limit_price = model_prob - required_edge`. For SELL, it remains the
  executable bid floor returned by the exit quote.

For BUY, the formula probability acts as a maximum acceptable token price:

```text
fair_cap = model_prob - required_edge
edge = model_prob - best_ask
```

An entry is valid only when both are true:

```text
model_prob >= min_entry_model_prob
edge >= required_edge
best_ask + min_fair_cap_margin_ticks * tick_size <= fair_cap
```

The FAK buffer itself is not part of the edge calculation. It is an execution
hint capped by `fair_cap`; if the live book cannot fill at that capped hint, the
order becomes `order_no_fill` instead of widening beyond the model price.
`min_entry_model_prob` is a separate quality gate: it rejects low-probability
lottery-style entries even when their price discount is large.

Low-priced tokens can be guarded without banning them outright:

```text
if best_ask < low_price_extra_edge_threshold:
    effective_required_edge = phase_required_edge + low_price_extra_edge
```

Both values default to `0` in code, which disables the guard. The current
aggressive profile enables a light guard at `<0.30 + 0.02`, based on replaying
the combined 168-window paper sample. This is intentionally softer than a
minimum entry price: recent paper data showed low-priced tokens can contain both
the largest losses and the largest winners, so the current preference is to
require more edge rather than discard the whole bucket.

Live-oriented configs add a one-tick fair-cap margin:

```text
fair_cap - best_ask >= min_fair_cap_margin_ticks * tick_size
```

`ask_avg` and `ask_limit` are still logged for analysis, but they no longer gate
BUY entry. Collector-only runs may also emit `ask_safety_limit` and
`ask_depth_ok` as depth diagnostics. This is deliberate: CLOB sometimes
refreshes BBO without fresh depth snapshots. The strategy now trusts fresh BBO
plus model cap, then lets FAK/no-fill decide whether executable depth actually
exists.

Live FAK BUY price hinting uses best ask as the buffer base:

```text
price_hint = min(best_ask + buffer_ticks * tick_size, fair_cap)
```

Example:

```text
model_prob = 0.62
required_edge = 0.06
fair_cap = 0.56

best_ask = 0.50
tick_size = 0.01
buffer_ticks = 2
price_hint = min(0.50 + 0.02, 0.56) = 0.52
```

If enough depth exists up to the capped hint, FAK fills. If not, the bot records
`order_no_fill`; on retry it refreshes the full strategy snapshot and only
reposts if the same side still passes the entry rules.

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
attempt 1: min(best_ask + 2 ticks, fair_cap)
attempt 2: min(best_ask + 5 ticks, fair_cap)
```

If `buy_retry_price_buffer_ticks` is configured below
`buy_price_buffer_ticks`, the runtime clamps it up to the first-attempt buffer
and emits `buy_retry_price_buffer_ticks_clamped_to_buy_price_buffer_ticks` in
the config row. Retry is never allowed to become less aggressive than the first
BUY attempt.

SELL also gets one retry. In CLOB FAK semantics any visible bid at or above the
sell floor can fill, while bids below the floor are rejected. Normal
profit-taking and stop-loss exits use the same configurable aggressive floor:

```text
normal exits:
  market_overprice_exit / defensive_take_profit / profit_protection_exit
  logic_decay_exit / risk_exit / market_disagrees_exit
  polymarket_divergence_exit
  attempt 1: bid_limit - 5 ticks
  attempt 2: bid_limit - 6 ticks

final_force_exit:
  attempt 1: bid_limit - 5 ticks
  attempt 2: bid_limit - 10 ticks
```

Earlier versions used separate profit/stop floors. Live testing showed that even
profit exits can hit FAK no-match when the local book moves between WS snapshot
and POST, so normal exits now share a `5/6 tick` ladder. `final_force_exit`
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

## Batch Exit

Small positions still use the single-order SELL FAK path above. Larger
positions can use batch exit:

```yaml
batch_exit_enabled: true
batch_exit_min_shares: 20
batch_exit_min_notional_usd: 5
batch_exit_slices: [0.4, 0.3, 1.0]
batch_exit_extra_buffer_ticks: [0, 3, 6]
```

The trigger is based on position size, not only low entry price. A 10 USDC
position can still create many shares if the entry price is low enough, and
large share counts are harder to exit against thin late-window bid depth.

When triggered, live mode posts up to 15 pre-signed SELL FAK orders through
Polymarket's batch `/orders` path in a single request. With the default slices,
the bot tries 40%, 30%, then the remaining shares, while each later slice uses a
more aggressive floor. Paper mode mirrors this with local book simulation and
allows partial fills. If only part of a position sells, the bot records realized
PnL for the sold shares and keeps the remaining shares open; new entries remain
blocked until the position is fully closed.

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

- `logic_decay_exit`: model probability falls below entry cost by
  `model_decay_buffer`. Current configs use `0.03`, chosen over `0.02/0.04`
  after replay because it slightly reduced early false exits without materially
  increasing drawdown. Same-side cooldown now applies to all risk exits, so a
  thesis that was just invalidated cannot immediately re-open in the same
  direction.
- `market_overprice_exit`: executable bid is above model probability by `0.02`.
- `defensive_take_profit`: when
  `defensive_take_profit_start_remaining_sec < remaining_sec <= defensive_take_profit_end_remaining_sec`,
  profit is at least `defensive_profit_min`, and the held-side model
  probability has not risen over `prob_stagnation_window_sec`. Current configs
  use the classic `30s-60s` band.
- `prob_drop_exit`: when enabled, the held-side model probability drops by at
  least `prob_drop_exit_threshold` over `prob_drop_exit_window_sec` and is below
  the entry-time model probability. Current MVP/aggressive configs set
  `prob_drop_exit_window_sec=0` and `prob_drop_exit_threshold=0`, so this guard
  is disabled by default. Recent replay showed it overlapped with
  `market_disagrees_exit` and lowered win rate. The code is kept for future A/B
  tests, but normal risk control now relies on market disagreement, logic decay,
  and final-force exits. Because probability history is cleared at entry, this
  guard cannot fire until the position has been open for at least
  `prob_drop_exit_window_sec` when re-enabled.
- `market_disagrees_exit`: when enabled, the Polymarket executable bid becomes
  materially cheaper relative to the model than it was at entry. It is
  intentionally constrained to late, already-losing positions:
  `remaining_sec <= market_disagrees_exit_max_remaining_sec`, loss at least
  `market_disagrees_exit_min_loss`, position age at least
  `market_disagrees_exit_min_age_sec`, and no meaningful current profit. This
  catches cases where the model probability stays high but the market is
  increasingly unwilling to pay for that outcome.
- `profit_protection_exit`: configurable late profit guard active when
  `profit_protection_start_remaining_sec < remaining_sec <= profit_protection_end_remaining_sec`;
  the current 30-second final-force boundary normally supersedes most of this
  band.
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
sigma_source
volatility_stale
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
Tick rows include a compact `reference` object only when analysis logs are on,
while a position is open, or when an exit decision needs that reference context.
Idle long-running live ticks omit it to keep logs small. Full price-source
diagnostics are reserved for analysis/order rows.

Live runs can also emit `clob_prefetch_failed` during startup or window
switching. This means the latency-only CLOB metadata warmup failed on either
`get_tick_size` or `get_neg_risk`; the row includes `failed_operation` and the
bot continues without treating it as a strategy failure.

Before every entry or exit FAK call, the bot writes an `order_intent` row. This
row is emitted before awaiting the CLOB response and includes the token id,
side, signal price, fair cap or exit floor, and intended amount/shares. The
subsequent `entry`, `exit`, `position_reduce`, `dust_position`, or
`order_no_fill` row records the response. `position_reduce` means the bot
intentionally sold a safe balance below the full position and left a tiny
residual for a follow-up/dust path; it is not treated as an exceptional partial
fill. If `POST /order` times out after Polymarket has already matched the order,
the intent row still proves which order the bot attempted.

Live FAK responses include timing telemetry under `order.timing` when available:
`create_order_ms`, `post_order_ms`, `sent_at_epoch_ms`, `response_at_epoch_ms`,
and `wall_latency_ms`. This separates local signing/build time from the actual
CLOB `POST /order` round trip.

Very small residual live positions are treated as dust. If remaining sellable
shares are below `live_min_sell_shares` (default `0.01`) or below an optional
`live_min_sell_notional_usd` threshold, the bot does not call `POST /order`.
It emits `dust_position`, writes off the tiny residual at zero value, and keeps
the process alive. This avoids CLOB `invalid amounts, maker and taker amount
must be higher than 0` errors on sub-cent residual shares after safe balance
reductions.

BUY FAK hints use a dynamic fair-room buffer when
`buy_dynamic_buffer_enabled=true`. Instead of always posting `best_ask + N`
ticks, the bot computes `fair_room = fair_cap - best_ask`, reserves
`max(buy_dynamic_buffer_min_reserved_edge, fair_room *
buy_dynamic_buffer_reserved_room_frac)`, and posts:

```text
attempt 1 room = min(5 ticks, fair_room * 0.45)
attempt 2 room = min(8 ticks, fair_room * 0.65)
hint <= fair_cap - reserved_edge
```

This lets thick-edge entries compete more aggressively while keeping a reserved
edge cushion. If disabled, BUY hints fall back to the fixed
`buy_price_buffer_ticks` / `buy_retry_price_buffer_ticks` ladder.

When analysis logs are enabled, the bot first writes a `config` row containing
the non-secret strategy, execution, risk, and runtime parameters used for the run.
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
lead_binance_vs_polymarket_usd
lead_binance_vs_polymarket_bps
polymarket_divergence_bps
lead_coinbase_vs_polymarket_usd
lead_coinbase_vs_polymarket_bps
lead_proxy_vs_polymarket_usd
lead_proxy_vs_polymarket_bps
lead_binance_return_1s_bps / 3s / 5s
lead_coinbase_return_1s_bps / 3s / 5s
lead_polymarket_return_1s_bps / 3s / 5s
lead_binance_side / lead_coinbase_side / lead_proxy_side / lead_polymarket_side
lead_*_side_disagrees_with_polymarket
```

Null and `"missing"` analysis values are omitted. Binance fields appear in
normal runs because Binance is the model source. Polymarket fields appear when
the reference feed has data. Coinbase fields appear only when Coinbase is
explicitly enabled.

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
entry depth limit price (same as best ask for BUY)
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
settings, volatility settings, or dynamic-entry fast/strong-move toggles. The
`dynamic_entry_enabled`, `fast_move_*`, and `strong_move_*` fields are
startup-level strategy controls; if you want to test them, pass the config/CLI
choice at process start and evaluate it as a separate profile family.

Default behavior:

- Every 5 completed windows, it launches a background analysis task.
- The analysis uses the last 50 completed windows from the strategy JSONL.
- Health requires at least 20 closed trades, win rate at least 55%, and
  non-negative total PnL.
- Two consecutive failed health checks are required before a profile switch can
  be scheduled.
- Candidate profiles are replayed with 3-tick BUY/SELL slippage. This is an
  intentional robustness bias, not a claim that live FAK normally slips 3 ticks.
- Profile risk direction is controlled by each profile's explicit `risk_rank`;
  lower ranks are more aggressive. YAML order is not used for safety decisions.
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
