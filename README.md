# New Poly

Fresh Polymarket trading/arbitrage bot workspace.

This repo intentionally starts strategy design from zero. The old project
`/Users/forrestliao/workspace/poly-bot` was used only to copy infrastructure
knowledge: Binance BTC data feed, Polymarket Gamma/CLOB APIs, CLOB WebSocket
order-book handling, authentication, and execution mechanics.

Old strategy knowledge has not been copied into this README.

## Knowledge Map

- [AGENTS.md](/Users/forrestliao/workspace/new-poly/AGENTS.md): guardrails for
  future agents and strategy clean-room rules.
- [docs/polymarket_api.md](/Users/forrestliao/workspace/new-poly/docs/polymarket_api.md):
  Polymarket and Binance API notes distilled from the old project.
- [docs/reusable_infrastructure.md](/Users/forrestliao/workspace/new-poly/docs/reusable_infrastructure.md):
  reusable strategy-neutral modules migrated from the old project.
- [docs/prob_edge_data_collector.md](/Users/forrestliao/workspace/new-poly/docs/prob_edge_data_collector.md):
  probability-edge data collector usage and JSONL schema notes.
- [docs/prob_edge_strategy_bot.md](/Users/forrestliao/workspace/new-poly/docs/prob_edge_strategy_bot.md):
  first probability-edge strategy bot usage and paper/live mode notes.
- [docs/prob_edge_backtest.md](/Users/forrestliao/workspace/new-poly/docs/prob_edge_backtest.md):
  offline replay backtest usage and interpretation limits.

## Core Infrastructure Notes

### AWS VPS

Current VPS:

```text
IP: 176.34.134.21
SSH user: ubuntu
PEM: /Users/forrestliao/workspace/new-poly/docs/LightsailDefaultKey-eu-west-1.pem
```

Connect:

```bash
ssh -i /Users/forrestliao/workspace/new-poly/docs/LightsailDefaultKey-eu-west-1.pem ubuntu@176.34.134.21
```

The PEM should be `600` or stricter. Some VPN exits fail even when TCP 22
connects; if SSH is closed during handshake, switch egress IP/route and retry.

Runtime on VPS:

```text
/opt/new-poly
/opt/new-poly/venv
/opt/new-poly/app
/opt/new-poly/shared
/opt/new-poly/logs
/opt/new-poly/data
```

Use:

```bash
/opt/new-poly/venv/bin/python
```

The VPS has Ubuntu 22.04, Python 3.10, a 2GB swapfile, and the baseline
Polymarket/Binance Python dependencies installed.

### Polymarket Account Config

The VPS account config is:

```text
/opt/new-poly/shared/polymarket_config.json
```

It is secret material with `600` permissions. Do not print or commit it. It was
copied from the local Polymarket CLI/account config and is used by CLOB auth.

### CLOB Smoke Test

Safe default probe:

```bash
ssh -i /Users/forrestliao/workspace/new-poly/docs/LightsailDefaultKey-eu-west-1.pem ubuntu@176.34.134.21 \
  '/opt/new-poly/venv/bin/python /opt/new-poly/app/probe_clob_light.py --side down --order-side buy --price 0.01 --size 1'
```

This discovers the current BTC 5m market, authenticates to CLOB, reads token
metadata/balances, and creates a signed order locally. It does not submit the
order unless `--post-intentional-fail` is explicitly provided.

### Probability Edge Data Collector

The current live observation script is:

```text
scripts/collect_prob_edge_data.py
```

It is a data collector only: no CLOB auth, no private keys, no order posting,
and no strategy entry/exit decisions. The old `scripts/prob_edge_dry_run.py`
entry point is only a compatibility wrapper.

Run locally:

```bash
python3 scripts/collect_prob_edge_data.py \
  --interval-sec 1 \
  --jsonl data/prob-edge-collector.jsonl \
  --sigma-eff 0.6 \
  --sigma-source manual \
  --windows 12
```

The script extracts the Polymarket UI Price to Beat from Polymarket's crypto
price API as `k_price`, then uses a Binance open-basis-adjusted proxy for
current `s_price`. It does not fetch or log Polymarket `closePrice`; simple
post-run direction checks should compare Binance window open/close prices.
It intentionally keeps `settlement_aligned=false` until a true
settlement-aligned realtime price source is available. It also fetches Deribit
BTC DVOL once at startup by default and records it under `volatility`.
`volatility_stale` is emitted so replay can ignore expired sigma snapshots. See
[docs/prob_edge_data_collector.md](/Users/forrestliao/workspace/new-poly/docs/prob_edge_data_collector.md).

### Probability Edge Strategy Bot

The first strategy robot entry point is:

```text
scripts/run_prob_edge_bot.py
```

It defaults to `paper` mode and uses one shared strategy state machine for both
paper and live runs. Live mode will not post orders unless both `--mode live`
and `--i-understand-live-risk` are provided.

Current default strategy behavior:

- Entry thresholds are time phased: `0.12` for `90 <= age < 120`, `0.08` for
  `120 <= age < 240`, and late entry is disabled from `240s` onward.
- FAK entry decisions use size-aware `ask_avg` for edge, require
  `ask_limit <= model_prob - required_edge`, require the formula cap to leave
  at least one tick of executable margin, and send BUY hints as
  `min(ask_limit + configured_tick_buffer, model_prob - required_edge)`.
- Entry depth uses a safety multiplier in live-oriented configs: the bot may
  buy `amount_usd`, but requires enough ask depth for
  `amount_usd * depth_safety_multiplier` inside the same formula cap.
- FAK BUY gets one capped retry. The default live BUY hint ladder is
  `+2 ticks` then `+4 ticks`, always capped by formula fair cap. FAK SELL also
  retries once, but the sell floor
  depends on exit urgency: normal profit/stop exits use `-3 ticks` then
  `-5 ticks`, and `final_force_exit` uses a fixed `-5/-10 tick` emergency
  ladder. Paper mode uses the same SELL floors, clamped at one tick for very
  low-priced tokens.
- A live CLOB `FAK no match` response is treated as `order_no_fill`, not a
  fatal bot error, and records the failed POST latency for later diagnostics.
- FAK exits use `bid_avg` / `bid_limit` for executable sell-depth checks.
- Exits include logic decay, market-overprice exits, final-60s defensive
  take-profit, final-30s profit protection, and final-15s forced risk exit.
- Live CLOB auth uses one cached `ClobClient` and configures the SDK HTTP
  client with `http2`, a larger keep-alive pool, and explicit timeouts to avoid
  waiting on connection setup during FAK posting.

Paper smoke test:

```bash
python3 scripts/run_prob_edge_bot.py --once
```

Paper strategy runs print analysis logs by default. Live mode defaults analysis
logs off; add `--analysis-logs` during live debugging or `--no-analysis-logs`
for compact long-running paper logs.

Current parameter files:

- `configs/prob_edge_mvp.yaml`: conservative baseline/default config.
- `configs/prob_edge_aggressive.yaml`: current optimized aggressive paper
  candidate from the first 96-window replay. It uses `100-240s` entry timing,
  `0.14/0.12` early/core edge thresholds, `max_entries_per_market=4`, and
  `$1` paper notional/depth.
- `configs/prob_edge_dynamic.yaml`: optional dynamic signal-parameter governor
  profiles and health thresholds. It only changes entry timing/edge/max-entry
  settings, and only at window boundaries.

Longer aggressive paper run:

```bash
python3 scripts/run_prob_edge_bot.py \
  --config configs/prob_edge_aggressive.yaml \
  --mode paper \
  --windows 48 \
  --jsonl data/prob-edge-bot-paper-aggressive-48w.jsonl
```

Longer aggressive paper run with dynamic risk governor:

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

For the first dynamic-parameter test, prefer `--windows 120`: the governor uses
the last 50 complete windows and checks every 5 windows, so 120 windows gives
enough room to observe multiple health checks and any `config_update` events.
Review `dynamic_check`, `config_update`, and `dynamic_error` rows before trying
dynamic parameters in live mode.
Dynamic mode requires `--jsonl` because the strategy log is used as the replay
input. It only moves toward equal-or-more-conservative profiles; returning to a
more aggressive profile requires resetting or editing the dynamic state file.

Strategy JSONL logs are pruned by timestamp by default:

- Default retention: 24 hours.
- Pruning runs at startup and every 5 completed windows by default.
- Disable pruning with `--log-retention-hours 0`.
- Dynamic parameter analysis needs retention to cover its lookback. The default
  `24h` retention covers about 288 BTC 5m windows, above the default 50-window
  dynamic lookback.

See [docs/prob_edge_strategy_bot.md](/Users/forrestliao/workspace/new-poly/docs/prob_edge_strategy_bot.md).

### Probability Edge Backtest

Replay collector JSONL through the current strategy state machine:

```bash
python3 scripts/backtest_prob_edge.py \
  --jsonl data/prob-edge-collector-96-20260503T162542Z.kprice-ok.jsonl
```

The backtest uses collector summary fields (`ask_avg`, `ask_limit`, `bid_avg`,
`bid_limit`) rather than full order-book replay, so it is suitable for
parameter screening and strategy-shape validation, not exact fill simulation.
Add `--slippage-ticks N` to simulate FAK fills moving by `N` price ticks
(`0.01` per tick by default) against both BUY and SELL.
For win-rate-first scans, use `--grid-sort-by win_rate --grid-min-entries N`;
the summary also reports skip reason counts and uncertain settlement counts.

### Reusable Modules

Strategy-neutral infrastructure migrated from the old project now lives under
`new_poly/`:

```text
new_poly/market/binance.py
new_poly/market/market.py
new_poly/market/series.py
new_poly/market/stream.py
new_poly/market/deribit.py
new_poly/trading/fak_quotes.py
```

Future strategy dry-run, backtest, and live execution scripts should reuse
these modules instead of reimplementing Binance feeds, Polymarket window
discovery, CLOB WebSocket parsing, or depth quote selection.

### Binance BTC Source

- Use Binance trade WebSocket as a low-latency BTC source:
  `wss://stream.binance.com:9443/ws/btcusdt@trade`
- Parse trade price from field `p`.
- Keep a rolling timestamped price history so later strategy code can ask for:
  latest price, price at or before a timestamp, and first price at or after a
  timestamp.
- REST fallback for missing 1-minute open:
  `GET https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=<epoch_ms>&limit=1`

### Polymarket Discovery

- Gamma markets endpoint: `https://gamma-api.polymarket.com/markets`
- BTC 5-minute slugs are of the form `btc-updown-5m-<window_start_epoch>`.
- Slug step is 300 seconds.
- `clobTokenIds` can be JSON text or a list.
- For the observed UP/DOWN markets, token index 0 is Up/Yes and token index 1
  is Down/No.

### Polymarket CLOB API

- Base URL: `https://clob.polymarket.com`
- Python SDK dependency used previously: `py-clob-client-v2==1.0.0`
- Polygon chain ID: `137`
- Proxy/Magic wallet signature type: `1`
- Useful SDK calls:
  - `derive_api_key()` / `create_api_key()` then `set_api_creds(...)`
  - `get_midpoint(token_id)`
  - `get_tick_size(token_id)`
  - `get_neg_risk(token_id)`
  - `get_balance_allowance(...)`
  - `create_market_order(...)`
  - `post_order(...)`
- `MarketOrderArgs` amount semantics:
  - BUY amount is dollars to spend.
  - SELL amount is token shares.
- FAK orders can partially fill. Handle partial fills and missing fill fields.
- Balance API returns 6-decimal integer shares. Convert with
  `float(balance) / 1_000_000`.

### Polymarket CLOB WebSocket

- URL: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- Subscribe with `assets_ids` and `operation=subscribe`.
- Send `{}` about every 10 seconds as heartbeat.
- Important event types:
  - `book`: full L2 snapshot
  - `price_change`: incremental depth update
  - `best_bid_ask`: top-of-book update
  - `last_trade_price`: executed trade price
  - `tick_size_change`: tick-size notification
- Maintain local L2 books per token:
  - bids sorted high to low
  - asks sorted low to high
  - local `received_at` monotonic timestamp

## Explicit Non-Goals For The First Copy

The following were deliberately left behind:

- Old entry timing windows.
- Old thresholds, caps, stop-loss rules, and signal formulas.
- Old strategy names and backtest results.
- Old run presets and VPS workflow details.

New strategy work should be introduced only by direct instruction in this repo.
