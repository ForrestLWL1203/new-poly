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
BTC DVOL once at startup by default and records it under `volatility`. See
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

- Entry thresholds are time phased: `0.10` for `40 <= age < 120`, `0.06` for
  `120 <= age < 240`, and late entry is disabled from `240s` onward.
- FAK entry decisions use size-aware `ask_avg` for edge, require
  `ask_limit <= model_prob - required_edge`, and send BUY hints as
  `min(ask_limit + tick_buffer, model_prob - required_edge)`.
- FAK BUY gets one capped retry; FAK SELL reposts once at the same
  `min_price` floor to catch changed book state without crossing lower.
- FAK exits use `bid_avg` / `bid_limit` for executable sell-depth checks.
- Exits include logic decay, market-overprice exits, final-60s defensive
  take-profit, final-30s profit protection, and final-15s forced risk exit.

Paper smoke test:

```bash
python3 scripts/run_prob_edge_bot.py --once
```

See [docs/prob_edge_strategy_bot.md](/Users/forrestliao/workspace/new-poly/docs/prob_edge_strategy_bot.md).

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
