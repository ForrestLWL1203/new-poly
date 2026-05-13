# AGENTS.md - New Polymarket Arbitrage Bot

## Project Intent

This is a fresh project for a new Polymarket trading/arbitrage bot.

The old project at `/Users/forrestliao/workspace/poly-bot` is only a reference
for infrastructure knowledge:

- Binance BTC WebSocket data feed usage.
- Polymarket Gamma API market/token discovery.
- Polymarket CLOB API authentication, balances, tick sizes, and order posting.
- Polymarket CLOB WebSocket market channel and order-book cache handling.
- General retry/reconnect hygiene for production data feeds.

Do not copy or reintroduce old strategy logic from `paired_window`, `crowd_m1`,
or any historical BTC 5-minute strategy. Strategy design in this repo starts
from zero.

## Current Active Strategy Status

The active runtime strategy is now the Polymarket live-data single-source
strategy:

- Active config: `configs/prob_poly_single_source.yaml`.
- Active strategy logic: `new_poly/strategy/poly_source.py`.
- `new_poly/strategy/prob_edge.py` contains shared DTOs only; the old
  dual-source probability-edge strategy logic has been removed.
- Runtime entry decisions do not use Binance/Coinbase `S`, `model_prob`,
  `required_edge`, Black-Scholes probability, aggressive profiles, or dynamic
  parameter profiles.
- Binance/Coinbase/proxy helpers remain available for data collection,
  diagnostics, and strategy-neutral infrastructure.

Any older section below that refers to `configs/prob_edge_aggressive.yaml`,
`configs/prob_edge_mvp.yaml`, `configs/prob_edge_dynamic.yaml`, `model_prob`,
`required_edge`, `market_disagrees_exit`, or Binance-as-model strategy behavior
is historical context only unless explicitly updated to mention
`poly_single_source`.

## Clean-Room Strategy Rule

Allowed to reuse:

- API endpoints and protocol details.
- Authentication flow and SDK usage patterns.
- WebSocket subscription formats.
- Order-book event parsing and local book-cache mechanics.
- Safe operational notes such as fresh-book checks, reconnect backoff, tick-size
  rounding, and balance lookup before selling.

Do not reuse unless explicitly requested:

- Old entry/exit windows.
- Old thresholds, caps, signal formulas, persistence rules, or stop-loss rules.
- Old backtest results, parameter grids, candidate names, or PnL claims.
- Old VPS run presets or strategy-specific commands.

## Infrastructure Facts

### AWS VPS Access

Current AWS/Lightsail VPS:

- Public IP: `176.34.134.21`
- SSH user: `ubuntu`
- Local PEM path:
  `/Users/forrestliao/workspace/new-poly/docs/LightsailDefaultKey-eu-west-1.pem`
- Required PEM permissions: `600` or stricter.
- Verified SSH command:

```bash
ssh -i /Users/forrestliao/workspace/new-poly/docs/LightsailDefaultKey-eu-west-1.pem \
  ubuntu@176.34.134.21
```

Observed network behavior:

- Some VPN/proxy exit IPs can reach TCP 22 but are closed before or during SSH
  handshake.
- Verified working exits during setup included a Germany route and a Hong Kong
  route.
- A Poland route and one Tokyo route were observed failing with
  `Connection closed by 176.34.134.21 port 22`.
- If SSH starts failing, first check current egress IP and route before changing
  VPS settings.

### AWS Runtime Layout

VPS base OS and runtime:

- Ubuntu `22.04.5 LTS`
- Python `3.10.12`
- Project root: `/opt/new-poly`
- Virtualenv: `/opt/new-poly/venv`
- App scripts: `/opt/new-poly/app`
- Shared secrets/config: `/opt/new-poly/shared`
- Logs: `/opt/new-poly/logs`
- Data: `/opt/new-poly/data`
- Swap: `/swapfile`, 2 GB, enabled and persisted in `/etc/fstab`

Use the venv explicitly:

```bash
/opt/new-poly/venv/bin/python
/opt/new-poly/venv/bin/pip
```

Installed system packages include Python venv/pip/dev headers, build tools,
`git`, `curl`, `jq`, `libffi-dev`, and `libssl-dev`.

Installed Python packages include:

```text
py-clob-client-v2==1.0.0
python-dotenv
eth-account
eth-utils
requests
httpx
websockets
pyyaml
pytest
pytest-asyncio
```

### Sweden VPS Access

Current Sweden VPS:

- Public IP: `70.34.207.45`
- SSH user: `root`
- Authentication is password-based. Do not print the password in logs or docs;
  use `SSHPASS` or an interactive prompt when automation is required.
- Local ignored password file:
  `/Users/forrestliao/workspace/new-poly/docs/sweden-vps-secret.txt`.
  This file is under ignored `docs/`; keep permissions at `600` and never copy
  it into tracked files or command output.
- When the user says "VPS" without specifying Ireland/AWS, default to this
  Sweden VPS.
- Do not waste time trying the Ireland PEM key against this host. Public-key
  auth has been observed failing with `Permission denied (publickey,password)`;
  use the password-based path directly when the user has authorized remote
  access.
- If using `sshpass`, pass the password through the `SSHPASS` environment
  variable and keep commands read-only unless the user asked to deploy/start/stop
  a run. Never echo the password or write it into tracked files.

Runtime layout:

- Project repo: `/opt/new-poly/repo`
- Virtualenv: `/opt/new-poly/venv`
- Shared secrets/config: `/opt/new-poly/shared/polymarket_config.json`
- Logs: `/opt/new-poly/logs`

Useful read-only status checks:

```bash
SSHPASS="$(cat /Users/forrestliao/workspace/new-poly/docs/sweden-vps-secret.txt)" \
  sshpass -e ssh root@70.34.207.45 'pgrep -af "run_prob_edge_bot|collect_prob_edge_data" || true'
SSHPASS="$(cat /Users/forrestliao/workspace/new-poly/docs/sweden-vps-secret.txt)" \
  sshpass -e ssh root@70.34.207.45 'ls -lt /opt/new-poly/logs | head -20'
```

Use the venv explicitly:

```bash
/opt/new-poly/venv/bin/python
/opt/new-poly/venv/bin/pip
```

Sweden has been used for recent live/paper runs because its CLOB order-post
latency has been comparable to Ireland and sometimes easier to access. Keep the
same secret-handling rules: never print `/opt/new-poly/shared/polymarket_config.json`.

### Polymarket Account Config

Sensitive account config is present on the VPS at:

```text
/opt/new-poly/shared/polymarket_config.json
```

Permissions are `600`; do not print, commit, or copy its contents into docs.
The file was copied from local Polymarket CLI config and contains fields such as
`private_key`, `proxy_address`, `chain_id`, and `signature_type`.

Local source paths used during setup:

```text
/Users/forrestliao/.config/polymarket/config.json
/Users/forrestliao/.polybot/accounts/newuser_poly.json
```

Treat both as secret material.

### Lightweight CLOB Probe

Safe CLOB smoke-test script:

```text
local: /Users/forrestliao/workspace/new-poly/scripts/probe_clob_light.py
VPS:   /opt/new-poly/app/probe_clob_light.py
```

Default mode does not submit an order. It:

- discovers the current BTC 5-minute UP/DOWN market using Gamma,
- initializes CLOB auth,
- reads midpoint, tick size, neg-risk, and balance for both tokens,
- creates a signed local FAK order,
- exits without calling `POST /order`.

Safe command:

```bash
ssh -i /Users/forrestliao/workspace/new-poly/docs/LightsailDefaultKey-eu-west-1.pem \
  ubuntu@176.34.134.21 \
  '/opt/new-poly/venv/bin/python /opt/new-poly/app/probe_clob_light.py --side down --order-side buy --price 0.01 --size 1'
```

The script also has `--post-intentional-fail`, which really calls
`POST /order` with an intentionally non-marketable FAK limit order. Use that
flag only when the user explicitly asks for an order-posting probe.

### Probability Edge Data Collector

Current live data collection script:

```text
local: /Users/forrestliao/workspace/new-poly/scripts/collect_prob_edge_data.py
VPS:   /opt/new-poly/app/collect_prob_edge_data.py
```

`scripts/prob_edge_dry_run.py` is only a compatibility wrapper for the collector.

The collector does not authenticate to CLOB, does not read private keys or API
credentials, does not submit orders, and does not evaluate strategy entry/exit
rules. It emits compact JSONL, one row per data tick, with enough fields for
later strategy dry-run and backtest code:

- market slug, start/end time, `window_bucket`, remaining seconds,
- Polymarket resolution source,
- Polymarket crypto price API `openPrice` as `k_price` / Price to Beat,
- Binance BTC/USDT trade price as the primary model `s_price`,
- Polymarket live-data `crypto_prices_chainlink` price as settlement-source
  reference / divergence diagnostic, not the normal model `S`,
- Coinbase price only when explicitly enabled for backup or multi-source
  diagnostics,
- `basis_bps`,
- Binance/Coinbase live spread diagnostics for source-divergence analysis when
  Coinbase is enabled,
- compact UP/DOWN book summaries,
- YES+NO sum monitoring,
- data-quality `warnings`.

Do not add strategy fields such as `decision`, `candidate_side`, `skip_reason`,
`edge`, `required_edge`, or PnL to the collector. Those belong in future
strategy dry-run/backtest scripts.

Safe local command:

```bash
python3 /Users/forrestliao/workspace/new-poly/scripts/collect_prob_edge_data.py \
  --interval-sec 1 \
  --jsonl /Users/forrestliao/workspace/new-poly/data/prob-edge-collector.jsonl \
  --sigma-eff 0.6 \
  --sigma-source manual \
  --windows 12
```

Safe VPS command after copying the script:

```bash
/opt/new-poly/venv/bin/python /opt/new-poly/app/collect_prob_edge_data.py \
  --interval-sec 1 \
  --jsonl /opt/new-poly/data/prob-edge-collector.jsonl \
  --sigma-eff 0.6 \
  --sigma-source manual \
  --windows 12
```

By default the collector runs until interrupted. Use `--windows N` to stop after
observing `N` windows that successfully obtained `k_price`.

Copy to VPS:

```bash
scp -i /Users/forrestliao/workspace/new-poly/docs/LightsailDefaultKey-eu-west-1.pem \
  /Users/forrestliao/workspace/new-poly/scripts/collect_prob_edge_data.py \
  ubuntu@176.34.134.21:/opt/new-poly/app/collect_prob_edge_data.py
```

Current status:

- `k_price` is extracted from Polymarket crypto price API `openPrice`.
- `k_source` is `polymarket_crypto_price_api`.
- `S` normally comes from Binance BTC/USDT WebSocket. This is intentional: the
  current strategy is testing whether CEX price movement leads Polymarket CLOB
  repricing.
- Polymarket live-data WebSocket is still started as a reference source:
  `wss://ws-live-data.polymarket.com`, topic `crypto_prices_chainlink`, filter
  `{"symbol":"btc/usd"}`.
- The strategy can use Binance-vs-Polymarket reference divergence as a
  position risk exit. Current configs use `polymarket_divergence_exit_bps=3.0`
  and `polymarket_divergence_exit_min_age_sec=3.0`; this is an exit guard, not
  an entry gate.
- A 2026-05-06 three-window probe showed Polymarket live-data boundary ticks
  exactly matching the crypto price API `openPrice`/`closePrice`.
- The Polymarket reference feed stores a short rolling history, currently about
  15 seconds. That is enough for latest-price and short-horizon divergence
  checks.
- If Polymarket live-data is missing or stale, the Polymarket WS feed closes and
  reconnects after `polymarket_stale_reconnect_sec` without a valid price tick,
  currently `5s`. Binance can continue to supply the model price, but the
  missing/stale Polymarket reference should be visible in logs.
- Coinbase is disabled by default in current configs. Do not enable it unless a
  run explicitly needs multi-source diagnostics.

### Probability Edge Strategy Bot

Main entrypoint:

```text
scripts/run_prob_edge_bot.py
```

Current architecture:

- `new_poly/bot_loop.py` owns the higher-level bot loop.
- `new_poly/bot_runtime.py` owns config loading, logging helpers, snapshots,
  settlement helpers, and volatility retry helpers.
- `new_poly/strategy/prob_edge.py` is IO-free strategy logic.
- `new_poly/trading/execution.py` contains paper/live execution gateways.

Default live-oriented profile:

```text
configs/prob_edge_aggressive.yaml
```

Current tuned strategy shape:

- Binance BTC/USDT WebSocket is the primary model `S`.
- Polymarket crypto price API `openPrice` is `K`.
- Polymarket live-data Chainlink stream is a reference/risk source, not the
  normal model `S`.
- Coinbase is disabled by default.
- New entries normally use `entry_start_age_sec=100`,
  `entry_end_age_sec=240`, `early_required_edge=0.16`,
  `core_required_edge=0.14`.
- The aggressive config is aggressive by entry count but stricter by entry
  quality. It currently uses `min_entry_model_prob=0.55`,
  `max_entries_per_market=2`, `low_price_extra_edge_threshold=0.30`, and
  `low_price_extra_edge=0.04`.
- The aggressive config also filters weak-distance mid-priced entries:
  when `best_ask > 0.35` and `abs(S-K) < 2bps`, the bot skips entry with
  `weak_sk_distance`. This targets tickets that are no longer cheap while the
  Binance model price remains too close to Polymarket's Price to Beat.
- Recent live analysis showed many large losses came from low-certainty
  "cheap ticket" entries where `model_prob` was near `0.50` but edge looked
  large because the ask was low. Backtests on recent live/paper/collector logs
  suggest raising `min_entry_model_prob` toward `0.55-0.60` improves win rate
  and drawdown at the cost of fewer trades. Treat this as the next parameter
  family to validate before widening entry windows or lowering edge thresholds.
- The aggressive config also enables price/probability BUY cap relaxation after
  a normal edge signal passes: low-priced tickets (`ask<=0.25`, `prob>=0.40`)
  can use up to `+8` ticks, mid-priced tickets (`ask<=0.65`, `prob>=0.60/0.75`)
  can use up to `+8/+10` ticks, and high-priced tickets only relax when
  `prob>=0.95`, capped at `+4` ticks. This improves FAK fill rate without
  broadly relaxing low-quality entries.
- Run directly from the committed config unless the user explicitly asks for a
  temporary YAML override. Check the actual run config in
  `/opt/new-poly/logs/<run-id>.yaml` before comparing logs.
- `prob_drop_exit` is disabled by default because `market_disagrees_exit` and
  Polymarket divergence exits now cover the main observed failure mode.
- `defensive_take_profit` is configurable but disabled in current MVP/aggressive
  configs. If the model probability has not clearly deteriorated and no
  market-disagreement/divergence guard fires, late profitable positions should
  usually keep their settlement upside instead of selling only because the
  probability has stalled.

Risk exits:

- `logic_decay_exit`: model probability falls below entry price minus
  `model_decay_buffer`.
- `market_disagrees_exit`: CLOB executable bid falls below a configured
  fraction of the entry price while model probability has dropped by at least
  `market_disagrees_exit_min_model_drop`. Current live configs use
  `market_disagrees_exit_threshold=0.48`, so a `0.42` entry can exit near
  `0.20` instead of waiting for the slower `logic_decay_exit`. CLOB price
  collapse alone still should not force an exit while the model thesis is
  mostly intact.
- `polymarket_divergence_exit`: Binance-vs-Polymarket reference divergence is
  adverse for the held side.
- `final_force_exit`: last-stage risk reduction before settlement.
- `risk_exit`: missing/stale model or book inputs.

Execution behavior:

- Live mode requires both `--mode live` and `--i-understand-live-risk`.
- BUY amount is USDC notional; SELL amount is shares.
- One position per market is allowed. If a SELL fails and a position remains
  open, new entries are blocked until the position is closed or settled.
- BUY FAK uses the current best ask plus a configured tick ladder, capped by
  `fair_cap`; in aggressive config that cap may be relaxed by the
  price/probability buckets above. It no longer reserves extra fair-room beyond
  the cap.
- BUY retry is a second attempt for the same signal. It does not re-run the
  full strategy signal refresh between attempts.
- SELL refreshes sell parameters from the latest local book immediately before
  the first POST and again before retries, then uses more aggressive buffers for
  risk/force exits.
- Unknown or timed-out FAK responses must be reconciled by balance before
  retrying or declaring failure. A timeout does not prove the order failed.
- CLOB `POST /order` can return transient `425 service not ready`; treat it as
  a recoverable order no-fill / request exception, not as a fatal bot crash.
- Important live accounting caveat: a successful SELL POST response may contain
  a usable fill size while the immediate token balance query still shows only a
  tiny decrease. When response status is matched and a nonzero fill can be
  derived from response fields, prefer the response fill for state accounting
  and use balance/trades as diagnostics. Use balance reconciliation as the
  primary source only for unknown/timeout responses. This avoids creating fake
  residual positions that later produce noisy
  `live sell balance unavailable; reconciliation no balance decrease` rows.
- Safe balance reductions with a residual position are logged as
  `position_reduce`; tiny residuals below live minimum sell size can finish via
  `dust_position`.
- Current CLOB HTTP helper timeout is short but allows slow FAK responses:
  total/read/write `5.0s`, connect `0.5s`, pool `0.2s`. A timeout still does
  not prove the order failed; reconcile by balance/trades before deciding.
- Live no-sellable-balance for a token position should not stop the whole bot;
  account-level insufficient USDC/funds can stop the bot.

Logging:

- Analysis-heavy fields should be emitted for paper/dry-run and short live
  diagnostics, not as permanent noisy live logs.
- Log entry lifecycle as `order_intent` before BUY POST and then the resulting
  `entry` or entry `order_no_fill` event. Log SELL attempts as `exit_intent`
  before POST so buy-intent/fill-rate statistics do not mix with normal
  stop/take-profit exits. Exit responses can be `position_reduce`, `exit`,
  `order_no_fill`, `dust_position`, or fatal/error events.
- Do not log signed orders, private keys, API credentials, full account config,
  or full order books.

Run log naming:

- Use a single filename shape for saved live/paper/dry-run/collector logs:
  `<mode>-<region>-<windows>w-<YYYYMMDDTHHMMSSZ>.<ext>`.
- `mode` should be `live`, `paper`, `collector`, or `probe`; use `paper` for
  strategy dry-runs that do not POST real orders.
- `region` should be `sweden`, `ireland`, `local`, or another explicit runtime
  location.
- The timestamp is UTC run start time. If an old run only has minute precision,
  normalize seconds to `00`.
- Keep companion files on the same stem, e.g. `.jsonl`, `.out`, `.yaml`, `.tgz`.
- Do not add ad-hoc labels such as `dynamic-sell`, `reconcile`, `posreduce`, or
  `analysis` to filenames. Put that context in a report, note, or the log
  contents instead.
- Preferred local locations:
  `data/live_runs/live-<region>-<windows>w-<timestamp>.jsonl` for real live
  runs, and `data/live_runs/paper-<region>-<windows>w-<timestamp>.jsonl` for
  paper strategy runs unless a task explicitly asks for another directory.

### Reusable Infrastructure Modules

Strategy-neutral modules migrated from the old project live under:

```text
new_poly/market/binance.py
new_poly/market/coinbase.py
new_poly/market/polymarket_live.py
new_poly/market/market.py
new_poly/market/series.py
new_poly/market/stream.py
new_poly/market/prob_edge_data.py
new_poly/market/deribit.py
new_poly/trading/fak_quotes.py
```

Future strategy scripts should reuse these for Binance model pricing,
Polymarket live-data reference pricing, optional Coinbase diagnostics,
Polymarket window discovery, CLOB WebSocket book handling, Binance 1-minute
realized-volatility snapshots, Deribit DVOL fallback snapshots, and FAK
quote/depth selection. Do not reuse old `poly-bot` strategy modules or old
thresholds.

- Shared collector/bot helpers such as `WindowPrices`, K refresh,
  boundary-open refresh, effective price calculation, token depth summaries,
  and BTC 5m window rollover live in `new_poly/market/prob_edge_data.py`.
  Entry scripts should import that module instead of importing from
  `scripts/collect_prob_edge_data.py`.
- Raw proxy model price formula:
  use only sources that have both a live price and same-window open price;
  `proxy_live = mean(valid paired live prices)`;
  `proxy_open = mean(valid paired open prices)`;
  `basis_bps = (proxy_open - k_price) / k_price * 10000`;
  `s_price = mean(live proxy sources)`. The basis is diagnostic only and must
  not be applied to the model `S`; this strategy uses Binance as the leading
  signal for Polymarket CLOB repricing.
- `price_source` is normally `proxy_binance` unless Coinbase diagnostics are
  explicitly enabled. `polymarket_price` is a reference field.
- Strategy volatility defaults to Binance 1-minute realized volatility:
  `volatility_source=binance_rv`, `rv_lookback_minutes=60`, `rv_refresh_sec=60`.
  It fetches one Binance kline response with 61 candles, computes close-to-close
  EWMA RV and Parkinson high-low RV, annualizes them, and uses the larger
  clamped value as `sigma_eff`.
- Deribit BTC DVOL remains a fallback when `dvol_fallback_enabled=true`.
- Strategy startup must obtain a valid volatility snapshot before entering the
  main loop. Runtime refresh failures must not overwrite the last valid snapshot
  with an empty one; keep using the previous sigma until `max_dvol_age_sec`
  marks it stale.
- `settlement_aligned` means the Polymarket reference source is available and
  the resolution source looks like Chainlink BTC/USD; it does not mean the model
  `S` came from Polymarket.
- The script should be used for data collection and paper analysis, not live
  order decisions.

### Dependencies

Known useful Python dependencies from the previous implementation:

```text
py-clob-client-v2==1.0.0
python-dotenv>=1.0.0
eth-account>=0.13.0
eth-utils>=4.1.1
requests>=2.31.0
httpx>=0.27
websockets>=12.0
pyyaml>=6.0
pytest>=8.0
pytest-asyncio>=0.23
```

### Binance BTC Trade Feed

- WebSocket URL template: `wss://stream.binance.com:9443/ws/{symbol}@trade`
- For BTC/USDT trades, use symbol `btcusdt`.
- Trade payload price field is `p`.
- The old feed stored `(local_time, price)` in a rolling deque and pruned by
  age.
- Useful methods for a strategy-neutral price feed:
  - latest price
  - first price at or after a timestamp
  - price at or before a timestamp
- REST fallback for a missing open price used:
  - `GET https://api.binance.com/api/v3/klines`
  - params: `symbol=BTCUSDT`, `interval=1m`, `startTime=<epoch_ms>`, `limit=1`
  - open price is response item `[0][1]`

### Coinbase BTC Trade Feed

- WebSocket URL: `wss://ws-feed.exchange.coinbase.com`
- Product: `BTC-USD`
- Subscribe with `{"type":"subscribe","product_ids":["BTC-USD"],"channels":["matches"]}`.
- Match payload price field is `price`.
- REST fallback for a missing open price uses:
  - `GET https://api.exchange.coinbase.com/products/BTC-USD/candles`
  - params: `granularity=60`, `start=<ISO>`, `end=<ISO+60s>`
  - candle shape is `[time, low, high, open, close, volume]`; open is item `[3]`.

### Polymarket API Systems

| API | Base URL | Purpose |
|---|---|---|
| CLOB | `https://clob.polymarket.com` | Trading, order book, balances, tick sizes |
| Gamma | `https://gamma-api.polymarket.com` | Market/event metadata and slug discovery |
| Data | `https://data-api.polymarket.com` | Positions, activity, history |

### CLOB Authentication

- Runtime SDK: `py-clob-client-v2==1.0.0`.
- Chain ID: `137` for Polygon mainnet.
- Signature types:
  - `0` = EOA
  - `1` = proxy/Magic wallet
  - `2` = Gnosis Safe
- The old project used signature type `1` for Polymarket proxy wallets.
- A `ClobClient` needs host, private key, chain ID, signature type, and funder.
- For proxy wallets, funder should be the proxy contract address when known.
- API credentials are required for L2 trading auth. The old flow first tried
  `derive_api_key()` and used `create_api_key()` only if derive returned none,
  then called `client.set_api_creds(creds)`.
- The current project caches a single `ClobClient` process-wide and configures
  the SDK HTTP helper client with `http2`, `max_connections=100`,
  `max_keepalive_connections=20`, `keepalive_expiry=30s`, and short trading
  timeouts (`total=5s`, `connect=0.5s`, `pool=0.2s`).
- The SDK HTTP helper mutation is process-global. This project currently
  assumes one bot/account per process; future multi-strategy same-process
  runners need a client-factory refactor.
- Existing CLI config may live at `~/.config/polymarket/config.json`.

### Environment Variables

Useful names:

```text
PK=0x...
FUNDER=0x...
CLOB_API_KEY=
CLOB_SECRET=
CLOB_PASS_PHRASE=
CLOB_API_URL=https://clob.polymarket.com
CHAIN_ID=137
HTTPS_PROXY=
```

Do not commit real private keys, account configs, or API credentials.

### Gamma Market Discovery

- BTC 5-minute market slugs use epoch starts, e.g. `btc-updown-5m-<epoch>`.
- The slug step is 300 seconds.
- Query exact market by slug with:
  - `GET https://gamma-api.polymarket.com/markets?slug=<slug>`
- Useful fields from Gamma market response:
  - `slug`
  - `question`
  - `active`
  - `closed`
  - `endDate`
  - `eventStartTime`
  - `clobTokenIds`
- `clobTokenIds` may be a JSON string or a list.
- For UP/DOWN markets observed in the old project:
  - token index 0 = Up/Yes
  - token index 1 = Down/No

### BTC 5m Price-To-Beat / K Source

For current BTC 5-minute UP/DOWN markets, Gamma and Polymarket pages show:

```text
resolutionSource = https://data.chain.link/streams/btc-usd
```

The market resolves Up if the BTC/USD price at the end of the five-minute
window is greater than or equal to the price at the beginning of that window.

Gamma market metadata does not directly expose `k_price`. The Polymarket event
page UI shows the target price / Price to Beat, and the same value is available
from Polymarket's crypto price API:

```text
GET https://polymarket.com/api/crypto/crypto-price?symbol=BTC&eventStartTime=<start>&variant=fiveminute&endDate=<end>
```

Use that `openPrice` as the collector `K` value. Do not extract `K` from
`question` or `description`; current BTC 5m descriptions describe the rule but
do not include the numeric target price.

The Chainlink website may show delayed informational prices. Do not compare the
current Chainlink webpage display directly with Polymarket Price to Beat and
assume they are the same timestamp.

### Polymarket CLOB WebSocket Market Feed

- URL: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- Subscribe:

```json
{
  "type": "market",
  "assets_ids": ["<token_id_1>", "<token_id_2>"],
  "operation": "subscribe",
  "custom_feature_enabled": true
}
```

- Unsubscribe:

```json
{
  "assets_ids": ["<token_id_1>", "<token_id_2>"],
  "operation": "unsubscribe"
}
```

- Heartbeat: send `{}` about every 10 seconds.
- Events can arrive as a single object or a list of objects.
- Important `event_type` values:
  - `book`: full L2 book snapshot. Use it to seed local bids/asks.
  - `price_change`: incremental book update. Often contains `price_changes`.
  - `best_bid_ask`: current top of book.
  - `last_trade_price`: latest execution price.
  - `tick_size_change`: tick-size update notification.

### Local Order-Book Cache

Recommended strategy-neutral cache shape:

```python
books[token_id] = {
    "bids": [(price, size), ...],  # sorted high to low
    "asks": [(price, size), ...],  # sorted low to high
    "received_at": monotonic_time,
}
```

Book parsing notes:

- Convert `price` and `size` to floats.
- Drop levels with non-positive size.
- Sort bids descending and asks ascending.
- On `book`, replace the full local book for that token.
- On `price_change`, update or delete the affected level:
  - old implementation treated `side == "BUY"` as bid-side update
  - old implementation treated `side == "SELL"` as ask-side update
  - if new size is zero, remove the level
  - otherwise insert/update and re-sort
- Treat `book + price_change` as the executable-depth source. Use
  `best_bid_ask` for top-of-book price hints and diagnostics only; it does not
  include reliable size and should not overwrite local depth.
- Track freshness with local monotonic timestamps.
- Any execution logic that uses book depth should reject stale or missing books,
  but freshness should match the action being taken. A one-sided late-window
  book is normal: if asks disappear but the held token still has fresh bids,
  exit logic should treat the bid side as usable instead of forcing a WS
  reconnect or stale-book risk exit.

### CLOB REST/SDK Trading Notes

- `MarketOrderArgs(..., order_type=OrderType.FAK)` with
  `client.post_order(signed, OrderType.FAK)` was used for taking liquidity.
- `price=<nonzero hint>` in `MarketOrderArgs` skips the SDK's internal
  `GET /book` call, so a fresh WS-derived hint can reduce latency.
- FAK means fill available liquidity immediately and cancel the remainder.
- Partial fills are possible and should be handled.
- BUY and SELL amount semantics differ in `py-clob-client-v2`:
  - BUY `amount` = dollars to spend, not shares.
  - SELL `amount` = shares to sell.
- `POST /order` may return only status/order identifiers and may omit fill
  details. When fill details are missing, use follow-up order/trade/balance
  queries for accounting.
- `POST /order` may time out even when the order later matches. Reconcile by
  token/USDC balance before retrying or recording no-fill.
- FAK no-match / no orders found should be treated as `order_no_fill`, not as a
  fatal bot error.
- Useful CLOB methods/endpoints:
  - `get_midpoint(token_id)`
  - `get_tick_size(token_id)`
  - `get_neg_risk(token_id)`
  - `get_balance_allowance(asset_type=CONDITIONAL, token_id=...)`
  - `GET /order/{orderID}`
  - `GET /trades`
- Balance response uses 6-decimal integer units. Convert shares with:
  `float(balance) / 1_000_000`.
- There is no generic "sell all" order. Query actual token balance, then sell a
  concrete share amount.
- Very small residual token balances can be below practical sell size; do not
  repeatedly submit dust SELL orders.
- Tick size can vary. Fetch `get_tick_size(token_id)` and round/clamp prices to
  valid ticks in `[0, 1]`.

### Operational Hygiene

- Use one persistent market WebSocket connection where possible; switch token
  subscriptions between markets instead of reconnecting unnecessarily.
- Clear cached prices/books when switching token sets.
- Use reconnect backoff for WebSocket failures.
- Record local receive times for every external data event.
- Keep raw API credentials and private keys out of logs.
- Log enough non-secret order diagnostics to debug latency and response shape.
- When inspecting VPS JSONL logs over SSH, avoid embedding multi-line Python in
  a single quoted `python -c` command. Use `ssh ... 'bash -s' <<'REMOTE'` with a
  heredoc, or upload/run a small temporary script, so shell quoting does not
  corrupt the diagnostic command.
