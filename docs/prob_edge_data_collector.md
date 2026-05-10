# Probability Edge Data Collector

`scripts/collect_prob_edge_data.py` continuously records live BTC 5-minute
market data for later strategy dry-run and backtest work.

It is a data collector only:

- no CLOB authentication,
- no private key or API credential loading,
- no signed orders,
- no `POST /order`,
- no entry/exit decision,
- no candidate signal,
- no PnL simulation.

The compatibility wrapper `scripts/prob_edge_dry_run.py` currently delegates to
this collector so old commands do not break, but new commands should use the
collector name.

Strategy-neutral helper code used by both the collector and strategy bot lives
in `new_poly/market/prob_edge_data.py`. Keep this script as an entrypoint and
JSONL row builder; do not add bot-only strategy decisions here.

## Data Sources

The collector records:

- Polymarket Gamma market slug/token metadata.
- Polymarket crypto price API for the UI Price to Beat:
  `k_price = openPrice`.
- Binance BTC/USDT trade WebSocket as the primary model BTC price source.
- Polymarket live-data WebSocket topic `crypto_prices_chainlink` as the
  settlement-source reference price, used for source-divergence diagnostics and
  not as the normal model `S`.
- Coinbase BTC-USD match WebSocket only when explicitly enabled for backup or
  multi-source diagnostics. It is disabled by default.
- Binance and, when enabled, Coinbase open prices inside the window boundary
  lookaround `(window_start - 5s, window_start + 5s)`, used only to compute
  basis diagnostics.
- Polymarket CLOB WebSocket top/depth summaries for UP and DOWN tokens.
- Deribit BTC DVOL snapshot as 30-day implied-volatility reference.

For `k_price`, the collector starts checking shortly after a new window opens
and retries at approximately 5, 8, 12, 20, 30, and 40 seconds until the
Polymarket crypto price API exposes `openPrice`. The collector does not fetch
or log `closePrice`; simple post-run direction checks can compare Binance
window open/close prices instead.

The model live price is now raw Binance by default. Polymarket live-data remains
important, but only as a reference stream observed by the event UI. In a
three-window probe on 2026-05-06, the WS boundary ticks matched the crypto price
API `openPrice`/`closePrice` exactly. Rows usually use:

```text
price_source = proxy_binance
s_price = binance_live
```

`binance_open_price`, `proxy_open_price`, and `basis_bps` are still logged as
diagnostics, but the basis is not applied to the model `S`. This preserves the
strategy's current goal: use Binance as the leading signal for Polymarket CLOB
repricing.

If the Polymarket live-data stream is missing or stale, the collector keeps
using Binance for the model price but marks the reference data as absent or old.
After `--polymarket-stale-reconnect-sec` seconds without a valid price tick, the
Polymarket WS feed closes and reconnects. Current default is `5s`. The
Polymarket reference cache is intentionally short, about 15 seconds.

Coinbase is disabled by default. Enable it with `--coinbase` only for runs that
need multi-source diagnostics. When both Binance and Coinbase are live, the
proxy uses their arithmetic mean for `proxy_price` / `s_price`. Once `k_price`
and at least one matching open price are known, the open-basis diagnostic is
calculated using only sources that have both a live price and a same-window open
price:

```text
proxy_live = mean(valid paired live prices)
proxy_open = mean(valid paired open prices)
basis = proxy_open - k_price
s_price = mean(live proxy sources)
```

If Coinbase is disabled or unavailable, the collector uses the existing Binance
single-source proxy. `source_spread_usd` and `source_spread_bps` show the live
Binance/Coinbase disagreement only when Coinbase is enabled and both sources are
available.

## Usage

Run one row:

```bash
python3 scripts/collect_prob_edge_data.py --once --sigma-eff 0.6 --sigma-source manual
```

Run continuously and write JSONL:

```bash
mkdir -p data
python3 scripts/collect_prob_edge_data.py \
  --interval-sec 1 \
  --jsonl data/prob-edge-collector.jsonl \
  --sigma-eff 0.6 \
  --sigma-source manual \
  --windows 12
```

Useful options:

```text
--once
--interval-sec 1
--jsonl <path>
--depth-notional 5
--depth-safety-multiplier 1.5
--collect-dvol / --no-collect-dvol
--dvol-refresh-sec 0
--warmup-timeout-sec 8
--include-current-window
--windows 12
--polymarket-price / --no-polymarket-price
--polymarket-stale-reconnect-sec 5
--polymarket-unhealthy-log-after-sec 10
--coinbase / --no-coinbase
--verbose
```

By default, the collector fetches Deribit BTC DVOL once at startup and writes
that snapshot into every row. `--dvol-refresh-sec N` can refresh it every `N`
seconds, but for 30-day implied volatility the default one-shot snapshot is
usually enough for a collection run.
Rows include `volatility_stale`; if the DVOL snapshot exceeds
`--max-dvol-age-sec` (default `900`), the collector marks it stale and writes
`sigma_eff=null` so replay can match live fail-closed behavior.

`--windows N` stops after observing `N` windows that successfully obtained
`k_price`. Windows skipped before `k_price` is available do not count.

By default, startup ignores the already-running five-minute window and begins
from the next full window. Use `--include-current-window` only for ad-hoc
debugging when a partial first window is acceptable.

## JSONL Schema

Each tick emits one compact JSON object. Important fields:

```text
ts
market_slug
window_start
window_end
age_sec
remaining_sec
window_bucket
resolution_source
settlement_aligned
sigma_source
sigma_eff
volatility_stale
volatility
price_source
s_price
k_price
k_source
binance_price
coinbase_price
polymarket_price
polymarket_price_age_sec
proxy_price
polymarket_open_price
polymarket_open_source
polymarket_open_delta_ms
binance_open_price
binance_open_source
binance_open_delta_ms
coinbase_open_price
coinbase_open_source
coinbase_open_delta_ms
proxy_open_price
basis_bps
source_spread_usd
source_spread_bps
lead_binance_vs_polymarket_usd / bps
polymarket_divergence_bps
lead_coinbase_vs_polymarket_usd / bps
lead_proxy_vs_polymarket_usd / bps
lead_binance_return_1s_bps / 3s / 5s
lead_coinbase_return_1s_bps / 3s / 5s
lead_polymarket_return_1s_bps / 3s / 5s
lead_binance_side / lead_coinbase_side / lead_proxy_side / lead_polymarket_side
lead_*_side_disagrees_with_polymarket
depth_notional
up
down
yes_no_sum
warnings
```

`volatility` currently has this shape:

```text
source = deribit_dvol
currency = BTC
dvol = Deribit DVOL point value, e.g. 39.47
sigma = dvol / 100, e.g. 0.3947
timestamp_ms
age_sec
```

Token summaries:

```text
bid
ask
book_age_ms
ask_avg
bid_avg
ask_limit
ask_safety_limit
bid_limit
stable_depth_usd
ask_depth_ok
bid_depth_ok
```

`ask_avg` and `ask_limit` are computed for `--depth-notional`. `ask_safety_limit`
is computed for `--depth-notional * --depth-safety-multiplier`, and
`ask_depth_ok` requires both the normal and safety depth checks to pass. This
lets replay enforce the same safety-depth guard used by the strategy bot without
logging full order books.

The collector intentionally does not log full order books. Add a separate book
recorder if full offline fill simulation becomes necessary.

## What Backtests Should Recompute

Backtests and strategy dry-run scripts should compute these from collected data:

- Black-Scholes `d2` probability.
- Required edge.
- Entry/exit decisions.
- Virtual position state.
- Cooldown and max entries per window.
- Logic-decay exits.
- PnL.

Keeping those fields out of the collector prevents strategy assumptions from
polluting the dataset.
