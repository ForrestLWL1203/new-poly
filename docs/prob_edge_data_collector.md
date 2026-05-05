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

## Data Sources

The collector records:

- Polymarket Gamma market slug/token metadata.
- Polymarket crypto price API for the UI Price to Beat:
  `k_price = openPrice`.
- Polymarket live-data WebSocket topic `crypto_prices_chainlink` as the primary
  live BTC/USD `S` source.
- Binance BTC/USDT trade WebSocket as the first backup proxy BTC price source.
- Coinbase BTC-USD match WebSocket as the second backup proxy BTC price source
  when enabled.
- Binance and, when enabled, Coinbase open prices inside the window boundary lookaround
  `(window_start - 5s, window_start + 5s)`, used to compute `basis_bps`.
- Polymarket CLOB WebSocket top/depth summaries for UP and DOWN tokens.
- Deribit BTC DVOL snapshot as 30-day implied-volatility reference.

For `k_price`, the collector starts checking shortly after a new window opens
and retries at approximately 5, 8, 12, 20, 30, and 40 seconds until the
Polymarket crypto price API exposes `openPrice`. The collector does not fetch
or log `closePrice`; simple post-run direction checks can compare Binance
window open/close prices instead.

The primary live price is now the same Polymarket live-data stream observed by
the event UI. In a three-window probe on 2026-05-06, the WS boundary ticks
matched the crypto price API `openPrice`/`closePrice` exactly. When that stream
is fresh, rows use:

```text
price_source = polymarket_chainlink
settlement_aligned = true
```

If the Polymarket live-data stream is missing or stale, the collector does not
immediately trade through the backup proxy. It remains fail-closed first. Only
after the source has been continuously unhealthy for
`--polymarket-backup-after-sec` seconds does it lazily start Binance/Coinbase
backup feeds and fall back to the previous basis-adjusted proxy. Rows emitted
before backup activation have `s_price=null` or `price_source=missing`, so
strategy replay will naturally skip them.

Coinbase can be disabled with `--no-coinbase`. It is a backup feed only in the
default setup; it is not started while Polymarket live-data is healthy. When
both Binance and Coinbase backup feeds are live, the proxy uses their arithmetic
mean for `proxy_price`. Once `k_price` and at least one matching open price are
known, it applies the open-basis adjustment using only sources that have both a
live price and a same-window open price:

```text
proxy_live = mean(valid paired live prices)
proxy_open = mean(valid paired open prices)
basis = proxy_open - k_price
s_price = proxy_live - basis
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
--max-polymarket-price-age-sec 3
--polymarket-backup-after-sec 180
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
