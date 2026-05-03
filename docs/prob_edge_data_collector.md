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
- Polymarket event-page hydration data for the UI Price to Beat:
  `k_price = openPrice`.
- Binance BTC/USDT trade WebSocket as the current proxy BTC price.
- Binance open price around the window boundary, used to compute
  `basis_bps`.
- Polymarket CLOB WebSocket top/depth summaries for UP and DOWN tokens.
- Deribit BTC DVOL snapshot as 30-day implied-volatility reference.

For `k_price`, the collector starts checking shortly after a new window opens
and retries at approximately 5, 8, 12, 20, 30, and 40 seconds until the
Polymarket HTML hydration data exposes `openPrice`.

Direct realtime Chainlink Data Streams access is not integrated yet, so:

```text
settlement_aligned = false
price_source = proxy_binance or proxy_binance_basis_adjusted
```

Strategy code and backtests should treat this as proxy data unless a true
settlement-aligned source is added.

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
--collect-dvol / --no-collect-dvol
--dvol-refresh-sec 0
--warmup-timeout-sec 8
--include-current-window
--windows 12
--verbose
```

By default, the collector fetches Deribit BTC DVOL once at startup and writes
that snapshot into every row. `--dvol-refresh-sec N` can refresh it every `N`
seconds, but for 30-day implied volatility the default one-shot snapshot is
usually enough for a collection run.

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
volatility
price_source
s_price
k_price
k_source
close_price
binance_open_price
binance_open_source
binance_open_delta_ms
basis_bps
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
stable_depth_usd
ask_depth_ok
bid_depth_ok
```

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
