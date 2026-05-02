# Probability Edge Dry-Run Logger

`scripts/prob_edge_dry_run.py` is the first live observation tool for the new
BTC five-minute probability-edge strategy.

It is intentionally dry-run only:

- no CLOB authentication,
- no private key or API credential loading,
- no signed orders,
- no `POST /order`.

The goal is to collect compact JSONL rows for strategy analysis, calibration,
and later replay.

## Current Price Model

Current BTC 5-minute markets resolve from Chainlink BTC/USD Data Streams:

```text
resolutionSource = https://data.chain.link/streams/btc-usd
```

The Polymarket UI shows a target price / Price to Beat. The dry-run script reads
the same value from the event-page HTML hydration data:

```text
["crypto-prices", "price", "BTC", "<window_start>", "fiveminute", "<window_end>"]
```

Fields:

```text
k_price = openPrice from Polymarket HTML
k_source = polymarket_html_crypto_prices
close_price = closePrice from Polymarket HTML, if present
```

Because direct realtime Chainlink Data Streams access is not integrated, current
`S` is a Binance proxy adjusted by the window-start basis:

```text
basis = binance_open_at_window_start - k_price
s_price = latest_binance_price - basis
price_source = proxy_binance_basis_adjusted
```

This is useful for dry-run and analysis, but it is not a fully settlement-aligned
live signal. The script therefore keeps:

```text
settlement_aligned = false
live_ready = false
```

## Local Usage

Run one row:

```bash
python3 scripts/prob_edge_dry_run.py --once --sigma-eff 0.6 --sigma-source manual
```

Run continuously and write JSONL:

```bash
mkdir -p data
python3 scripts/prob_edge_dry_run.py \
  --interval-sec 1 \
  --jsonl data/prob-edge-dry-run.jsonl \
  --sigma-eff 0.6 \
  --sigma-source manual
```

Useful options:

```text
--once
--interval-sec 1
--jsonl <path>
--order-notional 5
--base-edge 0.07
--sigma-eff 0.6
--sigma-source manual
--warmup-timeout-sec 8
--windows 12
--verbose
```

By default the logger runs forever. Use `--windows N` to stop after observing
`N` distinct five-minute market slugs. For example, `--windows 12` records about
one hour of BTC 5m windows.

## VPS Usage

Copy the script:

```bash
scp -i /Users/forrestliao/workspace/new-poly/docs/LightsailDefaultKey-eu-west-1.pem \
  /Users/forrestliao/workspace/new-poly/scripts/prob_edge_dry_run.py \
  ubuntu@176.34.134.21:/opt/new-poly/app/prob_edge_dry_run.py
```

Run on the VPS:

```bash
ssh -i /Users/forrestliao/workspace/new-poly/docs/LightsailDefaultKey-eu-west-1.pem \
  ubuntu@176.34.134.21 \
  'mkdir -p /opt/new-poly/data && /opt/new-poly/venv/bin/python /opt/new-poly/app/prob_edge_dry_run.py \
    --interval-sec 1 \
    --jsonl /opt/new-poly/data/prob-edge-dry-run.jsonl \
    --sigma-eff 0.6 \
    --sigma-source manual \
    --windows 12'
```

## JSONL Schema

Each evaluation tick emits one compact JSON object.

Core fields:

```text
ts
market_slug
window_start
window_end
age_sec
remaining_sec
phase
resolution_source
settlement_aligned
live_ready
sigma_source
sigma_eff
price_source
s_price
k_price
k_source
close_price
basis_bps
up_prob
down_prob
required_edge
edge_components
order_notional
up
down
yes_no_sum
decision
candidate_side
skip_reason
warnings
```

Token summaries are intentionally compact:

```text
bid
ask
book_age_ms
ask_avg
bid_avg
stable_depth_usd
edge
```

The script does not log full order books. If full replay is needed later, build
a separate CLOB book recorder.

## Skip Reasons

Stable primary `skip_reason` values include:

```text
warmup
final_no_entry
closed_market
bad_resolution_source
missing_chainlink_price
paper_proxy_only
missing_k
stale_price
stale_book
missing_book
insufficient_depth
edge_too_small
vol_stress
basis_too_wide
one_sided_book
no_trade_blackout
```

Current dry-run usually skips with `paper_proxy_only` when it has
Polymarket-derived `K` and Binance basis-adjusted `S`, because that is still not
direct Chainlink realtime data.

## Analysis Targets

Use collected JSONL to inspect:

- `basis_bps` stability by window,
- `up_prob` / `down_prob` behavior around `K`,
- edge frequency and timing,
- skip reason distribution,
- book freshness and stable depth,
- YES+NO paired opportunity frequency,
- whether basis-adjusted Binance proxy is close enough for paper modeling.

Do not use this logger as a live trading signal until settlement-aligned
realtime `S` is solved and offline replay supports the calibration.
