# Reusable Infrastructure Migrated From Old Poly Bot

The old project at `/Users/forrestliao/workspace/poly-bot` has several
strategy-neutral modules that are useful here. They were migrated into
`new_poly/` so future strategy scripts do not need to reimplement feed plumbing.

## Modules

```text
new_poly/market/binance.py
new_poly/market/series.py
new_poly/market/market.py
new_poly/market/stream.py
new_poly/market/deribit.py
new_poly/trading/fak_quotes.py
new_poly/config.py
new_poly/logging_utils.py
```

## What To Reuse

`new_poly.market.binance.BinancePriceFeed`

- Persistent Binance trade WebSocket.
- Rolling price history.
- `latest_price`.
- `price_at_or_before`.
- `first_price_at_or_after`.
- Binance 1-minute kline REST fallback through the standard library.

`new_poly.market.market`

- `MarketWindow`.
- `find_next_window`.
- `find_window_after`.
- Exact slug lookup through Gamma.
- Boundary-aware next-window discovery.

`new_poly.market.stream.PriceStream`

- Persistent Polymarket CLOB market WebSocket.
- `connect(token_ids)`.
- `switch_tokens(new_token_ids)`.
- Book snapshot and incremental price-change parsing.
- Best bid/ask and depth accessors.
- Cache clearing on token switch.
- Reconnect backoff.

`new_poly.market.deribit.fetch_dvol_snapshot`

- Fetches Deribit BTC volatility index data.
- Uses the latest close from `get_volatility_index_data`.
- Records both `dvol` point value and `sigma = dvol / 100`.
- Intended as a slow-moving implied-volatility reference, not a full 5-minute
  realized-volatility model.

`new_poly.trading.fak_quotes`

- Strategy-neutral depth quote helpers.
- BUY price hint buffering.
- SELL price hint buffering.
- Entry and exit depth summaries.

## What Not To Reuse

Do not import old strategy modules from `poly-bot`:

```text
polybot/strategies/*
polybot/trading/monitor.py strategy flow
old thresholds, entry windows, stop-loss rules, PnL claims
```

The new strategy should use migrated infrastructure only, then implement its own
probability model, position state, and execution rules in this repo.
