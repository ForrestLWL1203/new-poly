# BTC 5m Probability Edge Strategy Design

Date: 2026-05-03

This is the current clean-room design for a new BTC five-minute Polymarket
probability arbitrage strategy. It intentionally excludes old strategy logic,
old timing windows, old thresholds, old stop-loss rules, and old backtest
claims. It also excludes the late-window sweep idea for now.

## Objective

Trade only when the bot estimates that a BTC five-minute UP/DOWN token is
priced materially below its fair probability of settlement. The strategy should
avoid directional trend guessing and instead compare:

```text
fair probability from model
minus executable Polymarket price
minus uncertainty and execution penalties
```

The first version should prove whether a probability model can identify
repeatable mispricing before adding more aggressive latency-sensitive modes.

## Market Model

Treat each BTC five-minute UP/DOWN market as a binary option.

Inputs:

```text
S     = current BTC price from Binance
K     = market settlement threshold / window open price
sigma = annualized volatility estimate
T     = remaining seconds / 31,536,000
```

Important naming rule: `K` is not the Polymarket token price. `K` is the BTC
price threshold that determines settlement.

Simplified Black-Scholes probability:

```text
d2 = (ln(S / K) - 0.5 * sigma^2 * T) / (sigma * sqrt(T))
P_up = N(d2)
P_down = 1 - P_up
```

`N` is the standard normal cumulative distribution function.

The model output is a fair settlement probability, not an automatic order
signal. It must be adjusted by execution price, freshness, spread, volatility
uncertainty, and risk limits.

## Volatility Estimate

Version 1 may use Deribit BTC DVOL as the primary `sigma` source because it is
simple, forward-looking, and based on liquid BTC options markets.

However, DVOL should be treated as a volatility prior, not as an infallible
five-minute realized move forecast. The strategy should expose a `sigma_eff`
interface from the start so later versions can blend:

```text
DVOL implied volatility
recent fast realized volatility from Binance
slower realized volatility floor
orderflow or market-stress penalties
```

Initial conservative rule:

```text
sigma_eff = DVOL / 100
```

Future conservative rule:

```text
sigma_eff = max(DVOL_adjusted, RV_fast, RV_slow_floor)
```

Underestimating volatility is more dangerous than overestimating it because it
makes the model overconfident and pushes probability away from 0.5.

## Data Feeds

### Binance BTC Price

Use Binance `btcusdt@trade` as the primary source for `S`.

Optional auxiliary feeds:

```text
btcusdt@bookTicker
btcusdt@depth5@100ms
```

These auxiliary feeds are for freshness, spread, and market-stress checks. They
should not become a disguised trend signal in the MVP.

The BTC feed should keep local receive timestamps and a rolling price history.

### Deribit Volatility

Poll Deribit DVOL about once per minute or use the Deribit volatility index
subscription if available and stable. Record the receive time and reject stale
volatility values.

### Polymarket Market Discovery

Use Gamma to discover the current BTC five-minute market slug:

```text
btc-updown-5m-<window_start_epoch>
```

The UP token is token index 0 and the DOWN token is token index 1 for the
observed BTC UP/DOWN markets.

### Polymarket Order Book

Use the CLOB market WebSocket to maintain fresh local L2 books for both tokens.
Each book must track:

```text
bids
asks
received_at monotonic timestamp
```

Execution logic must reject missing or stale books.

## Executable Price

Do not use only best ask as the trading price. Compute the expected average fill
price for a configured order size from the local ask book:

```text
ask_avg_price(size)
```

For exits, compute the expected average bid price for the held share amount:

```text
bid_avg_price(shares)
```

The strategy edge is size-aware:

```text
edge(size) = fair_prob - ask_avg_price(size)
```

If there is insufficient visible depth for the desired size, either reduce size
or skip the trade.

## Entry Logic

The MVP has one entry mode: normal probability edge entry.

No new entries when:

```text
remaining_time <= 30 seconds
```

For each token:

```text
fair_prob = P_up for UP token, P_down for DOWN token
entry_price = ask_avg_price(target_size)
edge = fair_prob - entry_price
```

Enter only if:

```text
edge > required_edge
remaining_time > 30 seconds
all required feeds are fresh
visible depth is enough for the target size
token and market are active
risk limits allow a new position
```

For the first implementation, `required_edge` can start at 0.05 as a configured
base edge. It should be structured so it can later become:

```text
required_edge =
  base_edge
  + spread_penalty
  + latency_penalty
  + volatility_uncertainty_penalty
  + stale_data_penalty
  + time_to_expiry_penalty
```

The strategy should log the edge components for every accepted and rejected
candidate.

## Execution

Use CLOB FAK orders only for entries and exits.

FAK behavior:

```text
fills available liquidity immediately
cancels the rest
partial fills are possible
```

Therefore the bot must account for:

```text
actual filled shares
actual average entry price
remaining unfilled order amount
order response fields that omit fill details
follow-up order, trade, or balance queries when needed
```

Do not rest passive orders on the book in the MVP.

Do not use FOK as the default execution mode. FOK avoids partial fills, but it
would lower the fill rate for this strategy and is not needed for the MVP
because the strategy is buying one underpriced token, not trying to complete a
strictly paired multi-leg trade.

### Execution Path Optimization

The strategy should optimize the hot execution path without weakening the
pricing checks.

Priority 0 optimizations:

```text
use an async architecture for feeds, strategy evaluation, and order execution
reuse a persistent httpx.AsyncClient or SDK client session where possible
keep CLOB authentication initialized and API credentials set before trading
cache current market token ids
cache tick size and static token metadata
keep local CLOB books continuously warm from WebSocket updates
reject stale Binance price, stale Polymarket book, or stale volatility
```

The goal is to pre-warm the execution path, not to pre-sign complete orders.

Full order pre-signing is intentionally excluded. Complete signed orders are a
poor fit because the live order fields depend on the latest model and book
state:

```text
token_id
side
amount or share size
price cap
current tick size
remaining time
current book depth
model probability
required edge
```

Pre-signing real FAK orders can turn a once-valid trade into a stale order
intent. The bot should construct and sign the order only after it has read the
latest local state and confirmed the edge still exists.

Safe pre-warm work:

```text
initialize ClobClient
derive or create API credentials
set API credentials
cache market/token metadata
cache tick sizes
prepare reusable client objects
maintain latest books and price state
```

Live order work:

```text
read latest fresh state
compute fair probability
compute size-aware executable price
compute price cap
create signed marketable FAK order
post FAK order
reconcile partial fill or no fill
```

HTTP/2 may be enabled as a measured configuration option:

```text
httpx.AsyncClient(http2=True)
```

The bot should log response HTTP version and order POST latency before treating
HTTP/2 as a real improvement. It should not assume HTTP/2 is faster for every
CLOB endpoint.

Fast JSON parsing can be added after profiling. `orjson` is preferred over
`ujson` if a faster parser is needed, but JSON parsing should not replace the
higher-priority work of keeping the execution path asynchronous and nonblocking.

Do not replace size-aware depth checks with only `best_price + ticks`. A small
top-of-book quote can create fake edge for larger target sizes. A safe
compromise is:

```text
scan only the book levels needed to fill the target size
compute average executable price for that size
use a tick-limited price cap for FAK submission
skip if required depth is not visible
```

## Position State

Track each position by token:

```text
market_slug
token_id
side label: UP or DOWN
entry_time
entry_avg_price
filled_shares
entry_model_prob
entry_edge
last_model_prob
last_executable_bid
exit_status
```

Before selling, query or reconcile actual token balance. There is no generic
"sell all" CLOB order; the bot must sell a concrete share amount.

## Exit Logic

Recompute model probability about once per second for every open position.

For a held token:

```text
model_prob_now = current fair probability for that token
exit_bid = bid_avg_price(held_shares)
```

Exit triggers:

```text
logic_decay_exit:
  model_prob_now < entry_avg_price - model_decay_buffer

market_overprice_exit:
  exit_bid > model_prob_now + overprice_buffer

risk_exit:
  feed freshness fails, market state becomes unsafe, or configured risk limits
  require reducing exposure
```

The original fixed percentage stop-loss idea is intentionally not used. Exits
are based on whether the probability edge has vanished or inverted.

When deciding whether to sell immediately, compare:

```text
known value from selling at executable bid
versus model expected value from holding
```

This prevents the bot from mechanically selling into a terrible bid when the
spread is wider than the model deterioration.

## Explicitly Deferred

The late-window sweep mode is excluded from the MVP:

```text
no special entries during the final 10-20 seconds
no latency race mode
no "stale human order" sweep logic
```

The MVP treats `remaining_time <= 30 seconds` as a no-new-entry zone. Existing
positions may still be managed or exited.

## Risk Controls

The first implementation should include these controls before any live trading:

```text
max dollars per order
max shares per token
max open positions
max exposure per market
min visible depth for target size
max Binance price age
max Polymarket book age
max DVOL age
minimum remaining time for entry
dry-run mode
intentional live mode flag
structured non-secret logs
```

Risk checks should fail closed. If the bot is unsure, it should skip.

## Testing And Validation

Before live trading, validate:

```text
Black-Scholes probability math over known scenarios
UP and DOWN probabilities sum to approximately 1
sigma and T unit conversions
size-aware average fill price calculations
book freshness rejection
FAK partial-fill accounting
exit trigger behavior
no-new-entry behavior inside final 30 seconds
secret redaction in logs
```

Backtesting and paper simulation should replay point-in-time BTC price,
volatility estimate, and Polymarket book snapshots where available. If historical
order-book snapshots are unavailable, paper trading on live data should be used
before any real order posting.

## Current Recommended MVP

Build the simplest safe version:

```text
1. Discover current BTC 5m market.
2. Maintain Binance trade price.
3. Fetch DVOL and compute sigma_eff.
4. Maintain Polymarket UP/DOWN order books.
5. Compute P_up and P_down every tick or every short interval.
6. Evaluate size-aware edge for UP and DOWN.
7. In dry-run mode, log candidate decisions.
8. Only after paper validation, allow explicit live FAK entry.
9. Manage positions with probability-based exit logic.
```

The design goal is not to predict direction. The goal is to buy underpriced
settlement probability while avoiding stale data, thin books, and model
overconfidence.
