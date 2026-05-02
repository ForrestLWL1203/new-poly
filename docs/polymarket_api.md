# Polymarket And Binance API Notes

Distilled from the old project on 2026-05-02. This document contains
infrastructure knowledge only, not old strategy logic.

## API Systems

| API | Base URL | Purpose |
|---|---|---|
| CLOB | `https://clob.polymarket.com` | Trading, order book, balances, tick sizes |
| Gamma | `https://gamma-api.polymarket.com` | Market/event metadata and slug discovery |
| Data | `https://data-api.polymarket.com` | Positions, activity, history |

## Binance BTC Trade WebSocket

Use Binance trades as a raw BTC price source:

```text
wss://stream.binance.com:9443/ws/btcusdt@trade
```

Important fields:

- `p`: trade price
- The old project timestamped received trades locally with `time.time()`.

Recommended neutral feed behavior:

- Keep a rolling deque of `(timestamp, price)`.
- Expose `latest_price`.
- Expose `price_at_or_before(ts)`.
- Expose `first_price_at_or_after(ts, max_forward_sec=...)`.
- Prune old history by age.
- Reconnect after close/errors with a short sleep/backoff.

REST fallback for a missing open price:

```text
GET https://api.binance.com/api/v3/klines
```

Params:

```text
symbol=BTCUSDT
interval=1m
startTime=<epoch_ms>
limit=1
```

The open price is response item `[0][1]`.

## CLOB Authentication

Runtime dependency from the old project:

```text
py-clob-client-v2==1.0.0
```

Key facts:

- CLOB host: `https://clob.polymarket.com`
- Chain ID: `137`
- Signature type `0`: EOA
- Signature type `1`: proxy/Magic wallet
- Signature type `2`: Gnosis Safe
- Existing Polymarket CLI config may live at
  `~/.config/polymarket/config.json`.

Old client creation flow:

1. Load private key and wallet metadata from CLI config or env.
2. Determine `signature_type`.
3. For proxy wallets, use the proxy/funder address if available.
4. Create `ClobClient(host, key, chain_id, signature_type, funder)`.
5. Derive API key with `derive_api_key()`.
6. If derive returns none, call `create_api_key()`.
7. Call `client.set_api_creds(creds)`.

Useful environment variable names:

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

## Gamma Market Discovery

Endpoint:

```text
GET https://gamma-api.polymarket.com/markets?slug=<slug>
```

For BTC 5-minute UP/DOWN markets:

- Slug prefix: `btc-updown-5m`
- Slug step: `300` seconds
- Slug format: `btc-updown-5m-<window_start_epoch>`

Fields used previously:

- `slug`
- `question`
- `active`
- `closed`
- `endDate`
- `eventStartTime`
- `clobTokenIds`

`clobTokenIds` can be a JSON string or a Python/list JSON array. For observed
UP/DOWN markets:

- `clobTokenIds[0]`: Up/Yes token
- `clobTokenIds[1]`: Down/No token

## CLOB WebSocket Market Feed

URL:

```text
wss://ws-subscriptions-clob.polymarket.com/ws/market
```

Subscribe:

```json
{
  "type": "market",
  "assets_ids": ["<token_id_1>", "<token_id_2>"],
  "operation": "subscribe",
  "custom_feature_enabled": true
}
```

Unsubscribe:

```json
{
  "assets_ids": ["<token_id_1>", "<token_id_2>"],
  "operation": "unsubscribe"
}
```

Heartbeat:

- Send `{}` every 10 seconds.
- Messages may arrive as a single object or a list.

Events:

| Event | Use |
|---|---|
| `book` | Full L2 book snapshot; use to seed/replace local depth |
| `price_change` | Incremental depth update; often has `price_changes` array |
| `best_bid_ask` | Best bid/ask update; midpoint = `(bid + ask) / 2` |
| `last_trade_price` | Last executed trade price |
| `tick_size_change` | Tick-size update notification |

## Local Book Cache

Recommended structure:

```python
books[token_id] = {
    "bids": [(price, size), ...],
    "asks": [(price, size), ...],
    "received_at": time.monotonic(),
}
```

Rules:

- Parse `price` and `size` as floats.
- Drop malformed or non-positive size levels.
- Sort bids descending by price.
- Sort asks ascending by price.
- Replace the book on `book` events.
- For `price_change`, iterate `price_changes` when present.
- In observed CLOB WS messages:
  - `side == "BUY"` updates bids.
  - `side == "SELL"` updates asks.
- If update size is zero, remove the level.
- If update size is positive, insert/update the level and re-sort.
- Track local receive time with `time.monotonic()`.
- Consumers should require fresh books before using depth for execution.

## CLOB Orders

Useful SDK imports:

```python
from py_clob_client_v2 import MarketOrderArgs, OrderType
from py_clob_client_v2.order_builder.constants import BUY, SELL
```

FAK order shape used previously:

```python
args = MarketOrderArgs(
    token_id=token_id,
    amount=amount,
    side=BUY,  # or SELL
    order_type=OrderType.FAK,
    price=price_hint or 0,
)
signed = client.create_market_order(args, options=options)
resp = client.post_order(signed, OrderType.FAK)
```

Important behavior:

- FAK fills available liquidity immediately and cancels the remainder.
- Partial fills can happen.
- A non-zero `price` hint can avoid an internal SDK `GET /book`.
- BUY `amount` means dollars to spend.
- SELL `amount` means shares to sell.
- `POST /order` responses may omit `sizeFilled` and `avgPrice`.
- Response status may include `matched`, `live`, `delayed`, or `unmatched`.
- Fill accounting may require follow-up calls:
  - `GET /order/{orderID}` for `size_matched`
  - `GET /trades`
  - `GET /balance-allowance`

## Balance And Allowance

Endpoint/method:

```text
GET /balance-allowance
```

Params:

```text
asset_type=CONDITIONAL
token_id=<token_id>
```

Response balance is a 6-decimal integer string. Convert to shares:

```python
shares = float(resp["balance"]) / 1_000_000
```

There is no universal "sell all" API. Query the actual token balance and sell a
concrete share amount.

## Tick Sizes

- Use `client.get_tick_size(token_id)`.
- Tick size may vary by token/market/price band.
- Cache tick size per token for a session.
- Round and clamp prices to valid ticks in `[0, 1]`.
- Useful fallback tick from old code: `0.001`.

## Polymarket RTDS Crypto Feed

The old project also had a fallback/reference Polymarket RTDS crypto price feed:

```text
wss://ws-live-data.polymarket.com
```

Subscribe:

```json
{
  "action": "subscribe",
  "subscriptions": [
    {
      "topic": "crypto_prices",
      "type": "update"
    }
  ]
}
```

Notes:

- Client sent `"PING"` every 5 seconds.
- Server `"PING"` should be answered with `"PONG"`.
- BTC symbol used was `btcusdt`.
- Payload can contain a single object or batched `payload.data`.
- Ignore malformed, non-finite, or wrong-symbol values.

## Geoblock Notes

Observed notes from prior documentation:

- Taiwan: close-only.
- Hong Kong: allowed.
- US: blocked.

Treat geoblock behavior as operationally important and verify before live use.
