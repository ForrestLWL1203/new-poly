# Probability Edge Strategy Bot

`scripts/run_prob_edge_bot.py` runs the first BTC 5-minute probability-edge
strategy robot.

It shares one strategy state machine across two execution modes:

- `paper`: default. Simulates FAK fills from the live local CLOB book.
- `live`: posts real FAK orders only when both `--mode live` and
  `--i-understand-live-risk` are provided.

The bot is separate from `scripts/collect_prob_edge_data.py`. The collector
records raw market data only; this script computes probabilities, decisions,
virtual/live fills, position state, and PnL.

## Run

Paper smoke test:

```bash
python3 scripts/run_prob_edge_bot.py --once
```

Paper run for 12 complete windows:

```bash
mkdir -p data
python3 scripts/run_prob_edge_bot.py \
  --mode paper \
  --windows 12 \
  --jsonl data/prob-edge-bot-paper.jsonl
```

Live mode requires the explicit second guard:

```bash
python3 scripts/run_prob_edge_bot.py \
  --mode live \
  --i-understand-live-risk \
  --windows 1
```

## Default Rules

- New entries only when window age is `40-270s`.
- No new entries in the final 30 seconds.
- Default notional is `$5`.
- Default required edge is `0.05`.
- Default max successful entries per market is `2`.
- `sigma_eff` uses Deribit BTC DVOL divided by 100.
- K is the Polymarket UI Price to Beat from the crypto price API.
- Settlement/reporting in paper mode uses Binance proxy direction; the bot does
  not wait for Polymarket `closePrice`.
- S is Binance proxy price, basis-adjusted once K and Binance open are known.

## FAK Price Logic

The bot keeps two separate prices for each candidate:

- `price`: size-aware average executable price. This is used for edge:
  `edge = model_prob - price`.
- `limit_price`: worst book level needed to fill the target notional. This is
  used as the FAK order cap/floor.

For BUY, the bot may accept a candidate because `ask_avg` is cheap enough, but
the live FAK price hint is based on `ask_limit`, not `ask_avg`. This lets the
order cross every visible ask level needed to fill the configured notional.

For SELL, the bot uses `bid_limit`, the lowest bid level needed to sell the
position size, rather than `bid_avg`.

## Logs

Each JSONL row is compact and non-secret. Rows include:

```text
event
mode
market_slug
age_sec
remaining_sec
s_price
k_price
sigma_eff
up/down book summaries
decision
order
position
realized_pnl
```

The bot does not log private keys, API secrets, signed order payloads, or full
order books.
