# New Poly

Fresh Polymarket trading/arbitrage bot workspace.

This repo intentionally starts strategy design from zero. The old
`/Users/forrestliao/workspace/poly-bot` project was used only for
infrastructure knowledge: Binance BTC feeds, Polymarket Gamma/CLOB APIs, CLOB
WebSocket order-book handling, authentication, and FAK execution mechanics.

## Active Strategy

The active bot strategy is the Polymarket live-data single-source strategy.
Runtime decisions live in:

```text
new_poly/strategy/poly_source.py
```

The only active runtime config is:

```text
configs/prob_poly_single_source.yaml
```

The strategy uses Polymarket's BTC live-data reference price versus the window `k_price`:

- `polymarket_price > k_price` means UP-side reference confirmation.
- `polymarket_price < k_price` means DOWN-side reference confirmation.
- Entry score combines reference distance, rolling reference return, price
  quality, and market quality.
- Current live-oriented profile enters between `120s` and `220s` window age and
  uses a `10s` Polymarket reference return lookback.
- Very low asks are discounted in price quality instead of being banned.
- Exits use held-side reference distance, reference trend, executable bid, and
  late-window hold-to-settlement rules.

Binance/Coinbase modules remain in the repo for data collection, diagnostics,
and reusable market infrastructure. They are not the active strategy model.

## Run

Paper smoke test:

```bash
python3 scripts/run_poly_source_bot.py --once
```

Longer paper run:

```bash
python3 scripts/run_poly_source_bot.py \
  --config configs/prob_poly_single_source.yaml \
  --mode paper \
  --windows 96 \
  --jsonl data/live_runs/paper-local-96w-$(date -u +%Y%m%dT%H%M%SZ).jsonl \
  --analysis-logs
```

Live mode requires both explicit live flags:

```bash
python3 scripts/run_poly_source_bot.py \
  --config configs/prob_poly_single_source.yaml \
  --mode live \
  --i-understand-live-risk
```

Do not print, commit, or copy Polymarket account config/private key material.

## Data Collector

The collector is still useful for neutral market data capture:

```text
scripts/collect_poly_source_data.py
```

It does not authenticate to CLOB, submit orders, or evaluate strategy
entry/exit decisions. Example:

```bash
python3 scripts/collect_poly_source_data.py \
  --interval-sec 1 \
  --jsonl data/poly-source-collector.jsonl \
  --sigma-eff 0.6 \
  --sigma-source manual \
  --windows 12
```

Collector rows may include Binance/Coinbase proxy diagnostics and basis fields.
Those fields are diagnostic data, not active strategy entry logic.

## Backtest And Replay

The replay entrypoint is:

```text
scripts/backtest_poly_source.py
```

Example:

```bash
python3 scripts/backtest_poly_source.py \
  --jsonl data/live_runs/paper-sweden-96w-20260512T174319Z.jsonl \
  --poly-trend-lookback-sec 10 \
  --entry-start-age-sec 120 \
  --entry-end-age-sec 220
```

## Key Files

- `configs/prob_poly_single_source.yaml`: active bot config.
- `new_poly/strategy/poly_source.py`: active entry/exit logic.
- `new_poly/strategy/types.py`: shared strategy DTOs.
- `new_poly/bot_runtime.py`, `new_poly/bot_runner.py`,
  `new_poly/bot_execution_flow.py`, `new_poly/bot_loop.py`: runtime loop and
  execution orchestration.
- `new_poly/trading/execution.py`: paper/live FAK execution gateways.
- `new_poly/market/poly_source_data.py`: shared collector/runtime market data
  helpers.

## VPS Notes

Sweden is the default VPS when a task says "VPS":

```text
host: 70.34.207.45
user: root
repo: /opt/new-poly/repo
venv: /opt/new-poly/venv
logs: /opt/new-poly/logs
config: /opt/new-poly/shared/polymarket_config.json
```

Use the ignored local password file with `SSHPASS`; never echo the password or
print `/opt/new-poly/shared/polymarket_config.json`.

Useful read-only checks:

```bash
SSHPASS="$(cat /Users/forrestliao/workspace/new-poly/docs/sweden-vps-secret.txt)" \
  sshpass -e ssh root@70.34.207.45 'pgrep -af "run_poly_source_bot|collect_poly_source_data" || true'

SSHPASS="$(cat /Users/forrestliao/workspace/new-poly/docs/sweden-vps-secret.txt)" \
  sshpass -e ssh root@70.34.207.45 'ls -lt /opt/new-poly/logs | head -20'
```

