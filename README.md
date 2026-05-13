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

The strategy does not use Binance/Coinbase model probability, Black-Scholes
probability, `model_prob`, or `required_edge` for entries. It uses Polymarket's
BTC live-data reference price versus the window `k_price`:

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
python3 scripts/run_prob_edge_bot.py --once
```

Longer paper run:

```bash
python3 scripts/run_prob_edge_bot.py \
  --config configs/prob_poly_single_source.yaml \
  --mode paper \
  --windows 96 \
  --jsonl data/live_runs/paper-local-96w-$(date -u +%Y%m%dT%H%M%SZ).jsonl \
  --analysis-logs
```

Live mode requires both explicit live flags:

```bash
python3 scripts/run_prob_edge_bot.py \
  --config configs/prob_poly_single_source.yaml \
  --mode live \
  --i-understand-live-risk
```

Do not print, commit, or copy Polymarket account config/private key material.

## Data Collector

The collector is still useful for neutral market data capture:

```text
scripts/collect_prob_edge_data.py
```

It does not authenticate to CLOB, submit orders, or evaluate strategy
entry/exit decisions. The compatibility wrapper `scripts/prob_edge_dry_run.py`
delegates to the collector.

Example:

```bash
python3 scripts/collect_prob_edge_data.py \
  --interval-sec 1 \
  --jsonl data/prob-edge-collector.jsonl \
  --sigma-eff 0.6 \
  --sigma-source manual \
  --windows 12
```

Collector rows may include Binance/Coinbase proxy diagnostics and basis fields.
Those fields are diagnostic data, not active strategy entry logic.

## Backtest And Replay

The historical replay entrypoint remains:

```text
scripts/backtest_prob_edge.py
```

Despite the legacy filename, current replay code supports the single-source
strategy through `BacktestConfig(strategy_mode="poly_single_source")`.

Example:

```bash
python3 scripts/backtest_prob_edge.py \
  --jsonl data/live_runs/paper-sweden-96w-20260512T174319Z.jsonl \
  --poly-trend-lookback-sec 10 \
  --entry-start-age-sec 120 \
  --entry-end-age-sec 220
```

## Key Files

- `configs/prob_poly_single_source.yaml`: active bot config.
- `new_poly/strategy/poly_source.py`: active entry/exit logic.
- `new_poly/strategy/prob_edge.py`: shared DTOs only; old dual-source strategy
  logic has been removed.
- `new_poly/bot_runtime.py`, `new_poly/bot_runner.py`,
  `new_poly/bot_execution_flow.py`, `new_poly/bot_loop.py`: runtime loop and
  execution orchestration.
- `new_poly/trading/execution.py`: paper/live FAK execution gateways.
- `new_poly/market/prob_edge_data.py`: shared collector/runtime market data
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
  sshpass -e ssh root@70.34.207.45 'pgrep -af "run_prob_edge_bot|collect_prob_edge_data" || true'

SSHPASS="$(cat /Users/forrestliao/workspace/new-poly/docs/sweden-vps-secret.txt)" \
  sshpass -e ssh root@70.34.207.45 'ls -lt /opt/new-poly/logs | head -20'
```

## Archived Context

Older documents and reports may still mention the removed dual-source
probability-edge strategy, aggressive/dynamic profiles, `model_prob`,
`required_edge`, or Binance-as-model entries. Treat those as historical analysis
unless they have been updated to explicitly describe `poly_single_source`.
