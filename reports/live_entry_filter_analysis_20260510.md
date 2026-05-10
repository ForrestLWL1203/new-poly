# Live Entry Filter Analysis - 2026-05-10

Scope: all local live JSONL logs under `data/live_runs`, `data/live_logs`, and `data/remote/prob-edge-live*`.

Config baseline: current `configs/prob_edge_aggressive.yaml` replayed with raw Binance as model `S`.

## Baseline

- Windows: 195
- Entries: 57
- Closed trades: 56
- Win rate: 55.36%
- PnL: +16.3280
- Max drawdown: -5.4121
- Exit reasons: settlement 25, market_disagrees_exit 8, final_force_exit 8, market_overprice_exit 6, logic_decay_exit 5, polymarket_divergence_exit 4

## Bad Exit Entry Commonality

### market_disagrees_exit

- Count: 8
- PnL: -5.0084
- Entry side: mostly UP, one DOWN
- Entry price range: 0.33-0.67
- Entry model_prob range: 0.568-0.857
- Entry S-K bps range: -4.831 to +3.447
- Not a pure low-price-lottery problem.
- PM/Binance side disagreement appears in some cases, but not enough to explain all losses.
- Several losers had weak positive S-K distance around 1-2 bps while paying mid prices.

### final_force_exit

- Count: 8
- PnL: -3.7484
- Entry side: mostly UP, one DOWN
- Entry price range: 0.29-0.57
- Entry model_prob range: 0.561-0.812
- Several were likely direction-correct but forced out at poor bid.
- This suggests final_force_exit is more likely to sell winners too cheaply than to fix bad entries.

## Counterfactual Entry Filters

| Filter | Entries | Trades | Win rate | PnL | Max DD | Read |
|---|---:|---:|---:|---:|---:|---|
| baseline | 57 | 56 | 55.36% | +16.3280 | -5.4121 | current |
| adverse 3s momentum, zero tolerance | 55 | 54 | 55.56% | +15.6148 | -5.4121 | hurts pnl |
| adverse 3s momentum, 0.2 bps tolerance | 55 | 54 | 55.56% | +15.8412 | -5.4121 | hurts pnl |
| skip PM/Binance side disagreement | 51 | 50 | 54.00% | +11.3671 | -4.9326 | too destructive |
| ask > 0.35 and abs(S-K) < 1.5 bps | 54 | 53 | 58.49% | +16.6679 | -5.6344 | small positive |
| ask > 0.35 and abs(S-K) < 2.0 bps | 49 | 48 | 58.33% | +16.7450 | -4.7370 | best tested balance |
| combined momentum + PM side + weak S-K | 41 | 40 | 57.50% | +11.2983 | -4.2565 | over-filters |

Additional threshold check:

| Weak S-K threshold | Entries | Trades | Win rate | PnL | Max DD |
|---:|---:|---:|---:|---:|---:|
| 2.0 bps | 49 | 48 | 58.33% | +16.7450 | -4.7370 |
| 2.5 bps | 47 | 46 | 56.52% | +16.7340 | -4.3350 |
| 3.0 bps | 45 | 44 | 54.55% | +14.9195 | -4.7976 |
| 4.0 bps | 41 | 40 | 52.50% | +14.4053 | -4.1047 |
| 5.0 bps | 37 | 36 | 50.00% | +13.0223 | -4.2481 |

## Recommendation

Do not use PM/Binance side disagreement as a hard entry ban. It removes too many potentially profitable trades.

Do not add a short 3s momentum gate yet. It did not improve the live aggregate.

The most promising lightweight entry filter is:

```text
if ask > 0.35 and abs(S-K in bps) < 2.0:
    skip entry
```

This is easy to make configurable and directly targets the observed weak-distance mid-price entries. It improved win rate and drawdown with only a small PnL gain on current live samples.

Before production use, test on paper/dry-run and collector sets too, because live sample size is still small.
