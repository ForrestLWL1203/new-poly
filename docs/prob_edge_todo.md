# Probability Edge TODO

## 2026-05-07 96-Window Dry-Run Follow-Ups

Source log:

```text
/opt/new-poly/logs/prob-edge-paper-96w-20260506T163115Z.jsonl
```

Observed summary:

- Windows: 96
- Entries/exits: 32 / 32
- Paper PnL: +23.3183
- `stale_book`: 1429 skips
- `logic_decay_exit`: 3 exits, all losses
- `order_no_fill`: 40, all exit-side `risk_exit`, all `paper no fill: insufficient bid depth`

### 1. Investigate And Fix Excessive `stale_book`

Goal: determine whether the CLOB market WebSocket is actually stale, only sending BBO updates, missing book snapshots, or whether local `received_at` freshness handling is too strict.

Current finding:

- A 45-second live probe on 2026-05-07 saw normal CLOB market WS activity with hundreds of `book` / `best_bid_ask` events and thousands of `price_change` events; max inbound message gap was under 1 second.
- The 96-window dry-run had several windows where book age climbed for 100-270 seconds with frozen 0.50/0.51 style book values.
- Root cause can be either a true silent CLOB WS / subscription stall or a normal period where no depth-changing events arrive.
- Current mitigation: the CLOB inbound idle watchdog reconnects/resubscribes after 20 seconds without any market WS messages.
- Depth-idle alone no longer forces reconnect by default. A market can legitimately send no depth changes for a few seconds; strategy-level stale-book gates still protect entry/exit decisions.

Questions to answer:

- Are `book`, `price_change`, and `best_bid_ask` events arriving continuously during stale periods?
- Are depth levels fresh enough while `book_age_ms` reports stale?
- Should BBO freshness and full-depth freshness be tracked separately?
- Is `max_book_age_ms=1000` too strict for the current CLOB feed behavior, or is the local cache not updating timestamps correctly?

### 2. Counterfactual Review For 3 `logic_decay_exit` Losses

Goal: decide whether `logic_decay_exit` should remain aggressive, be delayed, or get an additional guard.

Current finding:

- All 3 `logic_decay_exit` trades would have settled as losers if held to settlement.
- Actual exits were roughly `-0.90`, `-0.8696`, and `-0.8095` instead of full `-1.0` loss.
- In this sample, `logic_decay_exit` was not a false-positive early exit; it slightly reduced loss.
- The remaining improvement is not "give more tolerance", but "detect decay earlier or avoid these entries".

For each of the 3 losing exits:

- Reconstruct entry price, entry model probability, exit price, exit model probability, and remaining time.
- Check what would have happened if the position was held until later exits or settlement.
- Classify the counterfactual as: saved loss, exited too early, or unavoidable loss.

### 3. Review 40 Exit-Side `risk_exit` No-Fills

Goal: determine whether no-fills are real depth failures or artifacts caused by stale book data.

Current finding:

- All 40 `order_no_fill` rows were exit-side `risk_exit`.
- All 40 happened with stale or missing book freshness; none happened on fresh book rows.
- Stale ages were short compared with the long WS stalls, roughly `1.1s` to `2.7s`.
- Every no-fill later exited successfully in the same window; later successful exits had total PnL `+22.8242`.
- These look more like "risk exit triggered by short stale freshness" artifacts than true depth failures.
- After the CLOB idle watchdog fix, retest before increasing sell buffer further.

Questions to answer:

- Were the no-fills concentrated during `stale_book` periods?
- Was there executable bid depth shortly before or after the no-fill?
- Did later exits succeed at a better or worse price?
- If most no-fills are stale-book artifacts, fix book freshness before increasing sell buffer further.
