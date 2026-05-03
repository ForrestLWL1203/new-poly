# Probability Edge Strategy 代码 Review

Date: 2026-05-03

Review 范围：

- [new_poly/strategy/prob_edge.py](../new_poly/strategy/prob_edge.py)
- [new_poly/strategy/probability.py](../new_poly/strategy/probability.py)
- [new_poly/strategy/state.py](../new_poly/strategy/state.py)
- [new_poly/trading/execution.py](../new_poly/trading/execution.py)
- [new_poly/trading/clob_client.py](../new_poly/trading/clob_client.py)
- [new_poly/trading/fak_quotes.py](../new_poly/trading/fak_quotes.py)
- [scripts/run_prob_edge_bot.py](../scripts/run_prob_edge_bot.py)
- [configs/prob_edge_mvp.yaml](../configs/prob_edge_mvp.yaml)
- [docs/prob_edge_strategy_bot.md](../docs/prob_edge_strategy_bot.md)

整体结论：骨架对了（BS 概率、size-aware edge、FAK-only、paper/live 双模式、StrategyState、settlement 计算 PnL），但**离 Final Design Plan 还有不少缺口**，并且发现 1 个严重 bug 和几个会影响 live mode 的实现错误。

---

## 严重问题（必须修）

### 1. DVOL 启动时取一次，整轮不再刷新

[run_prob_edge_bot.py:251-252](../scripts/run_prob_edge_bot.py#L251-L252)：

```python
volatility = await asyncio.to_thread(fetch_dvol_snapshot)
sigma_eff = volatility.sigma
```

跑 12 个 window（1 小时）期间 σ_eff 是**冻结**的。BTC 进入波动行情时 DVOL 早已上行而模型还在用 1 小时前的值 → fair_prob 严重偏离 → edge 看起来变大 → **在风暴里加仓**。

doc 写"约每分钟刷新 DVOL"，代码完全没有 refresh 逻辑。

**Fix**：主循环里加一个非阻塞的 DVOL 刷新任务（比如每 60s 后台 `asyncio.create_task(fetch_dvol_snapshot)` 更新缓存），sigma 失败时 fail-closed（保留旧值或拒绝交易）。

### 2. `_snapshot` 里 `sigma_eff` 永远是 `None`，靠外面"打补丁"

[run_prob_edge_bot.py:213-214](../scripts/run_prob_edge_bot.py#L213-L214)：

```python
volatility_sigma = None
snap = MarketSnapshot(..., sigma_eff=volatility_sigma, ...)
```

然后在 [L281](../scripts/run_prob_edge_bot.py#L281) 通过 `MarketSnapshot(**{**snap.__dict__, "sigma_eff": sigma_eff})` 替换。这是**两步构造一个不一致对象**的反模式。

**Fix**：把 `sigma_eff` 当参数传进 `_snapshot`，一次构造对。

### 3. Live FAK 的 price hint 计算错误，buffer 等于无效

[execution.py:133](../new_poly/trading/execution.py#L133)：

```python
price_hint = buffer_buy_price_hint(token_id, max_price, buffer_ticks=..., max_price=max_price)
```

把 `max_price` 同时当作 `best_ask`（位置参数）和 `max_price`（关键字参数）。`buffer_buy_price_hint` 内部（[fak_quotes.py:73-76](../new_poly/trading/fak_quotes.py#L73-L76)）：

```python
buffered = best_ask + tick * ticks   # = max_price + buffer
buffered = min(buffered, max_price)  # = max_price
```

净效果：**hint == max_price**，buffer 完全失效。

Plan 里 FAK 的关键设计是 `min(best_ask + buffer_ticks, fair_prob - required_edge_floor)` —— 当 best_ask 比 max_price 略低时，需要把订单价抬起来一点提升 fill 概率。当前实现等价于挂在 ask_avg 价位，会显著降低成交率。

**Fix**：把 `_snapshot` 里的 `best_ask`（已经在 `up["ask"]` / `down["ask"]` 里）一路传到 buy()，然后 `buffer_buy_price_hint(token_id, best_ask, ..., max_price=max_price)`。

### 4. Settlement 用 Binance 价决定胜负方（结算源问题）

[run_prob_edge_bot.py:329-332](../scripts/run_prob_edge_bot.py#L329-L332)：

```python
winning_side = "up" if feed.latest_price > prices.k_price else "down"
```

Polymarket BTC 5m 是 **Chainlink** 解算，不是 Binance。用 Binance proxy 在边界情况（BTC 距 K ±$1）会算反，**悄悄记错账**。

#### 关于"Chainlink 无免费数据源"这个前提

不准确——有两条免费路径：

##### 选项 A：直接读 Chainlink on-chain（真免费）

Chainlink BTC/USD aggregator 在 Polygon 上的合约地址：

```
0xc907E116054Ad103354f2D350FD2514433D57F6f
```

`latestRoundData()` 和 `getRoundData(roundId)` 是 **view 函数，调用不花 gas**。需要：

- 一个免费 Polygon RPC（Alchemy free tier / Ankr 公共 RPC / `polygon-rpc.com`）
- `web3.py` 或 `eth-abi` 直接调用

实现量：~30 行代码。给 window end timestamp，二分搜索找最接近的 `roundId`，读出 `answer` 和 `updatedAt`。

##### 选项 B：用 Polymarket 自己的 resolution（最简单）

直接问 Polymarket 这个 market 解算成什么了。Polymarket 在 settlement 后会更新自己的 market metadata：

- `gamma-api.polymarket.com/events/...` 或 `/markets/...` 返回 `resolved=true` + `resolvedOutcome`
- 这是**真正决定 PnL 的源**——不管底层用 Chainlink 还是别的，Polymarket 的解算结果就是钱进出的依据

实现量：~10 行 HTTP poll。代价是要等 Polymarket 解算（通常窗口结束后几秒到几十秒）。

##### 推荐方案：B 主 + A/Binance 副

```
window 结束 → 立刻用 Binance/feed.latest_price 估一个 winning_side（"早期估计"）
            → 再 poll Polymarket /markets/{slug} 直到 resolved
            → 用 Polymarket 解算结果重写 settlement_pnl，覆盖早期估计
            → log 两者，发现不一致时报警
```

理由：

- Polymarket 解算结果 = 真实 PnL，**这是账本来源**
- Binance 只能当"我现在大概赚没赚"的早期信号，不能当账本
- 边界情况（BTC 距 K 几美元）Binance 和 Chainlink 经常分歧

##### 如果坚持 paper 阶段不接，至少加边界 flag

```python
abs(feed.latest_price - prices.k_price) < BOUNDARY_USD  # 比如 5
→ row["settlement_uncertain"] = True
→ 不计入 realized_pnl 的回测统计
```

paper 阶段这个边界样本占 5-10%，不剔除会让 PnL 估计有系统性偏差。

---

## 与 Final Design Plan 的明显偏离

### 5. 没有时间带分级 required_edge

Plan 说：早期（15-120s）≥10%，核心（120-240s）5-7%，谨慎区（240-270s）需要更新鲜数据。

代码 [prob_edge.py:90](../new_poly/strategy/prob_edge.py#L90)：

```python
if up_edge >= cfg.required_edge:  # 全程 0.05
```

整个入场窗口用一个固定 `0.05`。最危险的窗口尾段反而和早期同等开仓。

**Fix**：`EdgeConfig` 加一个 `required_edge_by_age(age_sec) -> float`（或一个 `[(age_lo, age_hi, edge), ...]` 表）。

### 6. 没有同 token 出场后的冷却（5s）

Plan 写明 `default same-token cooldown after exit: 5s`，按出场原因分级（logic_decay/risk_exit 拉长到 30-60s，take-profit 保持 5s）。

代码：[state.py:45-53](../new_poly/strategy/state.py#L45-L53) 的 `record_exit` 把 `open_position` 置 None，下一秒就能立刻再开仓。在 logic_decay 出场后秒级再进非常容易被同一信号反复套住。

**Fix**：`StrategyState` 加 `last_exit_time / last_exit_reason / cooldown_until`，`evaluate_entry` 头部加冷却检查。

### 7. 缺 `defensive_take_profit` 和 last-60s profit-protection 出场

Plan 列了 4 种出场，代码只实现 3 种 ([prob_edge.py:101-123](../new_poly/strategy/prob_edge.py#L101-L123))：logic_decay / market_overprice / risk_exit。缺：

- **defensive_take_profit**：bid 已盈利 + 模型优势在衰减时主动了结
- **最后 60s 内有盈利时逐步收紧 profit-protection buffer**

### 8. 缺深度稳定性过滤 + 超大档位 cap/discount

Plan 写明：只统计可见 ≥ 2-3s 的深度；单一超大档位要 cap 或 discount 避免被一笔伪挂单主导。

代码 [collect_prob_edge_data.py:246](../scripts/collect_prob_edge_data.py#L246) 的 `token_state` 直接读当前快照算 `avg_price_for_notional`，没有时间窗口过滤，没有单档位封顶。**不上这条，被 spoof 的概率不低**。

### 9. `required_edge` 单一常数，没分量

doc 列了 base + spread + latency + volatility + stale + time + depth penalty 七项，代码全部塌缩为 `0.05` 一个数。

**Fix**：先把结构搭起来——`evaluate_entry` 里组合 `compute_required_edge(snapshot, cfg) -> float`，即使各 penalty 默认是 0，也方便后面接入。

### 10. Logic decay exit 无滞后，会抽搐

[prob_edge.py:119](../new_poly/strategy/prob_edge.py#L119)：

```python
if model_prob < position.entry_avg_price - cfg.model_decay_buffer:
    return ... "logic_decay_exit"
```

BTC 一秒动 $50 就能把 model_prob 推过去 → 立刻平仓 → 下一秒回来 → 错过。

**Fix**：`PositionSnapshot` 加 `decay_streak` 计数器，连续 N 秒（建议 2-3 次 tick）满足条件才出。或者 `entry - now > 30s` 才允许 decay 出场。

### 11. 没有 fast-RV penalty，σ 完全靠 DVOL

Plan：MVP 当前规则就是"RV_fast 显著大于 DVOL-implied 时 +1~2% 到 required_edge"。代码现在没有 RV 计算，σ_eff = DVOL/100 一条路走到底。

`BinancePriceFeed` 已经有价格历史，加一个 `last_5min_rv()` 不难。

---

## 中等问题

### 12. `fak_quotes.py` 是死代码

[fak_quotes.py](../new_poly/trading/fak_quotes.py) 写得挺细（cap_limited_depth_quote 等），但 `execution.py` 和 `prob_edge.py` 都没用，自己又实现了一套 `_avg_buy_fill` / `avg_price_for_notional`。两套 depth 计算逻辑，**注定在 size-aware 边界条件上不一致**（`fak_quotes.get_tick_size` 写死 0.001，与 `clob_client.get_tick_size` 不同源）。

**Fix**：要么删掉 fak_quotes.py，要么把 `execution.py` 改造成调它的 cap_limited_depth_quote / stop_loss_bid_quote 并删掉重复实现。建议后者，因为 fak_quotes 已经有 stability/skip levels 的钩子。

### 13. `paired_buffer` 配置加载但未使用

[run_prob_edge_bot.py:161](../scripts/run_prob_edge_bot.py#L161) 加载了 `runtime.paired_buffer`，整个 bot 路径里再没出现过。要么实现（用作两边 ask_sum < 1 + buffer 的 cross-token sanity），要么从配置删掉。

### 14. `final_no_entry_remaining_sec=30` 与 `entry_end_age_sec=270` 重复

5 分钟窗口下两者意思相同（age=270 ⇔ remaining=30）。[prob_edge.py:77-80](../new_poly/strategy/prob_edge.py#L77-L80) 两条 if 都生效但不冲突。可以保留作显式守卫，但要注释清楚是有意冗余。

### 15. Paper depth_notional 与 amount_usd 不绑定

`depth_notional=5.0` 决定 `ask_depth_ok` 判定，`amount_usd=5.0` 决定真实下单。当前默认都是 5 巧合一致。一旦 `amount_usd > depth_notional`，`evaluate_entry` 用浅深度判定 OK，paper fill 反而会"幸运地"越走越深。

**Fix**：`token_state` 的 `depth_notional` 默认就用 `execution.amount_usd`（同源）。

### 16. PriceStream `on_price` 回调返回未 await 的协程

[run_prob_edge_bot.py:250](../scripts/run_prob_edge_bot.py#L250)：

```python
stream = PriceStream(on_price=lambda _update: asyncio.sleep(0))
```

如果 `PriceStream` 调用方没 await 这个返回值，会有 `coroutine was never awaited` warning。不需要回调直接传 `None` 或返回 `None` 的同步 lambda。

### 17. DVOL 失败直接整盘崩

[run_prob_edge_bot.py:251-252](../scripts/run_prob_edge_bot.py#L251-L252) 不在 try 里，Deribit 临时挂掉 → bot 启动直接 SystemExit 1。应该 retry / 等下一次 refresh / 或在第一次失败时降级到一个保守的 σ floor。

### 18. settlement 时如果 `feed.latest_price` 是 None，结果是"没记录"

[run_prob_edge_bot.py:329](../scripts/run_prob_edge_bot.py#L329)：

```python
if state.has_position and ... feed.latest_price is not None:
```

Binance 断流时持仓窗口结束 → 不调 `record_settlement` → 持仓挂着进入下一窗口（被 `state.reset_for_market` 清掉但 PnL 没结算）。**会丢账**。

**Fix**：分支补一个 `if state.has_position: state.record_exit_at_market(...) or state.mark_lost(...)`，至少把 entry_price 当成最终成交价记一笔（或显式标 unsettled 让人工核对）。

接入选项 B（Polymarket resolution API）后这条天然解决——以 Polymarket 解算为准，Binance 断流不影响账本。

---

## 写得对/写得好的地方

- BS 概率正确（[probability.py](../new_poly/strategy/probability.py)）：边界处理、d2 公式、return 在 [0,1] 截断都到位。
- `evaluate_entry` 拒绝条件层次清晰（[prob_edge.py:73-85](../new_poly/strategy/prob_edge.py#L73-L85)），方便调试。
- size-aware `_avg_buy_fill` / `_avg_sell_fill` 用 `take = min(size, remaining/price)` 是对的。
- 不持仓时不评估 exit、持仓时不评估 entry —— 干净的状态切换。
- `record_settlement` 用 `entry_avg_price * filled_shares` 算 PnL 而不是用 last bid，这才是 binary settle 的真 PnL。
- `LiveFakExecutionGateway` 强制 `live_risk_ack` 在构造时校验，无法绕过。
- `clob_client` 的 `get_token_balance(safe=True)` 会按 tick 截断 —— 防止下单 amount 超过实际 balance 被拒。

---

## 优先级排序的修复清单

| 优先级 | Item | 理由 |
|---|---|---|
| P0 | DVOL 周期刷新（#1） | 不修就在波动行情里系统性亏 |
| P0 | Live FAK price hint bug（#3） | 直接降低 live 成交率 |
| P0 | 接入 Polymarket resolution API（#4 选项 B） | 真账本来源，~10 行 |
| P0 | settlement 不丢账（#18） | 接 #4 后天然解决 |
| P1 | `_snapshot` sigma_eff 一次构造（#2） | 防御性，避免后续误用 |
| P1 | 同 token 出场冷却（#6） | 防 logic_decay 反复套牢 |
| P1 | logic_decay 出场加滞后（#10） | 抽搐止损 |
| P1 | 时间带分级 required_edge（#5） | 与 plan 对齐 |
| P2 | Chainlink on-chain 副源（#4 选项 A） | 审计副源，可拖到 live 之后 |
| P2 | 删/接入 fak_quotes.py（#12） | 两套实现迟早分歧 |
| P2 | depth stability + 超大档位 discount（#8） | 抗 spoof |
| P2 | defensive_take_profit + last-60s 收紧（#7） | 与 plan 对齐 |
| P3 | fast-RV penalty（#11） | σ_eff 升级路线 |
