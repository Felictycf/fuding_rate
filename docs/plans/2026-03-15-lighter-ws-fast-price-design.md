# Lighter WebSocket Fast Price Design

> **Summary:** 用 Lighter 官方 `order_book/{market_id}` WebSocket 常驻维护本地盘口缓存，把 `/api/price?detail_level=fast` 的热路径从 “2 个全局 REST + N 个 market 明细 REST” 收敛到 “2 个全局 REST + 本地内存读取 + 少量缺口回退”。目标是把差价套利主排名的新鲜度从约 4 秒压低到约 1 秒附近，同时保留 full 明细与 live 兜底。

## Goal

让 `差价套利(参考)` 的主排名与主收益字段尽量实时，优先使用本地 WebSocket 盘口快照返回；`资金费率套利` 保持当前 fast/full 结构，但复用本地 Lighter 盘口缓存来加快明细补充。

## Current State

- `funding fast` 已经是少量 REST 请求，瓶颈不大。
- `price fast` 当前仍需拉 `Variational stats`、`Lighter orderBooks`，再对最多 40 个市场逐个请求 `orderBookDetails`，热路径计算约 3 秒，快照年龄约 4 秒。
- 当前前端已支持 fast/full 双阶段渲染，并显示返回耗时与快照年龄。

## Chosen Approach

### 1. 常驻 Lighter WebSocket 盘口缓存

- 新增 `LighterOrderBookStream` 后台组件。
- 启动后先通过 REST 获取 `Lighter orderBooks` 与 `Variational stats`，求出公共 symbol 集合。
- 为这些公共市场建立 `symbol -> market_id` 映射，并订阅官方 `order_book/{market_id}` 频道。
- 本地只维护 fast 排名需要的最小状态：
  - `best_bid`
  - `best_ask`
  - `mid`
  - `updated_at`
  - `market_id`
  - `source`

### 2. price fast 优先读 WS，本地缺口再回退 REST

- `build_price_payload_fast()` 先读 REST 的 `Variational stats` 与 `Lighter orderBooks` 元信息。
- 对于候选 market：
  - 若本地 WS 缓存存在有效盘口，则直接用 `best bid/ask` 计算 `L_ref` 与排名。
  - 若 WS 尚未覆盖该 symbol，则只对缺口 symbol 回退 `orderBookDetails`。
- payload 增加 WS 元数据，至少包含：
  - `lighter_ws_connected`
  - `lighter_ws_subscribed`
  - `lighter_ws_age_ms`
  - `lighter_ws_covered`
  - `lighter_ws_fallback_count`

### 3. funding fast / full 复用本地价格缓存

- funding 的主排序仍以 funding 费率和假设成本为主，不强依赖 WS。
- funding 的基差类字段优先读本地 Lighter 盘口缓存；如果缓存没有该 symbol，再保持现有行为。
- 这样 funding 页不会因为切换到 WS 而增加复杂度，但会受益于更快的盘口补充。

### 4. 连接与证书策略

- 默认使用系统证书连接 WebSocket。
- 若握手出现 `CERTIFICATE_VERIFY_FAILED`，允许像现有 HTTP 层一样进行一次受控的无校验重试，并打印警告日志。
- WebSocket 断线后自动重连，保留现有快照和 REST 回退能力。

## Data Flow

1. 后台线程刷新公共市场映射。
2. WebSocket 线程订阅 `order_book/{market_id}`，维护每个市场的顶级盘口。
3. `price fast` 请求到来时：
   - 拉 `Variational stats`
   - 读取本地 Lighter 盘口缓存
   - 仅对缺口 symbol 调 `orderBookDetails`
   - 生成快照 payload
4. 前端继续沿用 fast/full 双阶段渲染，并额外显示 WS 覆盖率和年龄。

## Error Handling

- WS 未连接或覆盖不完整时，不中断接口，直接回退 REST。
- WS 已连接但某个 symbol 盘口为空时，仅该 symbol 回退 REST。
- 若市场映射刷新失败，继续使用上一次成功的映射。
- 所有新增逻辑必须保持 `/api/price?force=1` 和 full 路径可用。

## Testing

- 单测：验证 order book 增量合并逻辑能正确维护 best bid/ask。
- 单测：验证 `build_price_payload_fast()` 在 WS 已覆盖时不会对已覆盖 symbol 请求 `orderBookDetails`。
- 单测：验证缺口 symbol 仍会 REST 回退。
- 单测：验证 WS 元数据会出现在 fast payload 中。
- 运行态验证：
  - 服务启动
  - `/api/price?detail_level=fast` 多次采样
  - 比较 `fetch_ms`、`snapshot_age_ms`、WS 覆盖率

## Non-Goals

- 这次不重写 full 路径。
- 这次不引入第三方行情中间层。
- 这次不改前端表结构，只补状态展示。
