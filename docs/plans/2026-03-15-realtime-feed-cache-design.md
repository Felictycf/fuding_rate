# Realtime Feed Cache Design

> **Summary:** 这次实现 A 后端 + B 前端过渡的第一阶段：后端增加常驻行情 feed cache，让 `funding fast` 和 `price fast` 从“请求时抓网络”变成“请求时读内存 + 本地 WS 状态”，前端继续保留现有 HTTP 读取与 fast/full 展示。

## Goal

把当前项目的数据获取延时进一步压低，尤其是 `funding fast` 和 `price fast` 的 live 计算延时；不改变现有网页交互方式，但让后端具备参考系统式的常驻数据状态。

## Scope

- 新增常驻 feed cache：
  - Variational `stats`
  - Lighter `orderBooks`
  - Lighter `funding-rates`
- fast 构建函数优先读取 feed cache，而不是直接走网络。
- 保留现有：
  - Lighter order book WebSocket 本地状态
  - fast/full 双阶段渲染
  - snapshot 接口与参数

## Architecture

### 1. Feed Cache

新增进程级 feed cache，保存：
- `data`
- `updated_at`
- `last_error`
- `refresh_interval_s`

三类 feed：
- `var_stats`
- `lighter_orderbooks`
- `lighter_funding`

### 2. Background Refresh

服务启动后开启后台刷新线程：
- `var_stats` 高频刷新
- `lighter_funding` 高频刷新
- `lighter_orderbooks` 低频刷新

这样网络 I/O 从请求链路挪到后台。

### 3. Fast Builders

`build_funding_payload_fast()` 与 `build_price_payload_fast()`：
- 优先使用 cache 里的原始 feed 数据
- Lighter 价格继续优先读本地 WS 盘口
- cache 缺失时再单次回退 live fetch

### 4. Frontend

前端维持当前模式：
- 继续请求 `/api/funding` 和 `/api/price`
- 继续显示快照年龄/计算耗时
- 新增 feed cache 年龄显示，帮助判断“数据新鲜度来自哪里”

## Expected Impact

- `funding fast`：live 计算从秒级进一步压到数十毫秒到数百毫秒
- `price fast`：live 计算继续下降，主要剩余成本是本地计算与少量 fallback
- 命中快照路径仍保持毫秒级返回

## Non-Goals

- 这次不改成 SSE / WebSocket 前端推送
- 这次不实现 symbol 级增量排名引擎
- 这次不重写 full 路径
