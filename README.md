# fuding_rate

一个基于 Python 标准库实现的本地套利监控项目，用于对比 `Variational` 与 `Lighter` 两个交易场所的永续合约数据，提供两类核心视图：

- `资金费率套利`：按预计净收益/天排序，评估跨平台对冲 funding 差的机会。
- `差价套利（参考）`：按参考净利润排序，评估买低卖高类价差机会。

项目同时提供：

- 两个可独立运行的 CLI 脚本
- 一个本地 Web Dashboard
- 基于 SQLite 的历史采样与回看能力
- 一组无第三方依赖的单元测试

> 说明：项目输出的是“估算/参考结果”，不是可直接执行的交易建议。真实收益会受到手续费、滑点、深度、成交延迟、资金费结算机制和盘口跳动影响。

## 特性

- 仅依赖 Python 标准库，无需安装第三方包
- 同时支持命令行输出与 JSON 输出
- 资金费率套利与价差套利分开计算，便于独立分析
- Lighter 价格优先使用 `orderBookOrders` 的 best bid/ask 中间价
- 当 Lighter 盘口缺失时，自动回退到 `orderBookDetails.last_trade_price`
- 内置价格比例 guardrail，过滤明显异常的跨平台价格映射
- 本地 `history.db` 持久化历史样本，支持图表回看
- 支持按 symbol 开启后台采样

## 项目结构

```text
.
├── arb_rank.py                 # 资金费率套利排行榜
├── price_arb_cn.py             # 差价套利（中文）排行榜
├── server.py                   # 本地 HTTP 服务 + Dashboard + 历史采样
├── history.db                  # SQLite 历史数据
├── web/
│   ├── index.html              # Dashboard 页面
│   ├── app.js                  # 前端逻辑
│   └── styles.css              # 页面样式
└── tests/
    ├── test_guardrails.py
    ├── test_history_helpers.py
    └── test_lighter_best_bid_ask.py
```

## 数据来源

代码当前直接请求以下公开接口：

- Variational
  - `https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats`
- Lighter
  - `https://mainnet.zklighter.elliot.ai/api/v1/funding-rates`
  - `https://mainnet.zklighter.elliot.ai/api/v1/orderBooks`
  - `https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails`
  - `https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders`

如果上游接口结构变化、返回异常或限流，CLI 和 Dashboard 都会受到影响。

## 运行环境

- Python `3.10+`
- 可访问外网

本地验证环境为 `Python 3.12`。

## 快速开始

### 1. 查看资金费率套利排行

```bash
python3 arb_rank.py --top 20
```

输出 JSON：

```bash
python3 arb_rank.py --json --top 10 --fetch-lighter-last
```

### 2. 查看差价套利排行

```bash
python3 price_arb_cn.py --显示前N 20
```

输出 JSON：

```bash
python3 price_arb_cn.py --json --显示前N 10
```

### 3. 启动本地 Dashboard

```bash
python3 server.py
```

默认地址：

```text
http://127.0.0.1:8099
```

如果 `8099` 端口被占用，可以改用环境变量：

```bash
PORT=18099 python3 server.py
```

然后在浏览器打开：

```text
http://127.0.0.1:18099
```

## 核心脚本说明

### `arb_rank.py`

用途：计算 `Variational` 和 `Lighter` 之间的资金费率套利机会，并按预计净收益/天排序。

默认逻辑：

- Variational funding `funding_rate` 默认按“百分数”解释
  - 例如 `0.1095` 表示 `0.1095%`
- Lighter funding `rate` 默认按“小数”解释
  - 例如 `0.0001` 表示 `0.01%`
- Lighter funding 间隔默认按 `28800s`（8 小时）处理
- Variational 价差估算默认使用 `size_1k`
- Lighter 参考价优先取 best bid/ask 中间价
- 预计净收益定义为：
  - `1天 funding 收益 - 估算开平成本`

常用参数：

- `--notional`：每条腿名义本金，默认 `1000`
- `--top`：返回前 N 条，默认 `30`
- `--fetch-lighter-last`：为部分结果补充 Lighter 价格参考
- `--fetch-lighter-last-limit`：补充价格的条数
- `--lighter-spread-bps`：Lighter 往返滑点/点差假设，默认 `5`
- `--var-fee-bps`：Variational 每笔 taker 手续费假设，默认 `0`
- `--funding-sign`：资金费符号约定，默认 `longs_pay`
- `--min-price-ratio` / `--max-price-ratio`：过滤异常价格映射
- `--symbol-aliases-json`：自定义符号别名映射
- `--json`：JSON 输出

示例：

```bash
python3 arb_rank.py \
  --json \
  --notional 1000 \
  --top 20 \
  --fetch-lighter-last \
  --fetch-lighter-last-limit 20 \
  --lighter-spread-bps 5
```

### `price_arb_cn.py`

用途：计算 `Variational` 与 `Lighter` 的参考价差套利机会，并按往返参考净利润排序。

默认逻辑：

- Variational 使用 `quotes.size_1k` 推导 bid/ask/mid
- Lighter 优先使用 `orderBookOrders` 的 best bid/ask 中间价
- 若无盘口，回退到 `last_trade_price`
- `diff_bps` 表示两个场所参考价格的差异
- `net_u_round_trip` 假设开仓和平仓成本对称

常用参数：

- `--名义本金`：每条腿名义本金，默认 `1000`
- `--显示前N`：显示前 N 条，默认 `30`
- `--并发`：并发抓取 Lighter 盘口，默认 `16`
- `--最多市场数`：最多处理的 Lighter perp 市场数，默认 `120`
- `--缓存秒`：盘口缓存秒数，默认 `30`
- `--Lighter盘口limit`：`orderBookOrders` 深度条数，默认 `50`
- `--Lighter点差bps`：Lighter 单笔点差/滑点假设，默认 `5`
- `--VAR手续费bps`：Variational 单笔手续费假设，默认 `0`
- `--symbol_aliases_json`：符号别名映射
- `--min_price_ratio` / `--max_price_ratio`：价格比例过滤
- `--json`：JSON 输出

示例：

```bash
python3 price_arb_cn.py \
  --json \
  --名义本金 1000 \
  --显示前N 20 \
  --并发 16 \
  --最多市场数 120 \
  --Lighter点差bps 5
```

## Web Dashboard

`server.py` 启动后会提供一个本地页面，主要包含三块内容：

### 1. 资金费率套利表

- 排名依据：`预计净收益/天`
- 展示字段包括：
  - symbol
  - 策略方向
  - funding 收益/天
  - 开平成本
  - 回本天数
  - 两边估算点差
  - 参考基差

### 2. 差价套利表

- 排名依据：`参考净利润（往返）`
- 展示字段包括：
  - symbol
  - 方向提示
  - 参考差价 bps
  - 理论毛利润
  - 往返成本
  - 参考净利润
  - VAR / Lighter 侧价格与点差信息

### 3. 历史价差分析

- 读取 `history.db` 中的历史样本
- 支持按 symbol、时间区间、数据来源筛选
- 支持三类 source：
  - `basis`
  - `price_diff`
  - `funding_basis`
- 如果旧数据缺少双边盘口，会尝试根据历史中位点差估算 bid/ask

## API 说明

### `GET /api/health`

健康检查。

示例：

```bash
curl 'http://127.0.0.1:8099/api/health'
```

### `GET /api/funding`

返回资金费率套利结果。

常用参数：

- `notional`
- `top`
- `lighter_spread_bps`
- `var_fee_bps`
- `fetch_lighter_last`
- `fetch_lighter_last_limit`
- `funding_sign`
- `cache_s`
- `force`
- `timeout_s`

示例：

```bash
curl 'http://127.0.0.1:8099/api/funding?top=10&notional=1000&lighter_spread_bps=5&fetch_lighter_last=1'
```

### `GET /api/price`

返回差价套利结果。

常用参数：

- `notional`
- `top`
- `lighter_spread_bps`
- `var_fee_bps`
- `max_markets`
- `concurrency`
- `orderbook_cache_s`
- `cache_s`
- `force`
- `timeout_s`

示例：

```bash
curl 'http://127.0.0.1:8099/api/price?top=10&notional=1000&lighter_spread_bps=5'
```

### `GET /api/basis_history`

返回某个 symbol 的历史序列。

常用参数：

- `symbol`
- `source`：`basis` / `price_diff` / `funding_basis`
- `range_s`
- `limit`
- `fill_quotes`

示例：

```bash
curl 'http://127.0.0.1:8099/api/basis_history?symbol=BTC&source=price_diff&range_s=86400&limit=1000'
```

### `GET /api/history_symbols`

返回指定时间窗口内有历史数据的 symbol 列表。

常用参数：

- `source`
- `range_s`
- `limit`
- `min_points`
- `min_quote_points`

示例：

```bash
curl 'http://127.0.0.1:8099/api/history_symbols?source=price_diff&range_s=2592000'
```

### `GET /api/watch`

开启或关闭后台采样。

常用参数：

- `symbol`
- `on`
- `interval_s`
- `replace`

示例：

```bash
curl 'http://127.0.0.1:8099/api/watch?symbol=BTC&on=1&interval_s=10'
```

### `GET /api/sample_symbol`

立即采样某个 symbol 一次并写入 `history.db`。

常用参数：

- `symbol`
- `source`

示例：

```bash
curl 'http://127.0.0.1:8099/api/sample_symbol?symbol=BTC&source=price_diff'
```

## 历史数据与缓存

### `history.db`

`server.py` 会在项目根目录维护一个 SQLite 数据库，表名为 `basis_samples`。

主要字段包括：

- `ts`
- `symbol`
- `source`
- `var_mid`
- `lighter_last`
- `bps`
- `var_bid`
- `var_ask`
- `lighter_bid`
- `lighter_ask`

数据写入来源：

- 调用 `/api/funding` 时写入 `funding_basis`
- 调用 `/api/price` 时写入 `price_diff`
- 调用 `/api/watch` + 后台采样时写入 `basis`
- 调用 `/api/sample_symbol` 时写入指定 `source`

### `.cache_price_arb/`

`price_arb_cn.py` 默认会在项目根目录使用 `.cache_price_arb/` 缓存 Lighter 盘口详情和订单簿结果，以减少重复请求。

## 测试

运行全部测试：

```bash
python3 -m unittest discover -s tests -v
```

当前测试覆盖的重点包括：

- symbol alias 归一化
- Lighter 市场元数据过滤
- 价格比例 guardrail
- best bid / ask 提取
- Lighter 参考价选择优先级
- 历史查询参数归一化
- 历史点位下采样逻辑

## 常见问题

### 1. `OSError: [Errno 48] Address already in use`

默认端口 `8099` 已被占用。换一个端口启动即可：

```bash
PORT=18099 python3 server.py
```

### 2. 页面打开后没有数据

优先检查：

- 本机网络是否能访问上游 API
- 上游接口是否临时异常或限流
- 请求是否超时

可以先用 CLI 验证：

```bash
python3 arb_rank.py --top 3
python3 price_arb_cn.py --显示前N 3
```

### 3. 历史图表没有可选 symbol

历史页面依赖 `history.db` 中已有样本。可以先：

```bash
curl 'http://127.0.0.1:8099/api/sample_symbol?symbol=BTC&source=price_diff'
curl 'http://127.0.0.1:8099/api/watch?symbol=BTC&on=1&interval_s=10'
```

## 已知限制

- 不是撮合级别回测，只是基于公开接口的静态估算
- funding 单位和符号约定依赖当前接口语义，若上游变更需要重新校准
- Lighter 侧并非始终能拿到完整盘口，部分场景只能退回 last trade
- 价差套利结果没有纳入完整成交路径、排队、冲击成本和资金占用成本
- 历史采样频率较低时，不适合分析极短周期微结构行为

## 建议改进方向

- 增加真实手续费配置与分平台费率模板
- 增加导出 CSV / Parquet
- 增加告警阈值与 webhook 推送
- 增加多 symbol watch 持久化配置
- 增加更细粒度的历史聚合与统计指标

## License

仓库中当前没有显式 License 文件；如需对外分发，请先补充许可证声明。
