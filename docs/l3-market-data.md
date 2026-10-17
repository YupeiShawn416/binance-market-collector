# L3 接入与 L2 降采样实验

核查日期：2026-10-16；实现版本 v0.3。已验收 Coinbase 历史逐单导入、快照桥接、v2 日志、双消费者和主动扫档对比。Kraken 实时认证路径待验收。

## 数据获取

|来源|结果|
|---|---|
|Kraken `wss://ws-l3.kraken.com/v2`|要求 token；当日无凭据探测超时|
|Coinbase Exchange `full` / `level3`|要求账户认证；当日 WS、REST L3 探测超时|
|Tardis Coinbase 月首历史|HTTP 200，取得2024-01-01首分钟 BTC-USD full 流与 REST L3 快照，解压后12,899,173字节|

历史请求：`from=2024-01-01,offset=0`；通道 `full_snapshot/received/open/done/change/match`。下一分钟的 `full_snapshot` 请求返回空响应。请求记录保存在 `evidence/l3-access-2026-10-16/`。

来源：[Kraken L3](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/level3) · [Coinbase 认证](https://docs.cdp.coinbase.com/exchange/websocket-feed/authentication) · [Tardis Coinbase](https://docs.tardis.dev/historical-data-details/coinbase) · [免费历史范围](https://docs.tardis.dev/api/http-api-reference)

## 模块

```text
Kraken SDK 签名 / token → aiohttp 原始 WS → l3/kraken.py
Tardis 历史 NDJSON → l3/coinbase_history.py
                     ↓ Event v2
SQLite Journal → L3Consumer / replay / Unix socket
                     ↓ L3Projection
逐事件 L3 / 同频 L2 / 因果100ms L2 → 同一扫档函数
```

|文件|职责|
|---|---|
|`l3/contracts.py`|冻结 v2 事件、逐单 ID、整数价格数量、原始十进制文本、时间、sequence / CRC、覆盖范围|
|`l3/book.py`|逐单状态、C 价格有序簿、Kraken CRC、批次校验和检查点|
|`l3/service.py`|复用 Journal / Consumer，候选状态校验后原子提交|
|`l3/kraken.py`|Decimal、纳秒 ISO 时间、add / modify / delete；保留未知删除原因|
|`l3/network.py`|原始行情落盘、有界队列、重连、新快照；认证信息单独管理|
|`l3/coinbase_history.py`|连续历史导入、源序号校验、迟到快照桥接和固定覆盖范围|
|`l3/experiment.py`|主动单探针、扫档对比和100ms盘口导出|
|`scripts/check_coinbase_full.py`|独立全深度 Decimal 重建，核对逐单事实|

沿用 Python 3.13、SQLite WAL/FULL、aiohttp、msgspec、orderbook C 扩展；Kraken SDK 3.5.1 负责 token 认证。原始 WS 由 aiohttp 接收，以保留字节和精度。

L3 使用独立 v2 日志与显式 v2 订阅；v1、v2 客户端按契约隔离。`data_origin` 标识 live / historical / synthetic，并进入事件链、检查点和消费者状态。

## 时间与覆盖

原始26711条消息：1份106918个订单的快照、8867条 received、8380条 open、8860条 done、578条 match、25条 change。full 消息源序号连续。

REST 快照 sequence 为70536614464，收到时 WS 已至70536616379。先回放1915条缓存消息，于 **2024-01-01 00:00:07.1661410 UTC** 发布首份可用状态，实验从此开始。桥接快照的 `exchange_time_ns` 为空；原始 REST / WS 时间保留。

`receive_*`、`normalized_*` 记录本机导入时间，`historical_capture_ns` 记录提供商历史接收时间；实验以 `historical_capture_ns` 驱动。

研究范围固定为初始每侧100个价位界定的价格带：买盘≥42185.50 USD，卖盘≤42626.29 USD。保留范围内每个可见订单和每次更新，范围耗尽后的剩余成交标记为未知。完整原始快照与范围外消息保留在原始日志。

Coinbase 快照缺少存量订单入队时间，`queue_order_known=false`；本轮验证主动扫档。Kraken 按订阅深度截断，价位退出标记为范围变化；每批验证前10个价位的官方 CRC，重连后重新获取快照。

## 实验结果

样本：2024-01-01 00:00:07.1661410–00:00:59.9922905 UTC，约52.8261495秒。输入 SHA256：

`c1b45646c134596da97bec1bc37e9969dedccf3e777793f66b4baa38e1919327`

每10ms分别提出买 / 卖0.0001、0.001、0.01 BTC，共31698个主动单探针。探针共享同一段行情，每笔从当时可见簿独立重置，逐档扣量，保留空价位。

三组输入：逐事件 L3、同频按价位合计的 L2、每100ms边界采样并持有至下一边界的 L2。采样相位固定在首份可用快照。

|指标|结果|
|---|---:|
|同频 L2 / L3 扫档差异|0|
|100ms L2 / L3 均价不同|5561 / 31698，17.5437%|
|平均绝对均价差|0.06746369 bps|
|最大绝对均价差|14.15186357 bps|
|已知成交数量不同|0|
|超出已知范围|0|

最大差异示例：0.0001 BTC 买单，逐事件均价42291.25 USD，100ms视图42351.10 USD，相差59.85 USD/BTC。指标度量视图导致的假想扫档成本差；1bps=0.01%。

独立 Decimal 实现处理快照后23982条消息，最终全簿106943个订单；筛入同一价格带的240个订单，其 ID、价格和数量与生产投影一致。

两个 socket 消费者读取22073个 v2 事件，其中一个在 seq=25 退出后恢复；逐事件哈希、订单状态哈希和最终链与历史回放一致。该版本73项回归、Ruff及82份 vendor 校验通过。

输出位于 `evidence/l3-engine-2026-10-16/`：导入摘要、对比 JSON、全部探针 CSV、529份100ms盘口、NDJSON、v2 SQLite 和消费者验收。合成真空场景单独存档。

## 复现

准备上述本地样本后执行：

```bash
uv sync --frozen --extra dev
uv run market-service l3-verify --db evidence/l3-engine-2026-10-16/coinbase-history.sqlite
uv run market-service l3-compare \
  --db evidence/l3-engine-2026-10-16/coinbase-history.sqlite \
  --csv data/coinbase-probes.csv --report data/coinbase-comparison.json
uv run market-service replay --schema-version 2 \
  --db evidence/l3-engine-2026-10-16/coinbase-history.sqlite
```

历史导入使用新数据集；失败后从新路径重试。只有 `import_complete=1` 的日志可供只读消费者和实验使用。

```bash
uv run market-service l3-import-coinbase \
  --source evidence/l3-engine-2026-10-16/coinbase-2024-01-01-0000.ndjson \
  --db data/coinbase-reimport.sqlite --levels 100 --report data/import.json
```

Socket 发布和消费统一使用 `--schema-version 2`，各消费者使用独立 state 文件。

Kraken：安装 `--extra kraken`，将 key / secret 存入本机0600 JSON或 `KRAKEN_MARKET_KEY` / `KRAKEN_MARKET_SECRET` 环境变量，运行 `l3-collect --credentials-file 本机路径 --db data/kraken.sqlite --depth 100 --seconds 60`。

## 研究选择

L2适合价差、深度、不平衡、可见价格空洞、扫档成本与成交压力研究。L3补充订单身份、生命周期和队列信息，适用于被动挂单与撤改单研究；执行校准还需场所规则、成交回报和延迟。

信息层级与采样周期分别评估：同一时刻、范围和主动订单，在忽略后续市场反应的条件下，同频 L2 / L3 提供相同可扫数量。100ms采样误差需跨日期、波动区间、采样相位和订单大小验证。

待验收：Kraken 实时认证、被动成交、市场冲击和持续负载。L3 每批候选状态复制、全状态哈希及 Python CRC 的性能需实测。
