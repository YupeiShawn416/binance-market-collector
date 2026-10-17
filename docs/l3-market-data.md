# L3 接入与 L2 降采样实验

L2 保存每个价格的挂单总量，L3 保存每笔可见订单的 ID、价格、数量和生命周期。本页记录一项历史实验：将同一段逐单行情保留为 L3、合并为同频 L2、降采样为100ms L2，再比较三种视图下的主动成交计算。

主动扫档指按最优价格向更远价格逐档买入或卖出。它需要每个价格的可见数量；被动挂单研究还涉及订单在队列中的位置。本实验使用独立的假想主动单，研究数据视图与更新频率对扫档结果的影响。

核查日期：2026-10-16；实现版本 v0.3。Coinbase 历史导入、快照衔接、v2 日志、双消费者和扫档对比已完成验收。Kraken 实时认证路径列入后续验收。本仓库的币安主采集路径使用 v1，L3 扩展使用独立 v2 数据集。

## 实验数据来源

本次分别探测交易所实时接口和历史数据提供商。下表记录2026-10-16的访问条件与结果：

|来源|结果|
|---|---|
|Kraken `wss://ws-l3.kraken.com/v2`|要求认证 token；当日使用公开连接探测，连接超时，凭据待配置|
|Coinbase Exchange `full` / `level3`|要求账户认证；当日 WS、REST L3 探测超时|
|Tardis 历史数据服务的 Coinbase 月首样本|HTTP 200，取得2024-01-01首分钟 BTC-USD full 流与 REST L3 快照，解压后12,899,173字节|

历史样本以 NDJSON 保存，即每行一条 JSON 消息。请求参数：`from=2024-01-01,offset=0`；通道 `full_snapshot/received/open/done/change/match`。其中 full_snapshot 提供全簿快照，received / open / done / change / match 分别记录订单接收、入簿、结束、修改和成交。下一分钟的 `full_snapshot` 请求返回空响应。请求记录保存在本地 `evidence/l3-access-2026-10-16/`。

来源：[Kraken L3](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/level3) · [Coinbase 认证](https://docs.cdp.coinbase.com/exchange/websocket-feed/authentication) · [Tardis Coinbase](https://docs.tardis.dev/historical-data-details/coinbase) · [免费历史范围](https://docs.tardis.dev/api/http-api-reference)

## 行情重建与覆盖范围

完整盘口从快照开始，再依次应用订单事件。快照请求与消息接收并行，程序用源序号 sequence 将快照与缓存事件衔接，这一步称为快照桥接。

原始26711条消息：1份106918个订单的快照、8867条 received、8380条 open、8860条 done、578条 match、25条 change。full 消息源序号连续。

本次 REST 快照 sequence 为70536614464，收到快照时，WebSocket（WS）消息已至70536616379。先回放1915条缓存消息，于 **2024-01-01 00:00:07.1661410 UTC** 发布首份可用状态，实验从此开始。桥接快照的 `exchange_time_ns` 为空；原始 REST / WS 时间保留。

`receive_*` 表示本机读取历史文件的时间，`normalized_*` 表示本机生成统一事件的时间。`historical_capture_ns` 保存提供商当年的消息接收时间，实验使用这条历史时间轴推进。

研究范围固定为初始每侧100个价位界定的价格带：买盘≥42185.50 USD，卖盘≤42626.29 USD。保留范围内每个可见订单和每次更新，范围耗尽后的剩余成交标记为未知。完整原始快照与范围外消息保留在原始日志。

Coinbase 快照提供存量订单和数量，存量订单的原始入队时间缺失，字段记为 `queue_order_known=false`。实验据此计算主动扫档，被动队列研究需要进一步补充顺序信息。

Kraken 适配器按订阅深度维护覆盖范围，价位退出时标记范围变化；每批计算前10个价位的 CRC 校验码，与交易所结果比较，重连后重新获取快照。

## 三种视图与对比方法

样本：2024-01-01 00:00:07.1661410–00:00:59.9922905 UTC，约52.8261495秒。输入 SHA256：

`c1b45646c134596da97bec1bc37e9969dedccf3e777793f66b4baa38e1919327`

三组视图使用相同原始输入和相同扫档函数：

|视图|生成方式|
|---|---|
|逐事件 L3|每个订单事件到来后更新逐单状态|
|同频 L2|在相同事件时刻，将逐单数量按价格合计|
|100ms L2|从首份可用快照起，每100ms读取边界时刻已经到达的盘口，在下一边界前持续使用该值|

每10ms分别提出买 / 卖0.0001、0.001、0.01 BTC，共31698个假想主动单，称为探针。每笔探针从对应时刻的盘口独立计算，按价格逐档累计可成交数量与金额，得到成交均价。探针对后续行情的市场影响设为0。

## 结果与解释

价差以 bps 表示，1bps=0.01%。本段样本的结果如下：

|指标|结果|
|---|---:|
|同频 L2 / L3 扫档差异|0|
|100ms L2 / L3 均价不同|5561 / 31698，17.5437%|
|平均绝对均价差|0.06746369 bps|
|最大绝对均价差|14.15186357 bps|
|已知成交数量不同|0|
|超出已知范围|0|

最大差异示例：0.0001 BTC 买单，逐事件均价42291.25 USD，100ms视图42351.10 USD，相差59.85 USD/BTC。这一差异来自两种视图在探针时刻保存的深度差，指标度量本段样本中的假想扫档成本。

用于交叉检查的独立程序采用 Decimal 十进制精确运算，处理快照后23982条消息，最终全簿106943个订单；筛入同一价格带的240个订单，其 ID、价格和数量与本项目的逐单状态对象一致。

两个本机 socket 消费者各自接收全部22073个统一 v2 事件并重建逐单状态。其中一个处理到 seq=25 后退出，再从检查点恢复；两端逐事件哈希、订单状态哈希和最终事件链与历史回放一致。该版本73项回归、Ruff及82份 vendor 校验通过。

输出位于 `evidence/l3-engine-2026-10-16/`：导入摘要、对比 JSON、全部探针 CSV、529份100ms盘口、NDJSON、v2 SQLite 和消费者验收。合成真空场景单独存档。

## 实现模块与接口

原始输入先落入 SQLite 日志 Journal，解析后生成 Event v2。L3Consumer 是带校验与持久进度的消费者，L3Projection 是它维护的逐单盘口状态。实时订阅和回放共用这些组件，基本消费流程见[行情接口](event-contract.md)。

```text
Kraken 实时 WS / Tardis 历史 NDJSON
    ↓ 原始输入落盘、按各场所协议解析
Event v2 → SQLite Journal → socket 订阅 / 历史回放
                                  ↓ L3Consumer 校验
                             L3Projection 逐单状态
                                  ↓
                逐事件 L3 / 同频 L2 / 100ms L2 → 同一扫档函数
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

基础设施复用币安采集器的 Python、SQLite 事务日志、aiohttp 网络客户端、msgspec 类型与编码，以及 orderbook C 扩展盘口。Kraken SDK 3.5.1 负责 token 认证。原始 WS 由 aiohttp 接收，以保留字节和精度。

L3 使用独立 v2 日志与显式 v2 订阅；v1、v2 客户端按契约隔离。`data_origin` 标识 live（实时）、historical（历史）或 synthetic（合成），并进入事件链、检查点和消费者状态，便于核查实验来源。

## 复现

本地 `evidence/l3-engine-2026-10-16/` 保存本次历史样本和日志。准备对应文件后，依次核验逐单日志、运行扫档对比并回放：

```bash
uv sync --frozen --extra dev
uv run market-service l3-verify --db evidence/l3-engine-2026-10-16/coinbase-history.sqlite
uv run market-service l3-compare \
  --db evidence/l3-engine-2026-10-16/coinbase-history.sqlite \
  --csv data/coinbase-probes.csv --report data/coinbase-comparison.json
uv run market-service replay --schema-version 2 \
  --db evidence/l3-engine-2026-10-16/coinbase-history.sqlite
```

从原始 NDJSON 重新导入时，使用新数据集路径。导入完成标记为 `import_complete=1`，只读消费者和实验据此确认整段样本就绪。导入失败后使用新路径重试。

```bash
uv run market-service l3-import-coinbase \
  --source evidence/l3-engine-2026-10-16/coinbase-2024-01-01-0000.ndjson \
  --db data/coinbase-reimport.sqlite --levels 100 --report data/import.json
```

Unix socket 用于同机进程间传递事件。发布和消费统一使用 `--schema-version 2`，各消费者使用独立 state 文件。

Kraken 实时接入需要账户行情凭据。将 key / secret 存入权限0600的本机 JSON，或配置 `KRAKEN_MARKET_KEY` / `KRAKEN_MARKET_SECRET` 环境变量，再运行：

```bash
uv sync --frozen --extra kraken
uv run market-service l3-collect --credentials-file /path/to/credentials.json \
  --db data/kraken.sqlite --depth 100 --seconds 60
```

`--depth 100` 表示订阅每侧100档价格深度，凭据文件路径替换为本机实际位置。使用环境变量时省略 `--credentials-file`。

## 研究数据选择

L2适合价差、深度、不平衡、可见价格空洞、扫档成本与成交压力研究。L3补充订单身份、生命周期和队列信息，适用于被动挂单与撤改单研究；执行校准还需场所规则、成交回报和延迟。

信息层级与采样周期分别评估：在相同可见价格范围内，同一时刻的同频 L2 / L3 对独立主动单提供相同可扫数量。100ms采样误差需跨日期、波动区间、采样相位和订单大小验证。

待验收：Kraken 实时认证、被动成交、市场冲击和持续负载。L3 更新时先复制候选状态，通过校验后再提交；状态复制、全状态哈希及 Python CRC 的耗时需在持续负载下测量。
