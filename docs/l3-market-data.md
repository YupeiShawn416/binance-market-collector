# L3接入、历史对比与L2研究边界

核查日期：2026-10-16；项目版本0.3.0。已经完成真实历史L3的获取、逐单保存、快照桥接、共用日志/回放/消费者和三组主动单实验。**没有取得今日实时L3，也没有实现被动挂单成交模型或真实订单执行。** 用户没有Kraken或Coinbase账户；不创建账户、不使用他人凭据。

## 实际获取结果与选择

| 路径 | 本次实际结果 | 能确认什么 |
|---|---|---|
| Kraken `wss://ws-l3.kraken.com/v2` | 无凭据探测超时；未收到消息 | 官方接口要求token；本次不能确认认证后能连通 |
| Coinbase Exchange `full` / `level3` | WS探测超时；REST L3快照亦TLS超时 | 官方认证页明确两个通道都需要账户认证；不能将超时说成服务器拒绝认证 |
| Tardis公开月首Coinbase历史full | HTTP200，下载2024-01-01 00:00这一分钟，解压后12,899,173字节 | 无交易所账户、无API key，实际取得逐单事件及REST L3快照 |

选用**Coinbase真实历史L3作为本轮实际验收源**，同时保留Kraken实时适配入口，等待可用账户和网络。历史数据取得成功不证明实时路径可用。逐次请求与HTTP响应头在`evidence/l3-access-2026-10-16/`。

官方依据：[Kraken L3](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/level3)、[Coinbase认证](https://docs.cdp.coinbase.com/exchange/websocket-feed/authentication)、[Tardis Coinbase原始通道](https://docs.tardis.dev/historical-data-details/coinbase)、[Tardis HTTP月首免费范围](https://docs.tardis.dev/api/http-api-reference)。下载使用`from=2024-01-01,offset=0`，BTC-USD，通道full_snapshot/received/open/done/change/match。下一分钟仅请求full_snapshot得到空响应，没有将其当作第二份独立快照。

## 复用与模块分工

```text
Kraken SDK（仅签名、token） ── aiohttp原始WS字节 ── l3/kraken.py
Tardis原始历史NDJSON ── l3/coinbase_history.py（source sequence桥接）
                                     ↓
                   EventV2：OrderSnapshot / OrderBatch
                                     ↓
      既有SQLite Journal事务、原始字节、事件链、processed水位
                  ├─ L3Consumer / replay：同一L3Projection
                  └─ 既有Unix socket Publisher → 两个独立L3Consumer
                                     ↓
      l3/experiment.py：逐事件L3 / 逐事件L2 / 因果100ms L2 → 同一扫档函数
```

Python3.13；SQLite WAL/FULL、aiohttp、msgspec和orderbook C扩展沿用既有依赖；可选`python-kraken-sdk==3.5.1`只复用认证请求。未改vendor。现有Cryptofeed Coinbase适配器指向Advanced Trade，Kraken适配器只提供L2，均不能直接充当这次L3采集器。SDK现有WS回调先做普通JSON浮点解码且不提供原始字节，因此没有用该回调承接原始日志。

| 文件 | 职责 |
|---|---|
| `l3/contracts.py` | v2冻结事件，逐单ID、价格/数量整数和原始十进制文本、订单时间、原子批次、来源sequence/CRC及覆盖边界 |
| `l3/book.py` | 逐单状态、C价格有序簿、Kraken队列CRC、批次验证、深度范围退出、检查点 |
| `l3/service.py` | 复用Journal/Consumer，候选状态验证后才提交；失败批次不泄露半更新盘口 |
| `l3/kraken.py` | Decimal解析、纳秒ISO时间、逐单add/modify/delete；删除原因未知时不擅自分类成成交 |
| `l3/network.py` | 原始市场消息落盘、有限队列、重连与新快照；认证请求不落盘，无下单方法 |
| `l3/coinbase_history.py` | 一段连续历史导入、完整序号校验、迟到REST快照桥接、固定已知价格范围 |
| `l3/experiment.py` | 相同到达时间/方向/数量的独立主动单探针；导出全部结果及100ms盘口 |
| `scripts/check_coinbase_full.py` | 独立全深度Decimal重建；不调用生产normalizer、整数订单结构或投影，核对最终逐单事实 |

v1文件不迁移；L3使用v2新日志。Journal/Consumer/Publisher仅增加显式契约类型选择，存储和socket实现复用。v1客户端拒绝v2，不静默丢弃逐单信息。`data_origin`分别记录live/historical/synthetic，进入事件链、检查点和消费者；它不授予任何下单权限。

## 时间、快照与覆盖范围

原文件保留26,711条消息，含1份106,918个订单的REST L3快照、8,867条received、8,380条open、8,860条done、578条match和25条change。除快照外的full源序号严格连续，未出现断线空行标记。

REST快照源sequence=70536614464，提供商收到时，WS已到70536616379。先按源sequence回放1,915条缓存消息，直到收到快照的2024-01-01 00:00:07.1661410 UTC才发布首份可用状态。此前约7秒不进入成交实验；不使用旧REST状态冒充当前簿。合并后的初始快照没有单一可信交易所E时间，因此该字段为空，原REST和WS时间仍留在原始行。

历史事件的`receive_*`和`normalized_*`仍是本次本机导入的实际时间；提供商2024年时间独立保存在`historical_capture_ns`。实验使用这个显式历史可用时间，不伪造本机2024年收包，也不声称等于当时策略收到数据或假想订单到达交易所的时间。

比较固定在初始每侧100个价位界定的已知范围：买盘价格≥42,185.50 USD，卖盘价格≤42,626.29 USD。保留范围内**每一次事件和每个可见订单**，范围随交易过程不移动、不以“最优100档”反复删补。源原始完整快照和范围外full消息仍全部保存。范围耗尽只能标记剩余成交未知。

Coinbase REST L3没有每个存量订单的入队时间；本次`queue_order_known=false`，不把数组顺序当成已证实FIFO。此限制不影响已知各价位数量的主动扫档实验，阻止我们据此声称被动挂单排队成交已经验证。

Kraken路径不同：消息批次完成后截断到订阅档数，价位退出范围不伪造成撤单；每批验证官方CRC，但CRC只覆盖前10个价位。Kraken没有被本适配器假设为具有交易所连续update ID。重连必须新快照，旧簿不能继续LIVE。L3仍不包含隐藏冰山量、在途订单等全部交易所内部信息。

## 本次真实历史实验结果

样本：2024-01-01 00:00:07.1661410至00:00:59.9922905 UTC，约52.8261495秒；原始输入SHA256：

`c1b45646c134596da97bec1bc37e9969dedccf3e777793f66b4baa38e1919327`

每10ms分别提出买/卖0.0001、0.001、0.01 BTC，合计31,698个假想主动单。每笔独立从当时可见簿开始，在同一个扫档函数中逐档扣量，不跨空价补流动性。它们不是连续投资组合订单，也不是31,698个统计独立市场样本。没有提交任何真实订单。

三组输入：逐事件L3逐单量；同频L2按价位合计；每100ms边界采样的L2，边界之间只持有当时已知状态。禁止取未来桶末值填过去。初始采样相位固定在首份可用快照，尚未做其他相位及多时段敏感性实验。

| 指标 | 真实历史样本结果 |
|---|---:|
| 同频L2与L3的扫档结果差异 | 0 |
| 100ms L2与L3均价不同的探针 | 5,561 / 31,698，约17.5437% |
| 平均绝对均价差 | 0.06746369 bps |
| 最大绝对均价差 | 14.15186357 bps |
| 已知可见成交数量不同 | 0 |
| 扫出本次已知范围 | 0 |

最大片段中，一笔0.0001 BTC的假想买单，逐事件簿均价42,291.25 USD，100ms旧簿均价42,351.10 USD，相差59.85 USD/BTC。100ms视图在这个例子里高估成本，其他探针也存在低估成本。1bps=0.01%，这里不是宣称发生了相同比例的真实滑点或亏损。

独立全深度Decimal参考从106,918个原始订单重建到最终106,943个订单，处理快照源sequence后的23,982条消息；筛入同一价格范围后240个订单与生产投影的ID、价格、数量全部相符。该交叉检查验证两个实现对同一来源的解读，不是另一家数据源对提供商数据完整性的认证。

实际输出：`evidence/l3-engine-2026-10-16/historical-import.json`、`historical-comparison.json`、全部探针`historical-probes.csv`、可回查的529份100ms盘口`historical-probes.l2-100ms.msgpack`、原始NDJSON、v2 SQLite、独立参考结果和回放验证。Synthetic目录另存合成流动性真空测试，不混入上述统计。

两个真实数据消费者通过既有socket读取22,073个v2事件，一个在seq25退出并从自己的检查点恢复；与独立历史回放的逐事件事件哈希、订单状态哈希转录及最终链完全一致。证据在`consumers/acceptance.json`。完整离线/本机socket回归73项通过，Ruff通过，82个固定上游源码输入未改动；远端CI未运行。

**这一分钟没有证明黑天鹅、长期误差率、maker排队、市场冲击或真实网络下单延迟的准确性。** 下一步实验应跨多个日期、波动/薄盘口区间、不同采样相位与订单大小，并与真实目标场所的执行回报校准。本轮没有运行交易账户或订单服务。

## 复现

在项目目录运行，不需要账户：

```bash
uv sync --frozen --extra dev
uv run market-service l3-verify --db evidence/l3-engine-2026-10-16/coinbase-history.sqlite
uv run market-service l3-compare \
  --db evidence/l3-engine-2026-10-16/coinbase-history.sqlite \
  --csv data/coinbase-probes.csv --report data/coinbase-comparison.json
uv run market-service replay --schema-version 2 \
  --db evidence/l3-engine-2026-10-16/coinbase-history.sqlite
```

复测导入时必须使用新的`--db`路径，避免覆盖证据：

历史导入是一次性离线构建；中途失败必须重试到新数据集，尚未实现导入任务断点续作。未写入`import_complete=1`的历史日志拒绝只读消费者和实验，避免半导入结果被当作完整数据。此标记只证明本地导入结束，不证明提供商全市场数据完整。

```bash
uv run market-service l3-import-coinbase \
  --source evidence/l3-engine-2026-10-16/coinbase-2024-01-01-0000.ndjson \
  --db data/coinbase-reimport.sqlite --levels 100 --report data/import.json
```

Unix socket：`serve --schema-version 2 --db ... --socket /tmp/l3.sock`；两个终端各自运行`consume --schema-version 2 --socket /tmp/l3.sock --state data/decision-l3.sqlite --no-follow`及独立trading state。当前参考消费者仅维护行情视图，不下单。

Kraken实时入口（尚未真实认证验收）：安装`--extra kraken`后，将账户的行情凭据保存在本机0600 JSON文件（key/secret）或`KRAKEN_MARKET_KEY`/`KRAKEN_MARKET_SECRET`环境变量，使用`l3-collect --credentials-file 本机路径 --db data/kraken.sqlite --depth 100 --seconds 60`。凭据不能发送到聊天、写入命令行参数值或放进交付目录。仅获取WS token，不查询资产或下单。

## L2是否够做研究

足以开始很多研究。L2提供价差、各价位深度、盘口不平衡、可见价格空洞和累计扫档成本；配合逐笔成交，可研究成交压力、流动性状态及秒/分钟级特征。是否足够取决于目标、有效数据频率、缺口处理和结果对误差的敏感性，不能仅由持仓周期判断。

同一时刻、同一可见范围、相同主动订单，在忽略我们对后续市场反应的条件下，L2与L3拥有相同的可扫数量，因此同频结果应相同。**L2描述信息层级，100ms描述时间更新方式，两者是独立维度。** 本次第三组正是分开验证这两项。

L2无法确定同价位每笔订单身份、撤单发生在我们前面还是后面、精确排队优先级。被动挂单策略、单笔订单寿命/撤单研究和极短时成交验收更需要L3及交易所规则、成交事件、延迟信息；L3可见数据也不是交易所全部内部真相。

研究可继续使用Binance L2+逐笔成交，L3作为信息损失和执行模型的验证样本。尚未建立长期实测SLO；L3初版每批候选状态复制、全状态哈希与Python队列CRC有明确成本，不能把这次离线成功当成高频生产吞吐或尾延迟承诺。
