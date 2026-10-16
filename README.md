# 共享行情服务：Binance L2 与逐笔成交、L3研究接入

**v0.3：新增L3契约、Kraken认证采集入口和Coinbase真实历史L3导入。** 决策/交易参考消费者、SQLite日志和Unix socket基础设施共用；原Binance Event v1与原数据保持兼容。实际已取得免账户的Tardis Coinbase月首L3样本，并完成逐事件L3、逐事件L2、100ms L2三组主动单扫档对照。Kraken和Coinbase今日实时L3仍未获取：两者需要账户认证，本机直连也超时。详见[L3接入与实验](docs/l3-market-data.md)。

**2026-10-16 更新：官方日度原始成交归档已取得，真实112个缺失ID已补齐。** 原始Event v1、盘口计算和观察日志不改写。3735笔原观察成交与日度trades逐笔一致；32/32/32/16分页、真实数据SIGKILL恢复和112笔重复导入通过。回补目录现在为REPAIRED，原observed和当时可见视图仍保留缺口。详见[回补说明](docs/trade-repair.md)和[本次证据](evidence/archive-2026-10-16/acceptance.json)。57项回归通过。

这是已经能运行的行情闭环：BTC/USDT 实时 L2 和原始逐笔成交 → 原始记录 → 规范事件 → 本地持久日志 → 两个独立消费者 → 历史重放与哈希验证。它对应作者架构中的**行情服务**。这里的两个消费者是决策、交易服务的行情入口示例，还没有实现模型、风控、交易账户或下单。

Binance实测结果见 [验收记录](evidence/live-verification.json) 和 [验收说明](docs/acceptance.md)。Binance采集不需要凭据；可选Kraken L3使用本机凭据获取行情认证token。服务没有查询资金、持仓或提交订单的功能。

## 启动

需要 Python 3.13、uv，以及能编译 C/Cython 扩展的本机工具链。macOS 与 Linux 使用本机 Unix socket。解压后在本目录执行：

```bash
uv sync --frozen --extra dev
uv run market-service --help
uv run pytest
uv run ruff check src tests scripts
uv run ruff format --check src tests scripts
```

需要Kraken认证组件时执行`uv sync --frozen --extra dev --extra kraken`。Coinbase历史导入、L3回放和对照实验不需要交易所账户或这个额外依赖。

`uv.lock` 固定了依赖。审查的 Cryptofeed 3.0.2 尚未发布到 PyPI，因此附带固定 commit 的精简**原样源码快照**，供 uv 从 `vendor/` 构建；不依赖先前工作区、系统里已有的包或特定机器的二进制扩展。来源、许可及文件校验在 [THIRD_PARTY.md](THIRD_PARTY.md)。部署时不要只复制本项目 wheel 而遗漏这两个源码依赖。

### 录制并核验

```bash
uv run market-service collect --db data/btc.sqlite --seconds 60 --socket /tmp/btc-market.sock --report data/capture.json
```

该命令录制真实公开行情，并从头重放已提交日志，核验事件链与每批盘口哈希。再次使用同一个数据库，会先恢复未处理的原始尾部，保留 dataset 和 market_seq，再建立新连接/epoch、获取新快照；不会把旧盘口当成断线期间仍然连续。

采集期间在另外两个终端运行：

```bash
uv run market-service consume --socket /tmp/btc-market.sock --state data/decision.sqlite
uv run market-service consume --socket /tmp/btc-market.sock --state data/trading.sqlite
```

这是两个独立 cursor 和盘口检查点。消费端退出后重新执行同一命令，即从自己最后一次提交的位置恢复。每个检查点只允许一个写者。业务系统接入时，应把业务状态与输入 cursor 放进自己的同一事务，不能先确认行情，再异步补记订单。

### 读取随项目附带的真实样本

```bash
uv run market-service verify --db evidence/live.sqlite --reference evidence/live-verification.json
uv run market-service replay --db evidence/live.sqlite --report /tmp/btc-replay.json
```

也可以先 `serve` 历史日志，再用**相同 socket 协议和消费者**读取：

```bash
uv run market-service serve --db evidence/live.sqlite --socket /tmp/btc-history.sock
uv run market-service consume --socket /tmp/btc-history.sock --state /tmp/btc-history-consumer.sqlite --no-follow
```

`serve` 不会把已结束的历史样本伪装成健康的实时连接。样本最后有显式 DISCONNECTED，消费者必须结合质量、事件时效和自己的实际到达时间判断是否可用于交易。

### 重新运行真实双消费者验收

使用新的路径，避免覆盖已有样本和消费状态：

```bash
uv run python scripts/acceptance.py --db /tmp/new-md-accept/live.sqlite --report /tmp/new-md-accept/report.json --seconds 30
```

这个脚本同时启动两个 socket 消费者；一个在第25条事件后关闭、从磁盘检查点重启；中途主动断开公开 WS；随后比较采集端、两个消费者、完整历史重放的全部事件及盘口哈希。网络条件不好会明确失败，不用合成数据替代真实验收。

## 模块边界

```text
network.py       aiohttp 公共WS/REST、整数接收时间、有界队列、重连
    ↓ RawFrame
service.py       编排：规范化 → 校验 → 原子提交；失败后禁止使用半提交内存态
    ↓
binance.py       Cryptofeed Binance 子类：复用真实快照/增量/序列代码
    ↓ frozen Event v1
store.py         SQLite WAL/FULL 原始日志、事件链、去重索引、检查点
    ├─ source.py / replay     历史读取
    └─ ipc.py / Publisher    同一已提交日志的catch-up + follow
                  ↓
            Consumer + book.py
            两个独立只读市场视图
```

- 事件契约独立于 Binance、数据库和网络，见 [contracts.py](src/market_service/contracts.py) 与 [接口文档](docs/event-contract.md)。`msgspec` 提供冻结消息、类型/范围校验、MessagePack 与 JSON Schema。
- `StrictBinance` 继承实际的 Cryptofeed Binance 类，复用 `_parse_snapshot`、`_book`、`_check_update_id`；只覆盖缺段上报、提前质量门禁和原始逐笔。没有复制整个 FeedHandler，也没改 vendor 源码。
- 原始输入与规范事件分开。已接收原始帧先落盘；一个输入的所有规范事件、processed 水位和可选检查点在同一 SQLite 事务中提交，socket 只能读取已提交事件。
- 消费端盘口复用 `orderbook` C 扩展。统一整数表示，增量数量为绝对量，0显式删除；一个双边批次应用完整后才检查和发布。采集端还将其结果与 Cryptofeed 原生 Decimal 簿逐批比较。
- Publisher 不维护另一份易丢的实时发布队列；历史追赶和实时追尾都从日志读。客户端独立进度，最多32连接，慢读有超时。消费 cursor 与完整盘口状态一起提交。

## 本地存储的取舍

首版采用 SQLite 作为**追加事件日志**，一个盘口批次或一条成交对应一个 MessagePack BLOB；没有把5000个价位逐行更新到关系表。WAL/FULL、事务、约束、锁与崩溃恢复由 SQLite 提供，避免先自研文件提交协议。

最初方案中的 Arrow/Parquet 分段归档保留为后续可替换导出层，本版本没有宣称已实现。跨日分区、压缩冷归档和保留策略尚未加入；长周期容量、性能与磁盘行为需专门验收。完整盘口哈希是当前闭环的校验成本，不是低延迟交易SLO。

## 自动成交回补（独立进程）

采集运行时，可在另一终端启动自动发现、回补和断点恢复任务。仅请求公开历史成交，客户端无账户与下单方法：

```bash
uv run market-service repair --db data/btc.sqlite --catalog data/repairs.sqlite --follow
```

对原样本执行一次任务、查询两种口径的连续性：

```bash
uv run market-service repair --db evidence/live.sqlite --catalog evidence/repair/repairs.sqlite --page-size 32 --report /tmp/repair-attempt.json
uv run market-service coverage --db evidence/live.sqlite --catalog evidence/repair/repairs.sqlite --view observed --first-id 6763420844 --last-id 6763424690
uv run market-service coverage --db evidence/live.sqlite --catalog evidence/repair/repairs.sqlite --view repaired --first-id 6763420844 --last-id 6763424690
```

`coverage`是只读查询，可与回补写者并行运行；它不会启动网络请求。连续性只判断显式ID范围，不是交易权限。`repair --retry-now`可跳过普通错误退避，仍不跳过已经持久化的418/429 Retry-After。

原REST网络验收与同时间段历史下载：

```bash
uv run python scripts/repair_acceptance.py --db evidence/live.sqlite --output evidence/repair
```

原始REST成交不可达时，该脚本保存失败和历史聚合对比后以非零码结束。10月15日失败记录保留于[evidence/repair/acceptance.json](evidence/repair/acceptance.json)。10月16日已通过官方日度ZIP补齐，新增`archive.py`读取及校验原始CSV，`scripts/archive_acceptance.py`复用同一回补事务和恢复逻辑。归档导入目前是显式离线任务，常驻`repair --follow`仍只自动请求REST，不会自行下载每日ZIP。

`verify` 校验原始字节 checksum、全部规范事件 hash chain 以及消费者重建的盘口 hash；hash 用于检测不一致，不提供抵御可同时篡改数据库和外部证据的数字签名保证。

## 质量与恢复语义

- Snapshot 只进入 SYNCING；第一批满足 update-ID 桥接的 delta 后 Book 才 LIVE。连接、盘口、成交三个质量域均为 LIVE，参考视图才 `ready`。
- 深度缺段、crossed/locked、格式或精度错误先记录 INVALID，丢弃这一帧的无效规范输出，再重新连接/获取快照；不会把错误簿先发给策略。
- 重连显式增加 book_epoch。raw trade 的 ID 去重与持久索引一起恢复；相同ID不同事实不能被静默吞掉。
- 断线期间可能缺失公开成交；原观察日志记录明确的 trade ID gap，并开启新的观察片段。独立回补任务在原始历史源可用时补入修复目录，不改写原日志。**本次112个ID已由官方trades日度文件补齐，maker队列与L2路径仍不因成交修复自动连续。** 下游必须按缺口和所需历史窗口重建相关状态。
- Writer 在原始落盘后、规范提交前、提交后遭 SIGKILL 的三个恢复位置已测试。已提交事件不会重复追加；未处理原始尾部会恢复规范化。新实时连接仍需独立快照桥接。
- 历史 replay 按 market_seq，不按交易所时间或接收时间排序。重放与实时共享 Event、记录校验和 Projection；没有另一份“近似盘口”回测代码。

## 范围

本版固定 Binance spot BTC-USDT、深度diff及原始trade；初始快照最多5000档/侧，之后保留已观测增量。有限覆盖以已知边界记录，不声称知道边界之外的全部深度。L2是批量更新，不是完整MBO；不具备精确订单队列信息。

工程闭环通过不等于整个交易底座已可实盘。尚未实现交易内核、模拟撮合、模型、账户与真实订单；本版也没有完成长期运行、容量压测或物理掉电测试。真实30秒样本中的耗时是一次观察，不能当成吞吐能力或尾延迟承诺。
