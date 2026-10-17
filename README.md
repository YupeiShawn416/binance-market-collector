# Binance BTC 现货采集器

本项目将币安 BTCUSDT 现货行情保存为可恢复、可校验的本地日志，供实时订阅、历史回放和数据研究使用。它实现量化系统的行情层；决策、风控、订单和账户服务通过行情接口接入。

采集内容包括 **100ms L2 盘口增量、每侧最多5000档初始快照和原始逐笔成交**。L2 将同一价格的挂单量合计为一档；快照提供起始盘口，增量更新发生变化的价位。100ms 是交易所的深度推送周期，逐笔成交使用独立数据流。

技术栈：Python 3.13、SQLite、Cryptofeed、orderbook。运行环境：macOS / Linux。币安公开行情使用免密钥接口。项目许可：AGPL-3.0-or-later。

## 数据流程

```text
交易所 REST 快照 / WebSocket 推送
    ↓ 原始消息先落盘，保留接收时间与原始字节
本地 SQLite 日志（Journal）
    ↓ 解析、序列校验、价格数量转整数
统一行情事件（Event v1）写入同一日志
    ├─ 发布器（Publisher）→ 本机 socket → 实时消费者（Consumer）
    └─ 历史回放（replay）──────────────→ 相同 Consumer
                                             ↓
                               更新本地盘口状态（Projection）
```

Event v1 是统一的数据格式，包含品种信息、盘口快照、盘口更新、逐笔成交和质量状态。实时消费与回放使用相同事件和盘口更新逻辑，通过逐事件哈希与盘口哈希核对计算结果。

消费者用**游标**记录已处理的事件序号，用**检查点**保存该序号对应的盘口和校验状态。两者一起提交后，进程重启即可从已保存的位置续读。事件按本地序号 `market_seq` 回放，保留当时的观察顺序和质量变化。

## 启动采集

安装 Python 包与环境管理工具 uv，并准备 C 编译器以构建盘口扩展。在项目目录执行：

```bash
uv sync --frozen --extra dev
uv run market-service run --config config/collector.toml
```

[配置文件](config/collector.toml)中的路径相对于 TOML 所在目录解析。默认输出如下：

|输出|用途|
|---|---|
|`data/btc.sqlite`|原始消息、统一事件、检查点和校验记录|
|`data/health.json`|当前连接、数据新鲜度和运行状态，每5秒更新|
|`/tmp/binance-btc-market.sock`|本机进程订阅行情的 Unix socket 地址|

采集持续至 Ctrl+C / SIGTERM；加 `--seconds 60` 可录制60秒。另开终端查询健康状态：

```bash
uv run market-service health --config config/collector.toml
```

连接、盘口和成交均处于 LIVE（可用）、输入新鲜且待处理帧为0时，命令返回退出码0。同步或断线显示 DEGRADED（采集降级），心跳超时显示 STALE（健康信息过期）。历史缺口通过下文的 `coverage` 查询。

## 订阅与历史回放

`consume` 是参考消费者，负责校验事件、更新盘口并保存消费进度。下面两个命令分别在独立终端运行，演示决策端和交易端各自持有行情状态：

```bash
uv run market-service consume --socket /tmp/binance-btc-market.sock --state data/decision.sqlite
uv run market-service consume --socket /tmp/binance-btc-market.sock --state data/trading.sqlite
```

这两个 state 文件保存各自的行情检查点。发布器先发送游标之后的已提交事件，再等待新增事件；服务与消费者重启后使用原路径继续运行。

校验和回放先使用数据库备份，以固定输入范围：

```bash
uv run market-service backup --db data/btc.sqlite --output data/backups/btc-001.sqlite
uv run market-service verify --db data/backups/btc-001.sqlite --report data/verification.json
uv run market-service replay --db data/backups/btc-001.sqlite --report data/replay.json
```

`verify` 检查原始消息校验和、事件链与盘口哈希；`replay` 从日志依次读取事件，交给相同的 Consumer 重建盘口。备份目标使用新文件。

历史日志也可通过相同订阅接口发布。先运行 `serve`，再另开终端运行消费者：

```bash
uv run market-service serve --db data/backups/btc-001.sqlite --socket /tmp/btc-history.sock
uv run market-service consume --socket /tmp/btc-history.sock --state data/history-consumer.sqlite --no-follow
```

`--no-follow` 表示追上发布器检查到的已提交位置后退出；上述备份文件固定了这次读取的数据范围。

## 数据质量热力图

热力图将日志中的盘口、成交和质量状态放在同一时间轴上，用于检查可见深度、断线区间和成交缺口。

![BTCUSDT 实际采集数据质量热力图：盘口深度、断线区间、质量状态与观察成交量](docs/assets/market-data-quality-preview.png)

样本时间：2026-10-16 15:44:37–15:45:06 UTC。**1047个事件、759笔观察成交、10个缺失成交 ID**；中央灰带为主动断线区间。

- 上图：各价格的可见挂单量和最优买卖报价；颜色越亮，挂单量越大。
- 中图：连接、盘口、成交质量；× 标记同步、断线和缺段边界。
- 下图：观察到的主动买卖成交量。灰色表示该区间的数据覆盖待确认，有效覆盖内的零量显示为最低色阶。

生成自己的预览：

```bash
uv sync --frozen --extra preview
uv run market-service preview --db data/btc.sqlite --output data/quality-001.html --png data/quality-001.png --report data/quality-001.json
```

默认读取最后20000条事件，核对窗口内的事件链、盘口哈希和引用的原始消息。每格盘口深度按持续时间加权，成交量按格求和。HTML 支持离线交互，PNG 提供静态图，JSON 保存统计和校验结果；输出文件使用新路径。参数与计算口径见[质量预览](docs/quality-preview.md)。

## 断线、落盘与恢复

采集器通过 REST 请求快照，通过 WebSocket（WS）接收持续推送。快照带有更新序号，程序将它与缓存增量衔接，确认后续更新连续后开放盘口使用，这一步称为**快照桥接**。

|情况|处理|
|---|---|
|REST / WS 连接失败|带随机抖动的递增间隔重试，间隔上限60秒；保留最近64条错误|
|HTTP 418 / 429 限流|按响应中的 Retry-After 等待；字段缺失或格式错误时等待120秒|
|盘口缺段、错误或过期|标记 INVALID（失效），重连后重新获取快照并桥接|
|消息积压|按原始接收时间判断新鲜度|
|Ctrl+C / SIGTERM|处理已落盘的待处理消息，记录 DISCONNECTED（连接结束），释放写者锁|
|进程崩溃|恢复检查点并处理日志尾部，建立新连接；用新 `book_epoch` 标记盘口重建代次|
|磁盘低于预留量|保留日志，记录 DISK_LOW，以退出码78停止|

SQLite 使用 WAL 写前日志和 FULL 同步设置保存提交。每条原始消息先持久化；其统一事件、去重索引、已处理位置和检查点随后在同一事务中提交。写入失败后，进程通过重启恢复到持久状态。

备份使用 SQLite backup API，将主库及 WAL 中的已提交内容合成快照。当前存储采用单个追加日志，容量按实际增长、备份需求和磁盘余量管理；自动分区、压缩归档及保留期清理属于后续工作。部署步骤见[部署与维护](docs/deployment.md)。

## 成交缺口与回补

币安原始逐笔成交带有递增 ID，相邻 ID 跳号时形成缺口。回补任务读取原日志，在独立的 `repairs.sqlite` 中记录缺失范围、历史来源、已补成交和分页进度：

```bash
uv run market-service repair --db data/btc.sqlite --catalog data/repairs.sqlite --follow
```

任务持续发现缺口，通过历史接口分页获取并去重；源暂时不可用时保存进度等待重试。官方日度 trades 文件也支持离线导入，已有112个缺失 ID 的真实回补验收。

查询提供两种视图：`observed` 表示当时观察到的成交，`repaired` 包含经过校验的事后回补。原始事件、连接边界和盘口缺口继续保留。连续区间通过 `coverage` 查询，参数和示例见[成交回补](docs/trade-repair.md)。

## 源码与工程验证

源码位于 `src/market_service/`，按职责组织：

|模块|职责|
|---|---|
|`config.py` / `runtime.py`|配置、进程生命周期、健康报告、磁盘保护|
|`network.py`|REST / WS 连接、接收缓冲、重连|
|`service.py` / `binance.py`|原始消息落盘与处理编排、币安解析、序列校验、快照桥接|
|`contracts.py` / `book.py`|统一事件格式、整数价格数量、盘口状态更新|
|`store.py`|SQLite 日志、事件链、去重、检查点|
|`ipc.py` / `source.py`|socket 发布、消费续读、历史回放|
|`backup.py` / `preview*.py`|一致性备份、预览聚合、HTML / PNG 输出|

Cryptofeed 提供交易所盘口处理基础，orderbook 提供 C 扩展有序盘口。依赖版本、复用范围与许可证见 [THIRD_PARTY.md](THIRD_PARTY.md)；`vendor/` 保存82份上游源文件及校验清单。

```bash
make install
make check test
uv run python scripts/acceptance.py --db data/acceptance-new/live.sqlite --report data/acceptance-new/report.json --seconds 30
```

`make install` 安装开发和可选依赖；`make check test` 执行代码检查、上游源码校验和自动化测试。联网验收脚本录制行情、强制重连、恢复消费者游标，比较采集端、两个消费者与历史回放的逐事件及盘口哈希。

阅读顺序：[事件接口](docs/event-contract.md) → [成交回补](docs/trade-repair.md) → [部署](docs/deployment.md)。运行结果见[采集验收](docs/collector-acceptance.md)，逐单数据实验见 [L3研究](docs/l3-market-data.md)。

数据库、凭据和本机验收文件由 `.gitignore` 排除。README 静态图随仓库发布；文档中的 `evidence/` 指本地验收目录，复现相关实验需准备对应样本。
