# Binance BTC 现货采集器

采集 BTCUSDT **100ms L2 增量、5000档初始快照和原始逐笔成交**，保留收到的原始字节，写入本地持久日志；实时消费、历史追赶和回放共用 Event v1 与同一盘口计算内核。Binance 采集不需要账户或 API 密钥。

v0.4 增加常驻运行、退避重连、健康心跳、磁盘保护、SIGINT/SIGTERM 停机及 SQLite 在线备份。网络恢复后重新获取快照并校验增量桥接；断线造成的盘口缺失仍有显式质量边界。

这是完整系统中的**行情服务**。决策和交易消费者目前是接口示例；模型、撮合、账户和真实下单不在这个仓库的已实现范围内。

## 快速启动

支持 macOS / Linux，Python 3.13 与 uv；构建依赖需要本机 C 编译器。依赖版本由 `uv.lock` 固定，首次安装需要联网。

```bash
uv sync --frozen --extra dev
uv run market-service run --config config/collector.toml
```

默认配置将日志写入 `data/btc.sqlite`，每5秒更新 `data/health.json`，并通过 `/tmp/binance-btc-market.sock` 发布已提交事件。路径相对于 **TOML 文件所在目录**解析。采集持续到 Ctrl+C / SIGTERM；`--seconds 60` 可用于短录制。启动前阅读 [配置](config/collector.toml)，尤其是磁盘预留量。

另一个终端检查状态：

```bash
uv run market-service health --config config/collector.toml
```

只有连接、盘口和成交质量均 LIVE、输入足够新且无待处理原始帧时，退出码才为0。尚未连通或正在同步时为 DEGRADED，心跳超时变为 STALE；`ready` 只描述当前采集状态，**不代表过去的观察区间没有缺口**。

## 两个服务共用的消费路径

```bash
uv run market-service consume --socket /tmp/binance-btc-market.sock --state data/decision.sqlite
uv run market-service consume --socket /tmp/binance-btc-market.sock --state data/trading.sqlite
```

两个进程各自保存 cursor 与盘口状态，重启后从自己最后的提交位置恢复。Publisher 从持久日志追赶和追尾，不通过另一条易丢的内存队列分发。采集端进程退出会断开 socket，消费者进程也会退出；服务恢复后重新执行消费命令，或由进程管理器重启消费者。

直接核验 / 回放：

```bash
uv run market-service verify --db data/btc.sqlite --report data/verification.json
uv run market-service replay --db data/btc.sqlite --report data/replay.json
```

`verify` 从头核验原始记录 checksum、事件链与每批盘口哈希，适合对已停止的日志或固定备份执行。对持续写入的库核验可能遇到不同读取时点，先创建备份能得到固定边界。

也可以在独立 socket 发布历史，消费者接口不变：

```bash
uv run market-service serve --db data/btc.sqlite --socket /tmp/btc-history.sock
uv run market-service consume --socket /tmp/btc-history.sock --state data/history-consumer.sqlite --no-follow
```

历史以 `market_seq` 重放，保留接收时钟、交易所时间与当时质量，**不按时间戳重新排序事实**。相同日志产生相同事件及盘口哈希；跨模式共用的是事件和计算路径，历史数据不能据此获得真实下单权限。

## 网络、恢复和存储

|情况|采集器的行为|
|---|---|
|启动时 REST 不通 / WS 断线|自动退避并重试；退避上限60秒，带抖动；错误历史最多64条|
|HTTP 418 / 429|遵守 Retry-After；未提供有效值时等待120秒，等待可被停机打断|
|盘口序列缺段 / 错误簿 / 过期|先记录 INVALID，再重连、获取新快照和验证增量桥接|
|消费积压|按原始接收时间衡量新鲜度；过期数据不能因刚处理完而伪装成新行情|
|Ctrl+C / SIGTERM|停止接收，处理已持久化尾部，记录 DISCONNECTED，再释放单写者锁|
|SIGKILL / 崩溃|重启恢复已提交日志与未处理原始帧，然后开启新 epoch；旧簿不跨断线继续使用|
|磁盘低于预留量|停止采集，保留文件，写 DISK_LOW，退出码78；不自动删除研究数据|

SQLite WAL / FULL 提供事务与恢复：原始输入先落盘，一个输入的规范事件、去重索引、processed 水位和检查点原子提交。写入失败后不能继续使用已推进的内存态。健康检查按间隔采样，磁盘预留不是绝对不写满保证，需要留足余量。

在线备份使用 SQLite 原生 backup API，包含 WAL 中已提交内容，不直接复制正在写入的 `.sqlite` 文件：

```bash
uv run market-service backup --db data/btc.sqlite --output data/backups/btc-001.sqlite
uv run market-service verify --db data/backups/btc-001.sqlite
```

目标必须不存在；备份不会覆盖旧文件。返回 dataset、序列边界、待处理帧数和文件 SHA256。恢复到备份意味着从该备份边界继续，之后的观察历史需要另行保留；复制备份后用 `run` 指向它即可恢复原始尾部并建立新连接。

当前存储仍是**单个追加 SQLite 日志**。自动跨日分区、压缩冷归档、保留期删除尚未实现，在线备份也不会缩小源库。先按健康信息中的磁盘余量和实际增长率管理容量。长期吞吐、整年容量和物理掉电尚未验收。

## 缺失成交与回补

回补是独立任务，不阻塞实时采集，不改写原观察日志：

```bash
uv run market-service repair --db data/btc.sqlite --catalog data/repairs.sqlite --follow
```

回补会发现 trade ID 缺口、去重、分页与断点恢复。REST 源不可用时缺口保持未解决；官方日度 trades 文件可作为显式离线导入来源。已验证过112个真实缺失 ID 的日度文件回补。

当时可见的 observed 与事后修复的 repaired 视图分开。修复成交不能恢复缺失的盘口路径或订单排队信息，也不会进入实时 Publisher 自动触发真实订单。连续区间和查询口径见 [回补说明](docs/trade-repair.md)。

## 模块和开源复用

```text
config.py + runtime.py       配置、进程生命周期、信号、健康、磁盘保护
            │
network.py                  aiohttp REST / WS、缓冲、退避和重连
            ↓ RawFrame
service.py                  规范化与提交编排；失败后禁止继续
            ↓
binance.py                  Cryptofeed Binance 子类：快照、增量、ID桥接
            ↓ frozen Event v1
store.py                    SQLite 原始记录、事件链、去重索引、检查点
            ├── ipc.py      同一日志的 catch-up + follow
            ├── source.py   历史读取与消费 cursor
            └── backup.py   SQLite 原生一致性备份
                     ↓
contracts.py + book.py       相同契约 + orderbook C 扩展盘口计算
```

|复用组件|职责|本项目维护的边界|
|---|---|---|
|Cryptofeed 固定源码|Binance 快照 / 深度更新 / 序列桥接|严格质量门禁、原始 trade 与适配|
|orderbook C 扩展|有序盘口|整数单位、零量删除、双边批次一致性|
|aiohttp / asyncio|REST、WebSocket、并发与取消|重连政策、持久化顺序与进程管理|
|msgspec|冻结事件、MessagePack、类型和范围校验|版本化事件字段与语义|
|SQLite / sqlite3 / fcntl|事务、WAL、恢复、备份与进程锁|日志表、事件链和单写者约束|

没有改写 vendor 源码；固定来源、许可证和82份上游源文件校验见 [THIRD_PARTY.md](THIRD_PARTY.md)。仓库采用 AGPL-3.0-or-later。附带源代码依赖用于可复现构建，不要只分发项目 wheel 而遗漏依赖来源。

## 检查与部署

```bash
make check test
```

自动化测试使用本地 HTTP / WebSocket，不依赖币安可达；包括缺段重同步、限流、静默断线、关闭握手超时、落盘后取消、进程级 SIGTERM / SIGKILL、断点恢复、双消费者和在线备份。Unix socket / TCP 测试需要运行环境允许本机套接字。

真实联网验收需使用新的目录，避免覆盖原数据：

```bash
uv run python scripts/acceptance.py --db data/acceptance-new/live.sqlite --report data/acceptance-new/report.json --seconds 30
```

它比较采集端、两消费者及历史回放的逐事件与盘口哈希，主动断开 WS，并重启一个消费者。网络不通会明确失败。[本次验收摘要](docs/collector-acceptance.md) 记录实际结果，不作性能承诺。

- [Linux systemd 与 macOS 运行说明](docs/deployment.md)
- [事件接口](docs/event-contract.md) / [原闭环验收方法](docs/acceptance.md)
- [成交回补](docs/trade-repair.md) / [可选 L3 研究模块](docs/l3-market-data.md)

原始行情、数据库、凭据和本机验收产物由 `.gitignore` 排除；GitHub 包含代码、文档和可复现测试。历史研究文档中的 `evidence/` 指本地成果目录，不随仓库发布。
