# 部署与维护

采集器需要持续联网和写盘。部署时由进程管理器维持运行，通过健康文件监控当前状态，通过数据库备份保留可恢复的数据副本。本页分别说明本机试运行和 Linux 常驻部署。

运行要求：Python 3.13、uv 包管理工具、C 编译器，以及支持长期开机的 macOS / Linux 设备。代码和依赖在项目目录安装，数据路径由 TOML 配置文件指定。

## 本机试运行

在项目目录安装依赖、执行检查，再录制60秒行情：

```bash
make install
make check test
uv run market-service run --config config/collector.toml --seconds 60
uv run market-service verify --db data/btc.sqlite
```

`run` 启动接收、落盘、解析和 socket 发布；`verify` 在录制结束后核对原始消息、事件链和重建盘口。省略 `--seconds` 后持续采集，Ctrl+C / SIGTERM 会处理已落盘的待处理消息再退出。设备休眠、终端关闭和断电会中断采集。

默认连接 Binance market-data-only 公开行情接口。REST 负责快照请求，WebSocket（WS）负责持续推送。HTTP 客户端通过 `trust_env=True` 读取标准代理环境变量，含口令的配置保存在本机。

## Linux 常驻部署

systemd 负责开机启动、进程异常重启、服务目录和权限。仓库提供的 unit 文件与 Linux 配置使用以下路径：

|用途|路径|
|---|---|
|代码与虚拟环境|`/opt/binance-market-collector`|
|采集配置|`/etc/binance-market-collector/collector.toml`|
|行情数据库与健康文件|`/var/lib/binance-market-collector`|
|本机订阅 socket|`/run/binance-market-collector/feed.sock`|

1. 将代码放入 `/opt/binance-market-collector`，创建用于运行服务的 `marketdata` 系统用户和组，授予代码读取权限。
2. 在代码目录执行 `uv sync --frozen --no-dev` 安装依赖。可设置 `UV_PYTHON_INSTALL_DIR=/opt/uv-python` 安装解释器，再授予该用户解释器与 `.venv` 的读取、执行权限。unit 中的 `ProtectHome=true` 将用户主目录隔离，因此解释器路径选在 `/opt`。
3. 将 `config/collector-linux.toml` 安装为 `/etc/binance-market-collector/collector.toml`，按磁盘容量调整预留量。
4. 将 `deploy/binance-market-collector.service` 安装到 `/etc/systemd/system/`。unit 中的 StateDirectory 和 RuntimeDirectory 由 systemd 创建并分配所有权。
5. 重新加载 unit 并启动服务：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now binance-market-collector
sudo systemctl status binance-market-collector
sudo journalctl -u binance-market-collector -f
```

更换数据盘路径时，同时修改 TOML 和 unit 的可写路径。消费者连接 socket 时需取得目录访问权限；socket 文件权限为0600，使用 `marketdata` 用户运行本机消费者可沿用该权限。

## 健康监控与故障处理

采集器每个心跳周期写入健康 JSON，`health` 命令读取该文件并检查更新时间：

```bash
sudo -u marketdata /opt/binance-market-collector/.venv/bin/market-service health --config /etc/binance-market-collector/collector.toml
```

退出码0表示当前采集就绪，1表示同步、降级、心跳过期或健康检查异常。主要字段如下：

|字段或指标|检查目的|
|---|---|
|`market_seq` / `raw_seq`|统一事件和原始接收记录各自处理到的位置|
|输入年龄与质量状态|当前消息的新鲜程度，以及连接、盘口、成交的可用状态|
|`pending_raw`|已经落盘、等待解析的原始消息数量|
|重连次数与最近64条错误|判断网络与解析故障|
|磁盘余量|判断剩余写入空间|

健康检查表达当前运行状态。历史成交覆盖通过 `coverage` 按 ID 区间查询，见[成交回补](trade-repair.md)。

网络故障由采集进程按递增间隔重试。进程异常退出后，systemd 以30秒间隔重启，5分钟内最多5次。磁盘低于预留量时进程以78退出，unit 中的 `RestartPreventExitStatus=78` 将重启交给人工处理；扩容或释放空间后执行：

```bash
sudo systemctl restart binance-market-collector
```

## 数据库备份与恢复

SQLite 使用 WAL 写前日志：新提交可先位于 `-wal` 文件，再由 checkpoint 操作合并回主库。`-shm` 是协作访问 WAL 的辅助文件。主库与这些 sidecar 文件构成运行中的数据库文件组。

备份命令通过 SQLite backup API 读取已提交状态，生成单个一致性快照，可在采集期间执行：

```bash
uv run market-service backup --db data/btc.sqlite --output data/backups/btc-001.sqlite
uv run market-service verify --db data/backups/btc-001.sqlite
```

目标使用新文件，并预留一份数据库所需空间。备份中的 `pending_raw` 表示待解析尾部，恢复时由 MarketService 编排处理。关闭后的备份可压缩，回放前解压。

恢复步骤如下：

1. 停止采集服务，保留原主库及 sidecar 文件组。
2. 核验备份，将 TOML 的数据库路径指向该备份。
3. 启动服务；程序恢复检查点、处理待解析消息，再建立新连接与盘口重建代次 `book_epoch`。
4. 分别检查回补库和消费者检查点。消费者游标表示已处理的事件序号，应处于恢复后日志的序号范围内；超前的检查点需保留归档，并使用新 state 文件从恢复日志重建。

## 容量与部署验收

当前存储采用单个追加 SQLite 日志。容量按实际日增长、备份空间和预留量规划；分区、冷归档和保留期清理列入后续实现。

健康报告中的 `journal_bytes` 合计主库和 WAL 大小，随 WAL 合并变化。磁盘每个心跳周期检查一次，预留量需覆盖周期内的写入突发和其他进程占用。

Linux CI 覆盖依赖构建与故障测试。目标设备部署后还需检查 systemd 开机启动、连续运行、持续吞吐和实际日增长，短时样本记录见[采集验收](collector-acceptance.md)。
