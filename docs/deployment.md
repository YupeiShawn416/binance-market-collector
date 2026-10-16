# 运行与维护

本次交付提供可运行的常驻进程和部署模板；没有在当前 Mac 安装后台服务，也没有声称完成连续数周运行验证。先短录制、确认网络和容量，再启动常驻采集。

## 本机 macOS / Linux

安装 Python 3.13、uv 和 C 编译工具。项目根目录执行：

```bash
uv sync --frozen --extra dev
make check test
uv run market-service run --config config/collector.toml --seconds 60
uv run market-service verify --db data/btc.sqlite
```

`run` 是进程入口，省略 `--seconds` 就持续运行。终端关闭、电脑休眠和断电都会影响采集，长期开机与网络稳定性应由部署设备保证。Ctrl+C 或 SIGTERM 会处理已落盘尾部后退出；磁盘保护不会清理历史数据。

默认公开行情端点是 Binance 的 market-data-only REST / WS。没有订单接口，也不需要交易所凭据。`trust_env=True` 遵循本机标准代理环境变量；仅在用户自己已有代理时配置，勿把带口令的代理地址写进仓库或报告。

## Linux systemd

示例假定仓库位于 `/opt/binance-market-collector`，系统配置位于 `/etc/binance-market-collector`。这些是部署目标路径，不是本次已经改动过的系统目录。

1. 创建无登录 shell 的 `marketdata` 用户和同名组，将仓库检出到 `/opt/binance-market-collector`。让该用户能读取代码并访问 `.venv`；代码无需运行时写权限。
2. 在项目目录安装编译工具和依赖，执行 `uv sync --frozen --no-dev`。uv 的 Python 解释器必须放在服务可读的位置，例如配置 `UV_PYTHON_INSTALL_DIR=/opt/uv-python`；不要让 `.venv` 指向被 `ProtectHome=true` 隐藏的个人主目录。
3. 安装 `config/collector-linux.toml` 到 `/etc/binance-market-collector/collector.toml`，按实际磁盘容量调整预留量。
4. 安装 `deploy/binance-market-collector.service` 到 `/etc/systemd/system/`。systemd 会创建 `/var/lib/binance-market-collector` 和 `/run/binance-market-collector`，并分配所有权。
5. 核对配置后启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now binance-market-collector
sudo systemctl status binance-market-collector
sudo journalctl -u binance-market-collector -f
```

监控应定期执行同一健康命令：

```bash
sudo -u marketdata /opt/binance-market-collector/.venv/bin/market-service health --config /etc/binance-market-collector/collector.toml
```

退出码0表示当前采集已就绪，1表示未就绪 / 心跳失效。这个命令不会重连交易所。日志和健康文件中的 `market_seq` / `raw_seq`、输入年龄、重连次数、最近64条网络错误、磁盘余量用于排障；`ready` 不检查任意历史窗口是否连续。

普通网络失败由进程内退避处理，不会不断重启整个进程。未处理异常由 systemd 重启，5分钟内最多5次；磁盘不足以78退出，由 `RestartPreventExitStatus=78` 阻止自动重启。释放空间或更换磁盘后，人工执行 `systemctl restart binance-market-collector`。

连接消费者需要能访问该 socket 所在目录，可通过专用组授权。示例不对外开放 TCP 端口。若把数据盘挂到其他路径，需要同时修改 TOML 和 unit 的可写路径。

模板未在当前 macOS 上实际启动 systemd；Linux 持续运行验收仍应在目标设备完成。仓库 CI 会在 Linux 上验证依赖构建和故障测试。

## 备份与恢复

```bash
uv run market-service backup --db data/btc.sqlite --output data/backups/btc-001.sqlite
uv run market-service verify --db data/backups/btc-001.sqlite
```

备份是 SQLite 的一致性快照。它可能保留“原始帧已落盘、尚未规范化”的尾部，返回结果会报告 `pending_raw`；恢复时仍由原来的 MarketService 处理。输出路径必须不存在，且备份过程不会替换原日志。

原始库与备份需要同时占用磁盘；留足备份空间。复制活动库的单个 `.sqlite` 文件可能漏掉 WAL 中的提交，不采用该方式。若需要压缩，可对已经关闭的备份使用操作系统标准压缩工具；回放前先解压为 SQLite 文件。

恢复流程：停止服务；保留原库及其 sidecar 文件；将 TOML 指向验证后的备份路径，再启动。恢复新库开启新连接和 epoch，备份之后的观察历史不会凭空出现。回补目录和消费状态也各有独立文件，不能用旧消费 cursor 指向更早的行情备份，否则越界会明确失败。

## 当前容量边界

采集器还不自动分区、冷归档或按保留期删除。备份不会回收源库空间。健康文件的 `journal_bytes` 包含主库和当前 WAL，在连接关闭和 checkpoint 后可能变化，不能只用短录制体积线性推断整年需求。

按实际运行的日增长量、剩余空间和备份预算扩容。磁盘检查每个心跳周期执行，极端写入或其他进程占满磁盘仍可能触发写入失败；不要把2GiB默认预留理解为绝对安全容量。

## 代码边界

`config.py` 只读和校验 TOML；`runtime.py` 管理进程与健康；`network.py` 只负责公开网络连接与数据流；规范化、存储、回放不依赖 systemd。没有因部署功能把模型、账户或订单代码塞进采集器。
