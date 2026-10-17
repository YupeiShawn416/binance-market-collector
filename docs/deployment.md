# 部署与维护

要求 Python 3.13、uv、C 编译器，以及支持长期开机、持续联网的 macOS / Linux 设备。

## 本机运行

```bash
make install
make check test
uv run market-service run --config config/collector.toml --seconds 60
uv run market-service verify --db data/btc.sqlite
```

省略 `--seconds` 持续采集。Ctrl+C / SIGTERM 处理已落盘尾部后退出。休眠、终端关闭和断电会中断采集。

默认使用 Binance market-data-only REST / WS。`trust_env=True` 读取标准代理环境变量；含口令的代理配置仅保存在本机。

## Linux systemd

|用途|路径|
|---|---|
|代码与虚拟环境|`/opt/binance-market-collector`|
|配置|`/etc/binance-market-collector/collector.toml`|
|日志与健康文件|`/var/lib/binance-market-collector`|
|socket|`/run/binance-market-collector/feed.sock`|

1. 创建无登录 shell 的 `marketdata` 用户和组，授予代码、解释器和 `.venv` 读取权限。
2. 安装依赖：`uv sync --frozen --no-dev`。可用 `UV_PYTHON_INSTALL_DIR=/opt/uv-python` 安装解释器，确保 `ProtectHome=true` 下可访问。
3. 将 `config/collector-linux.toml` 安装到配置路径，按磁盘容量设置预留量。
4. 将 `deploy/binance-market-collector.service` 安装到 `/etc/systemd/system/`。systemd 负责创建数据目录、运行目录并分配所有权。
5. 启动服务：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now binance-market-collector
sudo systemctl status binance-market-collector
sudo journalctl -u binance-market-collector -f
```

更换数据盘路径时，同步修改 TOML 与 unit 的可写路径。socket 消费者需有目录和文件访问权限。

## 监控与重启

```bash
sudo -u marketdata /opt/binance-market-collector/.venv/bin/market-service health --config /etc/binance-market-collector/collector.toml
```

退出码0：当前采集就绪；1：未就绪或心跳失效。检查 `market_seq`、`raw_seq`、输入年龄、重连次数、最近64条错误和磁盘余量；历史连续性使用 `coverage` 查询。

网络故障由进程内退避重试。未处理异常由 systemd 重启，5分钟内最多5次。磁盘不足以78退出，`RestartPreventExitStatus=78` 停止自动重启；扩容或释放空间后执行：

```bash
sudo systemctl restart binance-market-collector
```

## 备份与恢复

```bash
uv run market-service backup --db data/btc.sqlite --output data/backups/btc-001.sqlite
uv run market-service verify --db data/backups/btc-001.sqlite
```

SQLite backup API 将主库和 WAL 提交合成一致性快照，目标使用新文件。`pending_raw` 表示待规范化尾部，恢复时由 MarketService 继续处理。备份须预留额外磁盘空间；关闭的备份可压缩，回放前解压。

恢复：停止服务，保留原库及 sidecar，将 TOML 指向已验证的备份后重启。新连接从备份边界开启新 epoch；回补库和消费检查点分别管理，消费 cursor 须处于恢复后日志范围内。

## 容量与验证

当前采用单个追加 SQLite 日志。按实际日增长、备份空间和余量扩容；分区、冷归档和保留期清理待实现。

`journal_bytes` 包含主库及 WAL，随 checkpoint 变化。磁盘每个心跳周期检查一次，应为写入突发和其他进程留出余量。

Linux CI 覆盖依赖构建与故障测试；目标设备仍需完成 systemd 安装、持续运行和容量验收。
