# 初版行情服务验收

日期：2026-10-15。当前采集验收见 [collector-acceptance.md](collector-acceptance.md)。本地证据目录：`evidence/`。

## Binance BTC-USDT

|项目|结果|
|---|---:|
|采集时长|30.232秒|
|原始记录 / 规范事件|4011 / 4019|
|Snapshot / Delta / Trade|2 / 264 / 3735|
|Quality / Instrument|17 / 1|
|主动断线 / 重连|1 / 1|
|未处理原始尾部|0|

采集原生簿、规范簿、两个 socket 消费者和完整历史回放的逐事件及盘口哈希一致。决策消费者在 seq=25 退出，从磁盘检查点恢复后与持续消费者一致。

缺失成交 ID：`6763422659..6763422770`，共112笔，记录为 Quality 边界。后续日度文件回补见 [trade-repair.md](trade-repair.md)。录制末状态为 DISCONNECTED。

- 末盘口哈希：`bf054cd143c8777877cb5fb107cf0b6be7ce33af97f5e18d28bec20be1cd7801`
- 末事件链：`23f4c52c625d0ad5f6df593a93b7ca6170d986899d70844bc3626eb5ebe1d116`
- 证据：`evidence/live-verification.json`

## 初版回归

29项测试通过，覆盖：

- 原生盘口、规范事件、双消费者、精度、空档和哈希；
- 序列缺段、旧消息、重复 / 冲突成交、crossed / locked 和精度错误；
- 新 epoch、快照桥接、检查点和未处理原始尾部；
- dataset 隔离、单写者锁、严格 schema 和 checksum；
- socket 追尾、消费续读、残留 socket 恢复；
- 独立写者在原始提交后、规范 COMMIT 前后遭 SIGKILL 的恢复。

固定依赖与 vendor 源码在独立 venv 构建成功；Ruff、格式检查及82份上游源文件校验通过。

## 延迟样本

应用读帧至规范化：p50 **2.723ms**、p99 **33.584ms**、max **250.675ms**。测量包含快照缓冲与全量簿校验，范围截至规范化，提交及下游延迟需另行测量。

[事件 schema](event-v1.schema.json) · [契约](event-contract.md) · [依赖来源](../THIRD_PARTY.md)
