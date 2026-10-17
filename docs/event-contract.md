# 行情接口 v1

定义：`market_service.contracts` 中的冻结 msgspec Struct；编码：长度前缀 MessagePack。价格和数量采用整数，数值受有符号64位范围约束；时间单位为 ns。未知字段、版本、类型和越界值一律拒绝。

L3 使用独立 v2 日志和订阅，见 [L3说明](l3-market-data.md) 与 [v2 schema](event-v2.schema.json)。

## 标识与顺序

|字段|语义|
|---|---|
|`schema_version`|固定为1|
|`dataset_id`|持久日志身份，重启保留；消费游标按 dataset 隔离|
|`market_seq`|严格连续的规范事件序号，决定回放顺序|
|`raw_seq`|原始接收记录序号；每条原始记录可产生零至多个事件|
|`venue` / `instrument`|`BINANCE` / `BTC-USDT`|
|`session_id`|WS 连接实例|
|`book_epoch`|重连或质量断裂时递增|
|`parser_version`|规范化版本|

消费者分别持有 cursor、Projection 和输入记录。`dataset_id/raw_seq` 定位原始 payload、checksum 与 kind。

## 时间

|字段|采样点与精度|
|---|---|
|`receive_wall_ns` / `receive_mono_ns`|应用读到完整消息；`boot_id` 标记采集进程时钟实例|
|`normalized_wall_ns` / `normalized_mono_ns`|生成规范事件；`normalizer_instance_id` 标记规范化进程|
|`exchange_time_ns`|源 E 字段换算；默认毫秒流，`source_resolution_ns=1_000_000`|
|`trade_time_ns`|Trade 的源 T 字段换算|

单调时间差仅在同一时钟实例内计算。未处理原始帧恢复时，采集与规范化实例可能不同。消费者另记实际到达时间和处理顺序；fsync 完成时间属于独立观测点。

Snapshot 的交易所时间缺失时取 null。快照缓冲桥接可能使接收时间在规范序列内回退，回放仍按 `market_seq`。

## 载荷

|标签|语义|
|---|---|
|`instrument`|base、quote、price_tick、quantity_unit、order_step、spec_version|
|`snapshot`|双边已知簿、last_update_id、source_depth、已知价格边界；原子替换后进入 SYNCING|
|`delta`|first/last update-ID；双边绝对量原子更新，零量删除；桥接 last_update_id+1|
|`trade`|原始逐笔 trade_id、price_ticks、quantity_units、aggressor、trade_time_ns|
|`quality`|connection / book / trades 的 SYNCING、LIVE、INVALID、DISCONNECTED 及原因|

价格 = `price_ticks × price_tick`；基础资产数量 = `quantity_units × quantity_unit`。行情精度由 `quantity_unit` 表示，订单步长由 `order_step` 表示，分别校验。

已知覆盖取快照价格边界；边界外按未知处理。L2 按价位表达可见数量，逐单身份与队列顺序由 L3 契约表达。

## 哈希

- `book_hash = SHA256(MessagePack((bids, asks)))`，bids 按价格降序、asks 按升序，元素为整数价格与数量。
- `event_hash = SHA256(MessagePack(Event))`，绑定质量、精度、序号和时间。
- `chain_hash = SHA256(previous_chain_bytes + event_hash_bytes)`，初值为32字节0。

采集端逐批比较源原生簿与 Projection；回放逐事件核验盘口哈希和事件链。

## Socket 与续读

1. 发送 `Subscribe(schema_version, dataset_id?, after_seq, follow)`。
2. 帧头为4字节网络字节序长度，帧上限16MiB；返回 `Batch(dataset_id, records, frontier, complete)`，每批最多128条。
3. 每条 `Record` 含 Event、event_hash、chain_hash；消费者从完整盘口检查点和链前值恢复。
4. 输入 cursor 与业务状态在同一事务中提交；重送由事务或幂等逻辑处理。参考 Consumer 保存市场视图。
5. `follow=false` 读取当前提交前沿；`follow=true` 从同一日志继续追尾。
6. 越界游标、dataset / 版本不符或坏帧会关闭连接并报错。CLI 使用原 state 路径重启续读。

Socket 权限0600，最多32个连接，写出 drain 设超时。协议承载市场日志。
