# 决策和交易共用的行情接口 v1

本文保留Binance v1契约。新增L3使用独立v2日志和显式v2订阅，见[l3-market-data.md](l3-market-data.md)和[event-v2.schema.json](event-v2.schema.json)；v1客户端不会静默接收或降级v2逐单事件。

契约定义在 `market_service.contracts`，使用冻结 msgspec Struct；线协议为长度前缀 MessagePack。没有 Python对象pickle、浮点价格/数量、数据库连接或交易所回调引用。非负整数受有符号64位上限约束，时间为有符号64位ns；不支持的schema、未知字段、类型和范围明确拒绝。

## 标识与顺序

- `schema_version=1`；`dataset_id`是持久日志身份，重启保留；消费cursor不能跨dataset复用。
- `market_seq`是规范事件严格连续序号；`raw_seq`是原始接收记录序号；一个raw可以产生多个规范事件或仅审计记录。二者不能互换。
- `venue=BINANCE`，`instrument=BTC-USDT`；`session_id`是WS连接实例；`book_epoch`在重连/质量断裂时增加。
- 每个消费者有自己的cursor、Projection和input日志；market_seq不是跨服务天然存在的全局时钟。
- `raw_ref`由dataset_id/raw_seq解析到原始payload、checksum及kind；`parser_version`固定规范化版本。

## 时间

- `receive_wall_ns`/`receive_mono_ns`在应用读到完整消息时采样，`boot_id`在本版是采集进程时钟实例ID，不是网卡硬件时间。
- `normalized_wall_ns`/`normalized_mono_ns`表示生成规范事件时刻，`normalizer_instance_id`标记规范化进程实例。恢复未处理raw时可以不同于原boot_id；跨实例不要直接相减。
- 规范化时间不等于fsync完成或消费者收到的时间。live消费者应另录自己的实际到达/处理顺序；不能把源receive时间当成模型已获得信息的时刻。
- `exchange_time_ns`是源E字段按明确单位转换；本版请求默认毫秒流，`source_resolution_ns=1_000_000`，并不因为整数容器是ns就声称源有ns精度。Trade还保留T字段对应的trade_time_ns。
- Snapshot有update-ID但没有可靠的对应exchange timestamp时，exchange_time_ns为空；不捏造时间。缓冲/快照桥接可能使raw接收时间在规范序列内非单调，重放按market_seq。

## 五类载荷

|type标签|内容和语义|
|---|---|
|instrument|base/quote、price_tick、quantity_unit、order_step、spec_version|
|snapshot|双边完整已知簿、last_update_id、source_depth、known_bid_floor/known_ask_ceiling；原子替换，先SYNCING|
|delta|first/last update-ID；双边原子绝对量更新，quantity=0删除；必须桥接last_update_id+1|
|trade|仅原始trade，含trade_id、price_ticks、quantity_units、aggressor、trade_time_ns；不接aggTrade冒充逐笔|
|quality|connection/book/trades三个scope，SYNCING/LIVE/INVALID/DISCONNECTED与原因|

`price_ticks × price_tick`得到价格；`quantity_units × quantity_unit`得到基础资产数量。**quantity_unit是基础资产精度单位，不是订单最小步长。** 例如原始成交可以精确保存1 satoshi，而LOT_SIZE仍是更大的order_step。交易服务以后单独检查订单步长，不允许为通过交易规则而舍入行情。

已知边界取自快照：之外可能存在未观测流动性；后续观测到个别深档不自动证明中间全部连续。没有无上限深度、无隐藏量或精确队列保证。

## 哈希

`book_hash = SHA256(MessagePack((按价格降序bids, 按升序asks)))`，元素为整数(price_ticks,quantity_units)。只编码实际已知档位，保持空档；质量、精度、seq和时间由外层事件hash绑定。

`event_hash = SHA256(MessagePack(Event))`；事件链为`SHA256(previous_chain_bytes + event_hash_bytes)`，初始前值32字节0。Source原生簿与规范Projection逐批比较；回放每个事件再验证book_hash。单一最终余额或best价相同不能代替这些验证。

## socket订阅与续读

1. 客户端发送Subscribe：schema_version、可选dataset_id、after_seq、follow。
2. 每帧头为4字节网络字节序长度，后接MessagePack；上限16MiB。服务返回Batch(dataset_id,records,frontier,complete)，每批最多128个Record。
3. Record含完整Event、event_hash、chain_hash。客户端从自己的checkpoint加载盘口和链前值，再逐事件应用；不能只保存seq、丢掉对应状态。
4. 消费者业务状态与cursor提交成功后才认为已消费。网络断开可用同一cursor再次Subscribe；未确认的输入允许重送，由消费者事务/幂等处理。当前参考Consumer持久化的是市场视图。
5. follow=false读至当前已提交前沿结束；follow=true从同一日志追尾，不在“历史读完”和“订阅实时”之间另开容易漏消息的窗口。
6. 超版本、越界cursor、dataset不符、坏帧将关闭连接并使SDK报错，不静默降级。CLI重新运行同一state路径可续读，当前SDK不替业务方盲目循环重试错误订阅。

订阅只读取市场日志，不授予更改交易账户的能力。socket为本机权限0600；服务端最多32个连接并对写出drain设超时。控制面和账户消息不在本协议中。
