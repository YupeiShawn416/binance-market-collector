# 行情接口 v1

本接口约定行情服务向下游传递的数据格式、顺序和恢复方式。实时订阅与历史回放都将统一事件交给相同的盘口更新逻辑，调用方据此得到可比较的市场状态。

v1 表达币安 BTC-USDT 的 L2 盘口和逐笔成交。L2 按价格合计挂单数量；L3 进一步保留订单身份和生命周期，使用独立 v2 日志与订阅，见 [L3说明](l3-market-data.md) 与 [v2 schema](event-v2.schema.json)。

## 对象与处理流程

类型定义位于 `market_service.contracts`，使用 msgspec 的冻结结构体表达：对象创建后字段固定，解码时校验字段、类型和范围。传输编码为 MessagePack 二进制格式，每帧前附长度。

|对象|作用|
|---|---|
|`Event`|一条统一行情事件，包含序号、时间、数据载荷和更新后的盘口哈希|
|`Record`|Event 与事件哈希、累计事件链哈希的组合，是消费与回放的输入单位|
|`Subscribe`|消费者发送的订阅请求，指定数据集、起始游标和持续订阅方式|
|`Batch`|发布器返回的一批 Record，以及当前提交位置|

发布器 Publisher 从日志读取 Record，消费者 Consumer 校验并逐条处理。消费者内部的 Projection 保存品种信息、可见盘口和质量状态，应用每条 Event 后得到新状态。游标表示已处理序号，检查点保存该位置的完整状态及事件链位置，二者一起提交以支持重启续读。

## 数据身份与事件顺序

|字段|语义|
|---|---|
|`schema_version`|接口格式版本，固定为1|
|`dataset_id`|本地日志的数据集身份，重启保留；消费者用它核对检查点归属|
|`market_seq`|从1开始严格连续的统一事件序号，决定消费与回放顺序|
|`raw_seq`|原始接收记录序号；每条原始记录可产生零至多个统一事件|
|`venue` / `instrument`|交易场所与品种：`BINANCE` / `BTC-USDT`|
|`session_id`|WebSocket 连接实例标识|
|`book_epoch`|盘口重建代次，重连或质量断裂时递增|
|`parser_version`|将原始消息转为统一事件的解析器版本|

`dataset_id/raw_seq` 可定位事件引用的原始字节、校验和与消息种类。解析快照时会衔接先前缓存的增量，因此序列内的原始接收时间可能回退；`market_seq` 始终作为处理顺序。

## 事件载荷与盘口更新

每条 Event 携带下表中的一种载荷。价格和数量使用整数保存，校验范围为有符号64位；价格、数量和序号采用其中的非负范围。

|标签|内容与作用|
|---|---|
|`instrument`|品种及单位定义：base、quote、price_tick、quantity_unit、order_step、spec_version|
|`snapshot`|某个更新序号的双边已知盘口：last_update_id、source_depth、已知价格边界；整体替换本地簿并进入 SYNCING|
|`delta`|一批价位更新及 first/last update-ID；数量为该价位更新后的绝对量，零量删除价位|
|`trade`|单笔已成交记录：trade_id、price_ticks、quantity_units、aggressor、trade_time_ns|
|`quality`|连接、盘口或成交的质量状态及原因|

`base` 为基础资产 BTC，`quote` 为计价资产 USDT。价格 = `price_ticks × price_tick`，基础资产数量 = `quantity_units × quantity_unit`。行情数量精度由 `quantity_unit` 表示，委托数量步长由 `order_step` 表示，二者分别校验。

快照桥接要求后续增量覆盖 `last_update_id + 1`，之后按更新序号检查连续性，每批双边更新整体应用。已知价格覆盖由快照边界确定；边界外数量标为未知。Trade 记录成交事实，盘口数量由 Snapshot / Delta 更新。

质量按 `connection`（连接）、`book`（盘口）、`trades`（成交）分别记录：

|状态|含义|
|---|---|
|SYNCING|正在建立该流的可用状态|
|LIVE|该流已通过当前可用性检查|
|INVALID|该流出现缺段、校验错误或失效边界|
|DISCONNECTED|连接结束|

质量状态随事件一起保存，回放可复现当时各条数据流的可用范围。

## 时间字段

全部时间换算为纳秒（ns）存储，源数据分辨率另行记录。默认币安流采用毫秒时间，换算后 `source_resolution_ns=1_000_000`。

|字段|采样位置|
|---|---|
|`receive_wall_ns` / `receive_mono_ns`|应用读到完整消息时的墙钟 / 单调时钟；`boot_id` 标记采集进程时钟实例|
|`normalized_wall_ns` / `normalized_mono_ns`|生成统一事件时的墙钟 / 单调时钟；`normalizer_instance_id` 标记规范化进程|
|`exchange_time_ns`|交易所消息中的 E 事件时间；源缺失时取 null|
|`trade_time_ns`|Trade 载荷中的 T 成交时间|

墙钟用于 UTC 时间对齐；单调时钟用于同一进程时钟实例内的耗时计算。进程恢复日志尾部时，接收与规范化可能分属两个实例。消费者可另记自身到达和处理时间；磁盘同步完成时间需独立采样。

## 哈希校验

哈希是内容摘要，用于比较数据和状态。三种哈希分别检查盘口内容、单条事件和累计顺序：

- `book_hash = SHA256(MessagePack((bids, asks)))`。bids 按价格降序、asks 按升序，元素为整数价格与数量。
- `event_hash = SHA256(MessagePack(Event))`。摘要覆盖事件的质量、精度、序号、时间及盘口哈希。
- `chain_hash = SHA256(previous_chain_bytes + event_hash_bytes)`。前一链值与当前事件摘要连接后再计算，初值为32字节0。

采集端比较交易所适配器维护的盘口与 Projection。消费者和回放逐条核验事件链，并重新计算盘口哈希，与事件中保存的结果比较。

## Socket 订阅与续读

Unix socket 是同一台机器上进程间传递数据的连接。消费者先发送 Subscribe，发布器随后发送 Batch；两端使用相同 v1 类型。

1. 请求包含 `schema_version`、可选 `dataset_id`、`after_seq` 和 `follow`。`after_seq` 表示已处理到的序号，首次从0开始。
2. 返回 `Batch(dataset_id, records, frontier, complete)`；`frontier` 表示当前已提交的最大序号，每批最多128条。
3. 消费者从检查点恢复盘口与事件链前值，再处理 `after_seq` 之后的 Record。
4. 消费游标与业务状态在同一事务中提交，重送通过事务或幂等处理。参考 Consumer 保存行情状态；接入业务服务时由该服务提交自身状态。
5. `follow=false` 在追上发布器当次检查的已提交位置后结束；`follow=true` 读完历史后继续等待新事件。读取备份文件可固定历史范围。
6. 游标越界、数据集或版本冲突、坏帧会触发报错并关闭连接。CLI 使用原 state 路径重启续读。

帧头为4字节网络字节序长度，帧上限16MiB。socket 权限为0600，最多32个连接；发送缓冲区排空（drain）设有超时，用于处理读取过慢的连接。
