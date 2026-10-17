# 成交回补与连续性

回补用于恢复断线期间漏收的原始逐笔成交。币安为每笔成交分配递增 ID；例如相邻两笔 ID 为10和13，目录会记录11–12这一缺失范围，并向历史源请求对应事实。

缺口目录（catalog）是独立的 SQLite 数据库，保存缺失范围、回补来源、成交事实与处理进度。原始行情库保存当时收到的消息、统一事件和质量状态；目录库在此基础上提供按成交 ID 查询的研究视图。两个库分别保留当时观察与事后补充的事实。

## 数据路径

观察视图 `observed` 查询原始观察成交，修复视图 `repaired` 同时查询校验通过的回补成交。自动任务通过 HTTP 历史接口 REST 获取原始成交，离线导入读取官方日度文件。

```text
原始行情库 live.sqlite
    ├─ 发布 / 回放 → 原始事件消费者 → 重建当时的盘口和质量状态
    └─ sync_observed：逐事件校验并导入观察事实、发现缺口
            ↓
       回补目录 repairs.sqlite
       ├─ gaps：缺失范围、状态、分页进度、重试时间
       ├─ sources：历史响应或 CSV 原字节、获取时间、校验和
       ├─ facts + provenance：去重后的成交事实及其全部来源
       └─ boundaries：原始连接与盘口质量边界
            ↑
       回补任务 RepairWorker ← REST historicalTrades / 离线日度 trades
            ↓
       coverage：查询连续区间
       research：读取 observed / repaired 成交视图
            ↓
       研究记录 ResearchTrade
```

|模块|职责|
|---|---|
|`trade_history.py`|历史成交 GET、限速、Retry-After、精确解析、时间精度比较|
|`repair_store.py`|单写者目录、只读查询、原始来源保存、事实与游标原子提交|
|`repair.py`|增量发现、分页、pending 恢复、重试调度、常驻 follow|
|`archive.py`|日度 ZIP 的 SHA256 / CRC 校验、CSV 读取与来源保存|
|`history_compare.py`|逐笔与聚合成交按 ID、数量、价格、主动方向和成交时间对齐|

回补通过 aiohttp 请求历史 REST 接口，通过标准库读取日度 ZIP / CSV；msgspec 校验成交类型，SQLite 事务提交事实与进度。自动任务使用 REST，日度 ZIP 使用离线导入，每日归档发现与调度列入后续实现。

在采集期间另开终端运行回补任务，`--follow` 使任务持续发现和处理新缺口：

```bash
uv run market-service repair --db data/btc.sqlite --catalog data/repairs.sqlite --follow
```

目录同步观察记录后，可查询指定 ID 范围的连续性。以下以112笔缺口验收样本的首末 ID 为例，实际使用时替换为自己的成交范围：

```bash
uv run market-service coverage --db data/btc.sqlite --catalog data/repairs.sqlite \
  --first-id 6763420844 --last-id 6763424690 --view repaired
```

## 提交与恢复

原日志与目录库使用独立文件，打开前校验数据库结构（schema）。`repair` 持有写者锁，确保单进程修改目录；`coverage` 使用只读连接，可在回补运行时查询已提交结果。

1. 先提交原始响应字节、URL、获取时间和 SHA256，再解析。
2. 校验通过后，在同一事务中提交成交事实、来源引用、已处理位置、缺口游标（下一笔待补 ID）和目录修订号 revision。
3. 重启先处理 pending 来源，即原字节已落盘、事实仍待解析提交的来源，再请求后续页。
4. 相同 ID、相同事实只保存一条事实，保留全部来源；价格、数量、主动方向或共同时间精度下的 T 时间冲突时，整页回滚并置为 CONFLICT（事实冲突）。

分页从首个缺失 ID 开始，每页1–1000条。空页、重号、倒序、不连续、超出任务范围或精度错误均拒绝，游标保持原位。请求失败后按递增间隔重试，下一次请求时间写入目录。HTTP 418 / 429 表示限流，响应中 Retry-After 指定的等待时间跨重启保留，`--retry-now` 仍遵守该冷却时间。

## 连续性与知识时间

缺口游标 cursor 指向下一笔待回补的 ID，目录根据范围内已校验的事实更新任务状态：

|缺口状态|含义|
|---|---|
|OPEN|范围内仍有缺失，等待获取或重试|
|REPAIRING|已补入部分，cursor 指向首个缺失 ID|
|REPAIRED|目标成交 ID 范围齐全|
|CONFLICT|同一成交 ID 的来源事实矛盾，停止自动修复，等待核查|

`continuity(first_id, last_id, view=...)` 查询显式范围，返回连续区间、缺失区间、冲突及 `complete`。覆盖范围以请求边界为准，`complete` 表示该范围的成交 ID 齐全且无冲突。连接状态、盘口连续性和新鲜度分别判断；盘口恢复采用新快照与增量桥接。

|视图或字段|用途|
|---|---|
|原始 Event v1 回放|按 `market_seq` 复现原顺序、时间和缺口|
|`observed`|查询当时观察到的成交事实|
|`repaired`|查询观察事实与经校验的回补事实，按成交 ID 排列|
|`known_by_ns`|限制查询时已知的来源：观察事实取统一事件生成时间 `normalized_wall_ns`，回补事实取本地处理时间且晚于或等于获取时间|
|`revision`|目录事务的递增修订号|

知识时间表示行情服务获得事实的时间，采用本机 UTC 墙钟。例如10:05回补得到10:00的成交，`known_by_ns` 限定在10:01时，查询结果只采用当时已经获得的来源。逐事件复现观察过程使用原日志序号；按知识时间筛选用于研究数据的时间可见性。来源保存各自时间精度，跨来源比较取共同精度。

研究结果使用独立 `ResearchTrade` 类型，固定 `execution_allowed=False`，表示只供研究和状态恢复。原始事件发布器 Publisher 从 Event v1 日志读取 `Record`，实时消费者 Consumer 按该类型接收，形成独立的数据路径。未来订单服务在执行入口继续校验运行模式与权限，历史补算使用研究路径。

## 数据口径对比

原始逐笔成交一行对应一笔交易，聚合成交将若干原始成交合并为一组。回补事实使用原始逐笔来源，聚合来源用于核对组内范围与合计数量。下表中的 T 是交易所成交时间，E 是消息事件时间；主动方向表示发起成交的一方。

|维度|WS 原始成交|REST aggTrades|日度 trades|
|---|---|---|---|
|标识|每笔 trade_id|聚合 ID `a`，原始 ID 范围 `f..l`|每笔 trade Id|
|数量|每笔 quantity_units|组内合计 `q`|每笔 qty|
|时间|T、E、应用接收与规范化时间|T，本次下载使用默认毫秒|2025年起现货归档采用微秒|
|方向|aggressor|`m=true`：买方 maker，主动卖出|isBuyerMaker 映射主动方向|
|配套采集信息|L2、连接和质量边界|聚合成交字段|逐笔成交字段|

## 112笔真实回补验收

2026-10-16 使用官方 `BTCUSDT-trades-2026-10-15.zip` 补齐原30秒样本，证据保存于本地 `evidence/archive-2026-10-16/`。

归档大小29,880,144字节，4,346,924行，ID `6762627477..6766974400` 连续。完整读取校验 ZIP 成员 CRC，SHA256 与官方 CHECKSUM 一致：

```text
540c4776c3d8565548ec78107478428156b712049a984d1953935e7c9bd4cba8
```

按本地首末 ID 抽取3847笔，其中3735笔与原观察的价格、数量、主动方向和毫秒 T 时间一致。新增112笔共0.63009000 BTC，总量从8.13659000增至8.76668000 BTC；重新聚合后与758个官方 aggTrades 组逐组一致。

|验收项|结果|
|---|---|
|缺口|`6763422659..6763422770` → REPAIRED，cursor `6763422771`|
|分页与重启|每页笔数32 / 32 / 32 / 16；首32笔提交后，新进程从已提交游标继续|
|强杀恢复|第二页原始 CSV 提交后用 SIGKILL 强制终止进程，留下1个 pending 来源；重启恢复32笔，再完成32+16笔|
|重复导入|112笔再次输入，inserted=0、duplicates=112，事实总数3847|
|视图|repaired 连续；observed 保留3735笔及112笔缺口；知识时间限定在原采集结束时仍缺112笔|
|原日志|文件 SHA256、4019条事件链和末盘口哈希保持一致，保留17条质量边界|
|测试记录|当时57项测试及 Ruff 通过，覆盖归档协议与恢复|

归档成交时间为微秒，原 WS 样本为毫秒；3847笔中3839笔具有非零亚毫秒部分，比较按共同毫秒精度进行。WS 事件时间 E、本地接收时间、连接代次及 L2 路径由原始采集日志保存，trades 归档提供成交事实。

使用本地验收资料和全新输出目录复现：

```bash
mkdir -p data/archive-recheck
cp evidence/archive-2026-10-16/catalog-before.sqlite data/archive-recheck/repairs.sqlite
uv run python scripts/archive_acceptance.py \
  --db evidence/live.sqlite --catalog data/archive-recheck/repairs.sqlite \
  --archive evidence/archive-2026-10-16/BTCUSDT-trades-2026-10-15.zip \
  --checksum evidence/archive-2026-10-16/BTCUSDT-trades-2026-10-15.zip.CHECKSUM \
  --aggregates evidence/repair/historical-agg-1.json \
  --reference evidence/live-verification.json --output data/archive-recheck
```

## 初次验收与时间窗口

2026-10-15 的初次验收遇到 REST 超时、行情专用域 historicalTrades 404、当日 ZIP 尚未发布，目录保持 OPEN。次日归档完成上述回补。初次证据保存在本地 `evidence/repair/`，包括原始响应、查询 URL、获取时间及 checksum。

原库的数据集身份 dataset_id 为 `240927e9-f7aa-4415-9970-76c3874cc52b`，文件 SHA256：

```text
784afa34c44d1b6cde6237166a961166e924186aabc5a9f75c5b296d9d6325a4
```

两种查询窗口的结果分别保存，缺口按本地首末成交 ID 判断：

|查询窗口（UTC）|聚合组数|覆盖原始 ID 数|文件|
|---|---:|---:|---|
|交易所 T：02:42:07.636–02:42:35.659|758|3847|`historical-agg.csv`|
|本地墙钟数值作为 T：02:42:06.647–02:42:36.436|787|4073|`historical-wall-window.csv`|

首末 ID 范围内缺112笔，范围外多226个 ID（首端223、末端3）。3735笔已观察成交的价格、方向、毫秒 T 一致；737个完整聚合组数量一致。其余21组覆盖缺失112笔，总量0.63009000 BTC；每笔数量由次日原始归档取得。

测试覆盖分页、重叠去重、事务前后 SIGKILL、pending 恢复、冲突回滚、坏页、只读并发、知识时间、类型隔离及 Retry-After 跨重启。合成夹具位于 `tests/test_repair.py`，真实样本验收由 `scripts/repair_acceptance.py` 和 `scripts/archive_acceptance.py` 执行。

协议：[历史原始成交](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#old-trade-lookup) · [行情域接口](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md) · [日度归档](https://github.com/binance/binance-public-data)
