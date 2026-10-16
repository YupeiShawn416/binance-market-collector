# 成交回补、缺口目录与双视图

**2026-10-16：原30秒样本的112个缺失原始成交已由官方日度trades归档真实补齐。** 昨日REST超时和ZIP未发布的失败证据保留于`evidence/repair/`；今日成功证据独立保存在`evidence/archive-2026-10-16/`。未改写原观察日志，也未拆分聚合成交伪造逐笔。

## 2026-10-16 官方逐笔归档验收

下载`BTCUSDT-trades-2026-10-15.zip`（29,880,144字节），SHA256与官方CHECKSUM相符：

`540c4776c3d8565548ec78107478428156b712049a984d1953935e7c9bd4cba8`

归档共4,346,924行，ID 6762627477..6766974400；读取到EOF校验ZIP成员CRC，并确认全文件ID连续。按本地首末成交ID抽取3847行，其中3735笔与原观察的价格、每笔数量、主动方向及共同毫秒精度的成交T时间全部一致。新增112笔总量0.63009000 BTC，完整样本总量8.76668000 BTC。3847笔重新聚合后与昨日下载的758个官方aggTrades组逐组吻合，差异0。

| 验收 | 真实数据结果 |
|---|---|
| 原缺口 | 6763422659..6763422770；现在REPAIRED，cursor=6763422771 |
| 分页与重启 | 首32笔提交、关闭；新进程从已提交cursor继续 |
| SIGKILL恢复 | 第二页32笔CSV原始字节提交后强杀，留下1个pending来源；重启恢复32笔，再完成32+16笔 |
| 重复导入 | 112笔再次输入；inserted=0、duplicates=112，事实总数仍3847 |
| 研究视图 | repaired连续；observed仍3735笔且缺112笔；known_by_ns取原采集结束时间仍缺112笔 |
| 执行隔离 | ResearchTrade仍execution_allowed=False，原实时Consumer拒绝该类型；系统尚无真实订单服务 |
| 原日志 | SHA256不变，4019事件链与末盘口哈希不变，17条质量边界保留 |
| 回归 | 原52项+新增5项归档协议测试，共57项通过；Ruff通过 |

新增`archive.py`复用Python标准库ZIP/CSV/SHA256；逐笔事实复用既有精确解析，回补目录新增来源种类`historical-archive`，保留CSV页原字节和包含ZIP哈希的来源引用。未新增运行依赖。原始ZIP、CHECKSUM、HTTP响应头、完整对齐CSV、验收JSON和回补前目录备份均保留。

归档CSV是原始成交粒度，与WS raw trade的成交事实可对应；文件时间为微秒，本地旧WS时间为毫秒。选定3847笔中有3839笔包含非零的亚毫秒部分；只能在共同毫秒精度声称一致，不能声称本地原观察具备这些低位时间。归档没有WS的E时间、本机接收时间、连接代次或L2盘口变化。补齐成交不恢复断线期间盘口，不证明模拟成交精度。

这是显式离线归档导入和真实验收，**常驻REST回补任务尚未增加每日ZIP自动发现/调度**。先前`evidence/repair/acceptance.json`和`gap-catalog.json`为10月15日的历史快照；当前目录状态以`repairs.sqlite`只读查询及今日`acceptance.json`为准。之前发布的v0.2 ZIP也保留原状态。

在项目目录重现本次强杀验收，使用回补前目录备份和全新输出目录，避免覆盖今日证据：

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

下文保留原设计及10月15日验收经过，标明当时实际限制。

## 模块与工程边界

```text
原始观察库 live.sqlite（只读）
    ├─ 既有 Journal → Publisher / replay → 原始 Record → 既有 Consumer
    └─ sync_observed（事件游标与链校验）
            ↓
       独立 repairs.sqlite
       ├─ gaps：发现位置、缺失范围、状态、分页游标、重试时间、错误
       ├─ sources：原始HTTP响应、URL、获取时间、精度、checksum、处理状态
       ├─ facts + provenance：单一成交事实、多来源证据、冲突检查
       └─ boundaries：原始连接和盘口质量边界，成交修复不清除它们
            ↑
    RepairWorker → HistoricalClient → 官方 GET /api/v3/historicalTrades
            ↓
    coverage / research(observed 或 repaired, known_by_ns)
            ↓
    ResearchTrade(execution_allowed=False)，不进入实时Publisher
```

继续复用SQLite的事务、WAL、FULL与恢复、aiohttp的HTTP传输、msgspec的冻结类型/校验/编码，以及原有Cryptofeed与orderbook盘口。新增代码集中在业务所需的缺口、来源、知识时间和分页规则，不另造消息中间件或文件事务协议。运行依赖没有增加；版本更新为0.2.0。

| 文件 | 职责 |
|---|---|
| `trade_history.py` | 一个公开历史GET、限速、Retry-After、原始成交精确解析与精度兼容比较 |
| `repair_store.py` | 独立单写者目录、只读并发查询、原始响应先提交、事实/来源/游标原子提交 |
| `repair.py` | 增量发现缺口、分页任务、恢复pending响应、重试调度、常驻follow |
| `history_compare.py` | 原始成交与历史聚合成交按ID范围、数量、价格、主动方向、T时间进行比较，不拆聚合 |
| `scripts/repair_acceptance.py` | 只用真实样本与公开下载数据的验收；原始成交补齐未通过则非零退出 |
| `tests/test_repair.py` | 明确标注为合成数据的离线协议与崩溃测试，与真实证据隔离 |

## 数据保留与恢复

原观察库不追加回补事件、不更改旧Quality、不重新排序。回补库不能与原库同一路径或同一文件；外来schema在DDL前拒绝。`repair`独占目录写者锁，`coverage`使用SQLite只读连接，可以在任务运行时查询已提交状态。

每个HTTP响应先保存原始字节、URL、获取时间与SHA256，再做解析。通过校验的一页中，成交事实、来源引用、处理水位、缺口分页游标和目录修订号在同一个SQLite事务中提交。失败不会留下半页事实或提前前进的游标。已落盘但未处理的响应会先恢复，再发新的请求。

相同成交ID与事实重复收到不增加事实行；每份来源仍保留。相同ID价格、数量、主动方向或共同时间精度下的T时间冲突，则整页回滚并进入CONFLICT，保留错误原始证据。不会静默覆盖旧事实。微秒来源与毫秒来源只在共同精度比较时间，来源自己的原始时间仍保留，不宣称恢复毫秒来源已经丢掉的低位精度。

分页请求从第一个未补齐ID开始，限制1..1000条；拒绝空页、页内重号/倒序、不连续、超出任务和精度错误。页内校验失败不推进。请求超时使用持久退避；418/429的端点冷却持久化，重启及`--retry-now`不会绕过Retry-After。

## 缺口和连续性

- OPEN：发现缺口或请求失败，缺失尚未填满。
- REPAIRING：已补入部分，cursor指向首个仍缺失ID。
- REPAIRED：目标ID范围内事实齐全。仅指成交ID覆盖。
- CONFLICT：出现矛盾事实，停止自动修复，待人工核查。

`continuity(first_id,last_id,view=...)`要求显式范围。返回连续区间、缺失区间、冲突、complete以及作用域。范围之外仍判缺失，不凭最小/最大已知ID推断全市场覆盖。不根据LIVE状态判历史连续，也不根据新快照推断断线期间的L2路径。

`complete=True`不授予下单权限，不证明行情新鲜、不证明盘口路径连续、不证明账户对账完成。成交回补不会清除17条原样本质量边界。盘口断线恢复仍按原计算内核重新快照/桥接。

## 当时观察到的与后来补齐的

1. **原始实时和历史观察回放**继续读取原Event v1日志，得到原顺序、原时间、原缺口，与本次回补无关。
2. **observed研究视图**只读已导入的原始观察事实，不包括后来历史接口补回的事实。
3. **repaired研究视图**包含经过校验的补回事实；按成交ID排列，只供完整成交历史研究和状态恢复。它不是原始到达顺序。
4. `known_by_ns`限制来源知识时间。原观察事实使用事件的`normalized_wall_ns`；回补事实使用本地响应处理阶段的墙钟时间，不得早于HTTP获取时间。因此按原样本结束时间查询，不会看见事后补回的事实。

知识时间是行情服务层的时间，不是网卡时间、fsync完成时间或策略实际收到的时间；它依赖墙钟质量。逐事件复现实盘观察过程仍以原日志`market_seq`为准。目录revision是提交修订号，审计原始证据不可改写；本版还没有长期不可变修复数据集导出/签名功能。

`ResearchTrade`与原始`Record`是不同类型，包含`execution_allowed=False`；既有Consumer在入口拒绝此类型，Publisher也无法把回补库当作Event v1日志打开。历史补算没有到实时发布或订单端的连接。当前项目仍无真实订单服务；未来接入真实执行时，必须继续在执行边界验证模式与权限，不能仅相信调用者传来的布尔字段。

## 2026-10-15 历史验收：当时未通过112笔补齐

原库dataset为`240927e9-f7aa-4415-9970-76c3874cc52b`；文件SHA256前后均为：

`784afa34c44d1b6cde6237166a961166e924186aabc5a9f75c5b296d9d6325a4`

| 项目 | 真实结果 |
|---|---|
| 原始观察 | 3735笔；首ID 6763420844，末ID 6763424690 |
| 目录缺失 | 6763422659..6763422770，共112个ID；OPEN，cursor保持6763422659 |
| 原始历史接口 | 主REST及官方备用域名/WS API连接超时；行情专用域对historicalTrades返回404 |
| 真实回补写入 | 0；`real_112_raw_backfill_verified=false` |
| 官方历史聚合下载 | 同一T时间范围，758条，覆盖3847个原始ID；已保存原响应、获取时间、checksum及CSV |
| 已观察事实对照 | 3735笔的价格、主动方向、毫秒T时间全部相符；737个完整聚合组的数量相符，差异0 |
| 缺失组 | 21条聚合组覆盖112个缺失ID，总量0.63009000 BTC；不恢复其中每笔数量 |
| 总成交量口径 | 本地已观察8.13659000 BTC；官方历史聚合8.76668000 BTC |
| 按本地墙钟数值下载的完整观察窗口 | 787条聚合，覆盖4073个原始ID；比本地首末ID范围额外包含226个ID，不能都算作中间缺段 |
| 原日志验收 | 4019事件链及盘口哈希仍通过；未处理原始尾部0 |

API资料与真实网络响应有区别：币安行情专用域支持列表未列historicalTrades，本次实际返回404；不能把“已有公开历史接口”写成“此域名一定能补”。当日每日ZIP/CHECKSUM探测也返回NoSuchKey；官方说明日度文件次日才发布。原始逐笔不可获取时，本实现保留OPEN，不把聚合总量均分为逐笔，不以邻近日期替代。

下载的是官方历史**聚合成交**，不是逐笔历史ZIP；历史比较任务已经完成这一口径的真实对照，但原始112笔补齐/去重/恢复的真实端到端验收仍受网络/数据可用性阻塞。下次历史逐笔端点可达后，重跑验收脚本即可继续原目录任务，不必改原日志。

### 两个时间范围，不混算

| 查询范围 | 起止（UTC） | 聚合行数 | 聚合记录覆盖的原始ID数 |
|---|---|---:|---:|
| 按本地首末成交的交易所T时间对齐 | 02:42:07.636..02:42:35.659 | 758 | 3847 |
| 将本地观察墙钟数值作为交易所T查询范围 | 02:42:06.647..02:42:36.436 | 787 | 4073 |

第二行包括连接建立前、采集尾部及到达时延/时钟口径的影响，不能把4073−3735=338全判为112笔缺口扩大。原始ID范围外另有226个ID，其中首端223个、末端3个；ID范围内缺失112个。`historical-agg.csv`保存第一种，`historical-wall-window.csv`保存第二种；每页原JSON和meta保存完整查询URL、实际获取时间与checksum。

### 字段口径

| 维度 | 原观察事件 | 本轮下载的历史aggTrades | 官方日度trades文件（本轮未取得） |
|---|---|---|---|
| 标识 | 每笔trade_id | 聚合ID `a`；`f..l`是包含的原始成交ID范围 | 每笔trade Id |
| 数量 | 每笔quantity_units | `q`是组内合计，不能均分 | 每笔qty |
| 时间 | T成交时间、E事件时间、应用接收与规范化时间 | T；本轮REST默认毫秒，不含原接收时间 | 2025年起现货文件按官方说明为微秒 |
| 方向 | aggressor | `m=true`表示买方maker，即主动卖出 | isBuyerMaker可映射主动方向 |
| 盘口、连接、缺段 | 有L2与质量边界、原始引用 | 没有 | trades文件也没有L2路径 |

公开协议资料：[原始历史成交](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#old-trade-lookup)、[公开行情域支持列表](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md)、[日度下载说明](https://github.com/binance/binance-public-data)。协议资料用于说明口径，真实HTTP结果以本轮保存的meta和响应为准。

## 测试范围

离线夹具用ID1、114构造中间112个ID缺口，明确是合成协议数据，不使用这些事实填真实样本。测试涵盖32/32/32/16分页、重复与重叠响应、按已提交cursor续作、原始响应落盘后/事实提交前/提交后的SIGKILL、冲突整页回滚、坏页、不支持aggTrade、只读并发查询、未来知识/冲突隔离、Retry-After跨重启、回补类型不能进入原始消费者。

这些测试证明代码处理边界；不替代真实112笔验收，也不证明跨断线的完整订单队列、长期容量或交易执行正确性。

最终完整回归52项通过（原29项＋新增23项）；Ruff检查/格式检查与82个固定上游源码输入校验通过。新增端点分页测试还覆盖“满页最后一条恰好等于查询终止毫秒”的情况，不提前截断同毫秒后续成交。具体终端结果见`evidence/repair/tests.txt`。
