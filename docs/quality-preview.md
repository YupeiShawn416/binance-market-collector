# 数据质量预览

只读消费 Event v1，复用 Consumer、Projection 和校验逻辑；Plotly 输出离线 HTML，Matplotlib Agg 输出 PNG。

## 使用

```bash
uv sync --frozen --extra preview
uv run market-service preview --db data/btc.sqlite --output data/quality-001.html --png data/quality-001.png --report data/quality-001.json
```

HTML 支持悬停、缩放、图例和图片导出；JSON 记录口径、计数和哈希。输出路径须为新文件，并与源库及 SQLite sidecar 分离。

|参数|默认值|含义|
|---|---:|---|
|`--max-events`|20000|尾部事件数，上限100000|
|`--time-bin-ms`|100|基础时间格宽；长窗口自动取整数倍|
|`--max-time-bins`|600|最大时间列数|
|`--price-bins`|120|价格格数，边界对齐整数 tick|
|`--band-bps`|20|以窗口盘口中位价为中心的单侧价格带|
|`--stale-seconds`|10|盘口过期阈值，应与采集配置一致|

窗口按事件数量截取，时长随成交活跃度变化。页面和 JSON 显示实际格宽。

活动库通过一次只读 WAL 事务固定水位。长读取会延迟 WAL 回收，大库可先备份再预览。

## 图层与口径

|图层|口径|
|---|---|
|盘口热力图|可见基础币挂单量的时间加权均值；颜色为 log(1+数量)，色条和悬停显示实际数量|
|灰色单元|无效、过期、同步中、部分缺失或快照范围外；完整有效覆盖内的零量使用最低色阶|
|最优报价|完整有效时间格内的末次已知买卖报价|
|质量条|连接、盘口、成交各自的格内最差状态；× 保留瞬时边界|
|成交量|观察到的主动买卖数量，按格求和、非负堆叠|

LIVE、SYNCING、INVALID、DISCONNECTED 来自事件；STALE、PARTIAL 为展示状态。盘口新鲜度取最后一条 Snapshot / Delta 的原始接收年龄；检查点尾部向前有界查找深度时间基准，缺失时保留未知。

## 顺序与校验

横轴为规范化时间 UTC。事件顺序固定为 `market_seq`；墙钟回退仅在显示坐标中单调化并计数。

校验范围：窗口前最近检查点的 checksum、身份和盘口哈希，随后事件链与逐事件盘口哈希，以及窗口事件引用的原始输入 checksum。整库校验使用 `verify`。

缺失 ID 数取窗口内显式 `trade_id_gap`。盘口覆盖取快照已知边界；跨边界的价格格标为未知。回补数据通过独立 `repaired` 视图查询。

## 验收

2026-10-17：样本含1047个事件、759笔观察成交、2次快照、268批深度更新，显示10个缺失成交 ID；有效盘口时长约91.1%，末事件链与盘口哈希匹配原验收。

测试覆盖时间加权、零量与未知、部分覆盖、断线、过期、检查点、瞬时缺口、墙钟回退、输入损坏及路径保护。PNG 已人工检查；HTML 结构和离线脚本已检查，浏览器交互与窄屏布局待验证。

`preview` 可选依赖由 `uv.lock` 固定。生成文件保留在 `data/`，README 样例图位于 `docs/assets/`。
