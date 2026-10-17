# 开源组件

本项目复用开源组件处理网络、交易所盘口、数据编码、存储和绘图。本页说明各组件承担的职责、项目补充的逻辑，以及构建时使用的来源。

项目许可证为 AGPL-3.0-or-later，见 [LICENSE](LICENSE)。`uv.lock` 固定依赖版本和下载包哈希（内容摘要），使安装使用相同依赖；源码依赖还固定到下表中的 Git 提交。

|组件|来源与许可|复用内容|
|---|---|---|
|Cryptofeed 3.0.2|`bmoscon/cryptofeed@bd0fe96093837262ce2c89ebc00286f48495fdad`，AGPL-3.0-or-later|提供 Binance 快照解析、L2 价格档更新和序列衔接；本项目子类补充严格校验与原始逐笔成交解析|
|orderbook 1.1.0|`bmoscon/orderbook@349c340f3b06c20d69e3ef9f49f28f5acea75560`，GPL-3.0-or-later|以 C 扩展维护按价格排序的买卖盘；本项目包装层处理整数单位、零量删除和整批更新|
|msgspec 0.22.0|发布包，BSD-3-Clause|定义创建后字段固定的事件类型，校验数据，编码 MessagePack 并导出 JSON Schema|
|aiohttp 3.14.5|发布包，Apache-2.0 AND MIT|发送 REST 请求、接收 WebSocket 推送、处理连接心跳与关闭|
|python-kraken-sdk 3.5.1，可选|PyPI，Apache-2.0|通过 SpotAsyncClient 签名请求、生成请求序号 nonce、获取 WebSocket 认证 token|
|Plotly 6.9.0，可选|发布包，MIT|离线交互热力图、缩放、图例与图片导出|
|Matplotlib 3.11.2，可选|发布包，附上游许可证|通过 Agg 渲染器生成静态 PNG|
|SQLite / Python 标准库|运行环境|事务、WAL、备份、事件循环、文件锁和哈希|

## 源码随仓库构建

Cryptofeed 与 orderbook 使用仓库内固定源码构建，便于核查实际运行的代码。

`vendor/` 保留构建源文件和许可证，排除上游测试行情、文档站点、Git 对象及编译产物。`third_party_manifest.json` 记录源文件清单和哈希，以下命令逐项核验：

```bash
uv run python scripts/check_vendor.py
```

Cython 生成文件、egg-info 和缓存属于构建产物。分发项目时须包含两个源码依赖或对应构建包。

## L3 扩展的复用边界

L3 保留每笔可见订单的身份和变化，主采集路径的 L2 按价格合计数量。L3 扩展复用本地日志 Journal、带进度的消费者 Consumer、本机进程间订阅 IPC、整数换算及 C 盘口；Kraken 队列校验码 CRC 和 Coinbase full 订单生命周期由本项目解析器实现。Cryptofeed 的 Coinbase 适配器面向 Advanced Trade，Kraken 适配器提供 L2；L3 原始 WS 由 aiohttp 接收。

Kraken SDK 审查参考提交：`btschwertfeger/python-kraken-sdk@04238ef35beb723d5b9aa1efd7d7f980b8a781c9`；运行依赖为 PyPI 3.5.1。

组件如何连接见[项目说明](README.md)，L3 解析与实验见 [L3 文档](docs/l3-market-data.md)。

行情数据适用各提供商及交易所条款。Coinbase 历史样本来源为 Tardis 月首接口；Kraken CRC 样例来源见 [SOURCES.md](tests/fixtures/SOURCES.md)。
