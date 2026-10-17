# 开源组件

项目许可证：AGPL-3.0-or-later，见 [LICENSE](LICENSE)。依赖版本与包哈希固定在 `uv.lock`。

|组件|来源与许可|复用内容|
|---|---|---|
|Cryptofeed 3.0.2|`bmoscon/cryptofeed@bd0fe96093837262ce2c89ebc00286f48495fdad`，AGPL-3.0-or-later|Binance 快照解析、L2 更新与序列桥接；严格门禁和 raw trade 位于本项目子类|
|orderbook 1.1.0|`bmoscon/orderbook@349c340f3b06c20d69e3ef9f49f28f5acea75560`，GPL-3.0-or-later|C 扩展有序盘口；包装层处理整数单位、零量删除和批次语义|
|msgspec 0.22.0|发布包，BSD-3-Clause|冻结契约、MessagePack、JSON Schema|
|aiohttp 3.14.5|发布包，Apache-2.0 AND MIT|REST / WS、ping/pong、连接生命周期|
|python-kraken-sdk 3.5.1，可选|PyPI，Apache-2.0|SpotAsyncClient 的签名、nonce 和 GetWebSocketsToken|
|Plotly 6.9.0，可选|发布包，MIT|离线交互热力图、缩放、图例与图片导出|
|Matplotlib 3.11.2，可选|发布包，附上游许可证|Agg PNG 渲染|
|SQLite / Python 标准库|运行环境|事务、WAL、备份、事件循环、文件锁和哈希|

## 固定源码

`vendor/` 保留构建源文件和许可证，排除上游测试行情、文档站点、Git 对象及编译产物。原始源文件按 `third_party_manifest.json` 校验：

```bash
uv run python scripts/check_vendor.py
```

Cython 生成文件、egg-info 和缓存属于构建产物。分发项目时须包含两个源码依赖或对应构建包。

## L3 适配

L3 复用 Journal、Consumer、IPC、整数换算及 C 盘口；Kraken 队列 CRC 和 Coinbase full 生命周期由本项目实现。Cryptofeed 的 Coinbase 适配器面向 Advanced Trade，Kraken 适配器提供 L2；L3 原始 WS 由 aiohttp 接收。

Kraken SDK 审查参考提交：`btschwertfeger/python-kraken-sdk@04238ef35beb723d5b9aa1efd7d7f980b8a781c9`；运行依赖为 PyPI 3.5.1。

行情数据适用各提供商及交易所条款。Coinbase 历史样本来源为 Tardis 月首接口；Kraken CRC 样例来源见 [SOURCES.md](tests/fixtures/SOURCES.md)。
