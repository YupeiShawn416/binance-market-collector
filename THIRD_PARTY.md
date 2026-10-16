# 开源组件与维护边界

本项目代码采用AGPL-3.0-or-later；根目录LICENSE为对应许可文本。以下记录源码许可与出处，不替具体分发场景作法律解释。

|组件|固定来源|复用内容|改动|
|---|---|---|---|
|Cryptofeed3.0.2|bmoscon/cryptofeed `bd0fe96093837262ce2c89ebc00286f48495fdad`，AGPL-3.0-or-later|实际Binance类的快照解析、L2更新、ID桥接及依赖类型|vendor原样保留；我们在binance.py继承覆盖严格门禁与raw trade|
|orderbook1.1.0|bmoscon/orderbook `349c340f3b06c20d69e3ef9f49f28f5acea75560`，GPL-3.0-or-later|C扩展有序盘口、整数/Decimal数据结构|vendor原样保留；零量删除和序列语义由小wrapper承担|
|msgspec0.22.0|锁定发布包，BSD-3-Clause|类型契约、MessagePack、JSON Schema|不修改|
|aiohttp3.14.5|锁定发布包，Apache-2.0 AND MIT|公开REST/WS、ping/pong、连接生命周期|不修改|
|python-kraken-sdk3.5.1（可选）|PyPI固定发布包，Apache-2.0；包哈希见uv.lock|SpotAsyncClient的nonce、HMAC、token请求、异常和连接清理|不修改；只调用GetWebSocketsToken。未复用其会转为float的WS回调，也未调用账户或交易端点|
|SQLite / Python标准库|运行环境SQLite、sqlite3、asyncio、fcntl、hashlib|WAL/事务/完整性、事件循环、文件锁和哈希|不自研数据库、网络栈或锁算法|

vendor去掉大体积上游测试行情、文档站点、Git对象和已编译产物，保留构建需要的原文件及许可。不是重新实现整个开源平台，也没有复制其整个账户/交易框架。实际构建依赖在uv.lock固定。

源码校验清单在third_party_manifest.json。运行`uv run python scripts/check_vendor.py`验证保留的文件没有被改写。生成的Cython C文件、egg-info、构建缓存不属于源码清单，打包时不带入。

L3适配层复用已有SQLite日志、Consumer持久游标、IPC帧、MessagePack校验、整数换算和C价格有序簿；Kraken队列CRC及Coinbase full订单生命周期是本项目的协议适配代码。现有Cryptofeed Coinbase是Advanced Trade适配器，不支持Exchange full/level3；Kraken适配器也只提供L2，未假装已复用到L3。SDK审查参考仓库为`btschwertfeger/python-kraken-sdk`，本次检出的commit为`04238ef35beb723d5b9aa1efd7d7f980b8a781c9`；实际依赖为锁定的PyPI3.5.1，不宣称此commit等同发布包。

行情数据来源和开源代码许可分开记录：Coinbase历史样本来自Tardis公开月首接口，适用提供商及交易所的数据使用条件；本地研究下载不等于获得再分发授权。Kraken官方CRC样例来源见`tests/fixtures/SOURCES.md`。
