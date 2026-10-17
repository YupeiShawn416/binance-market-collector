# 协议样例来源

本目录保存协议测试使用的固定输入，来源记录用于核查夹具与预期结果。

`kraken-official-l3-snapshot.json` 来自 Kraken 官方 L3 快照校验样例，包含35个可见订单。测试按协议将订单价格和数量组成校验输入，再计算 CRC32，预期结果为1063832831。

来源：[Kraken L3 校验规则](https://docs.kraken.com/exchange/guides/websockets/l3-checksum-v2)

核查日期：2026-10-16。测试代码自行构造的时间序列标记为 synthetic，用于覆盖缺段、重连等处理规则。
