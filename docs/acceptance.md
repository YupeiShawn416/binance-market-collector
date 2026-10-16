# 行情服务验收记录

日期：2026-10-15。本轮实现并验收的是作者架构的行情服务；决策/交易仅接入行情消费示例，未实现模型、账户或下单。

## 实际公开行情

- 官方公开市场数据端点，Binance BTC-USDT spot，未用API key。
- 采集 30.232 秒，4011 条原始记录；4019 条规范事件。
- Snapshot 2；Delta批 264；原始Trade 3735；Quality 17；Instrument 1。
- 中途主动断开1次，实际重连 1 次，重新快照/桥接。
- 决策/交易两个本机socket消费者的完整事件与盘口哈希序列相同；决策消费者在seq=25后关闭并从磁盘恢复，拼接序列无重复/遗漏。
- 采集原生簿与规范簿逐批一致，完整历史回放的事件和每批盘口哈希与采集端一致；未处理原始尾部0条。
- final_book_hash：`bf054cd143c8777877cb5fb107cf0b6be7ce33af97f5e18d28bec20be1cd7801`。
- final_event_chain：`23f4c52c625d0ad5f6df593a93b7ca6170d986899d70844bc3626eb5ebe1d116`。

最终样本带有主动停止后的DISCONNECTED，属于录制事实，不把历史末状态当作正在工作的实时服务。

主动重连期间检测到的公开成交缺口为 `trade_id_gap:6763422659..6763422770`。它已保留为显式Quality边界，未自动补齐，也未把缺口两侧认定为连续maker队列。这证明服务识别了丢失，不证明公开流在断线期间没有丢失。

## 自动化回归

本轮最终pytest：29项通过，详见 [终端输出](../evidence/tests.txt)。涵盖：

- 原生C盘口与规范事件、双消费者、逐事件/盘口哈希、精度和空档；
- 序列缺段、旧消息、重复/冲突Trade、trade ID gap、crossed/locked与不对齐；
- 重连换epoch、SYNCING桥接、重启不能把上一连接旧盘口重新标LIVE；
- raw未处理尾部、checkpoint+tail、消费者完整状态/cursor、dataset隔离、单写者；
- 原始/规范checksum、严格schema/范围/版本；
- socket追尾、消费中断续读、残留socket恢复及不替换活动监听者；
- 独立写者进程在raw已提交、规范事务COMMIT前、COMMIT后被SIGKILL，恢复后Trade不重不漏。

SIGKILL覆盖软件进程中止，没有把它说成物理掉电或任意存储硬件故障测试。CI工作流已配置为离线契约/恢复测试，本地已跑；尚未在远端GitHub运行。真实网络验收为显式脚本，不混进日常离线pytest。

## 可安装与可维护性

独立venv从uv.lock和两个固定vendor源码实际构建成功；没有用旧审查venv冒充最终环境。Ruff检查、格式检查与82个上游源文件校验通过。vendor原文件没有改写，适配只在本项目的StrictBinance子类。

[事件schema](event-v1.schema.json)、[契约语义](event-contract.md)、[源码来源](../THIRD_PARTY.md)、[真实验收JSON](../evidence/live-verification.json)。

## 性能观察范围

这段数据的应用读帧→规范化耗时 p50=2.723ms，p99=33.584ms，最大=250.675ms。包含缓冲/快照等待与全量簿校验成本，不含规范事件提交、socket/网络/模型。它是一次实际样本，不是负载基准、饱和吞吐或SLO。

当前首版为Python编排+C扩展盘口；没有为了图上“全部Rust”重写已有成熟组件。后续只有测到瓶颈后再替换热路径，实现公共契约和相同验收夹具。
