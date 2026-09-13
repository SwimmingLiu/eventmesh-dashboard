# Kafka 指标采集清单

## 范围与依据

本文记录 Kafka 运维监控和报表需要保存的指标、指标口径、建议数据源，以及 EventMesh Dashboard 当前主分支的实现状态。

EventMesh Dashboard 的代码基线是远程 `origin/main` 最后一次提交 [`b534b99c2d5e2ad4f529ec61fad0076bdffaab6b`](https://github.com/SwimmingLiu/eventmesh-dashboard/tree/b534b99c2d5e2ad4f529ec61fad0076bdffaab6b)。工作树中的未提交和新增代码不属于本文基线。

指标定义来自：

- 飞书 Wiki：[【指标】Kafka 可采集的指标](https://my.feishu.cn/wiki/BMSCwpc2EiHmgjk2su5c2iRHnTf)。该页面定义了副本指标、JMX 指标组、指标类型和采集口径。
- KnowStreaming 当前源码：[指标说明](https://github.com/didi/KnowStreaming/blob/33524eaaaf8d539cb71f36a36e2b47688fff7591/docs/dev_guide/%E6%8C%87%E6%A0%87%E8%AF%B4%E6%98%8E.md)、[BrokerMetricVersionItems](https://github.com/didi/KnowStreaming/blob/33524eaaaf8d539cb71f36a36e2b47688fff7591/km-core/src/main/java/com/xiaojukeji/know/streaming/km/core/service/version/metrics/kafka/BrokerMetricVersionItems.java)、[ClusterMetricVersionItems](https://github.com/didi/KnowStreaming/blob/33524eaaaf8d539cb71f36a36e2b47688fff7591/km-core/src/main/java/com/xiaojukeji/know/streaming/km/core/service/version/metrics/kafka/ClusterMetricVersionItems.java)、[TopicMetricVersionItems](https://github.com/didi/KnowStreaming/blob/33524eaaaf8d539cb71f36a36e2b47688fff7591/km-core/src/main/java/com/xiaojukeji/know/streaming/km/core/service/version/metrics/kafka/TopicMetricVersionItems.java)、[PartitionMetricVersionItems](https://github.com/didi/KnowStreaming/blob/33524eaaaf8d539cb71f36a36e2b47688fff7591/km-core/src/main/java/com/xiaojukeji/know/streaming/km/core/service/version/metrics/kafka/PartitionMetricVersionItems.java)、[GroupMetricVersionItems](https://github.com/didi/KnowStreaming/blob/33524eaaaf8d539cb71f36a36e2b47688fff7591/km-core/src/main/java/com/xiaojukeji/know/streaming/km/core/service/version/metrics/kafka/GroupMetricVersionItems.java)、[ReplicaMetricVersionItems](https://github.com/didi/KnowStreaming/blob/33524eaaaf8d539cb71f36a36e2b47688fff7591/km-core/src/main/java/com/xiaojukeji/know/streaming/km/core/service/version/metrics/kafka/ReplicaMetricVersionItems.java)。
- KnowStreaming 的 JMX 对象名：[JmxName.java](https://github.com/didi/KnowStreaming/blob/33524eaaaf8d539cb71f36a36e2b47688fff7591/km-common/src/main/java/com/xiaojukeji/know/streaming/km/common/jmx/JmxName.java)。

## EventMesh Dashboard `main` 分支现状

基于上述提交，Kafka 指标目前没有形成可工作的采集和存储链路，正常启动下实际可落库的 Kafka 指标为 **0 个**。

| 项目 | `main` 分支状态 |
|---|---|
| Kafka 指标模型 | `report/model` 下只有 RocketMQ 模型，没有 Kafka 模型和 `kafka_*` 报表名 |
| Kafka 指标表 | [`report-RocketMQ.sql`](https://github.com/SwimmingLiu/eventmesh-dashboard/blob/b534b99c2d5e2ad4f529ec61fad0076bdffaab6b/eventmesh-dashboard-console/src/main/resources/report/report-RocketMQ.sql) 只创建 RocketMQ 表 |
| Prometheus 采集 | [`CollectExporter`](https://github.com/SwimmingLiu/eventmesh-dashboard/blob/b534b99c2d5e2ad4f529ec61fad0076bdffaab6b/eventmesh-dashboard-console/src/main/java/org/apache/eventmesh/dashboard/console/function/report/collect/exporter/CollectExporter.java) 可以发起 HTTP GET，但只能根据已注册模型构造样本，未知指标会被丢弃 |
| Exporter 注册 | [`CollectManage`](https://github.com/SwimmingLiu/eventmesh-dashboard/blob/b534b99c2d5e2ad4f529ec61fad0076bdffaab6b/eventmesh-dashboard-console/src/main/java/org/apache/eventmesh/dashboard/console/function/report/collect/CollectManage.java) 的注册入口没有正常调用方；运行时和 `collect` 表读取仍是占位代码 |
| Kafka AdminClient | [`KafkaAdminOperation`](https://github.com/SwimmingLiu/eventmesh-dashboard/blob/b534b99c2d5e2ad4f529ec61fad0076bdffaab6b/eventmesh-dashboard-core/src/main/java/org/apache/eventmesh/dashboard/core/function/SDK/operation/kafka/KafkaAdminOperation.java) 用空 `Properties` 创建客户端，没有连接参数和指标读取逻辑 |
| Kafka JMX | [`JmxConnector`](https://github.com/SwimmingLiu/eventmesh-dashboard/blob/b534b99c2d5e2ad4f529ec61fad0076bdffaab6b/eventmesh-dashboard-core/src/main/java/org/apache/eventmesh/dashboard/core/gather/jmx/JmxConnector.java) 的连接实现全部处于注释中 |
| 默认配置 | [`application-dev.yml`](https://github.com/SwimmingLiu/eventmesh-dashboard/blob/b534b99c2d5e2ad4f529ec61fad0076bdffaab6b/eventmesh-dashboard-console/src/main/resources/application-dev.yml) 默认关闭 `function`，没有 Kafka/JMX endpoint 配置 |

通用 Exporter 链路本身也还不能作为 Kafka 采集方案：普通 Prometheus 样本的 `value` 没有写入对象，报表处理阶段又只保存了时间字段。因此需要先补齐模型、采集注册、数值写入和存储逻辑，再接入 Kafka 指标。

## 指标字典

下表中的 P0 表示 Kafka 运行状态、积压和故障判断所依赖的数据；P1 表示由 P0 原始数据聚合或按产品范围启用的指标。

### Replica 和 Partition

| 优先级 | 指标 | 类型/单位 | 维度和采集口径 | 建议来源 |
|---|---|---|---|---|
| P0 | `LogStartOffset` | Offset | Replica 的日志起始 offset；Partition 口径为 Leader 副本 | Replica JMX `kafka.log:type=Log,name=LogStartOffset` 或 AdminClient |
| P0 | `LogEndOffset` | Offset | Replica 的日志结束 offset；Partition 口径为 Leader 副本 | Replica JMX `kafka.log:type=Log,name=LogEndOffset` 或 AdminClient `ListOffsets` |
| P0 | `Messages` | Count | `LogEndOffset - LogStartOffset`；查询时应保留 offset 回退或清理导致的边界处理 | 由 offset 计算 |
| P0 | `LogSize` | Gauge，byte | 指定 Replica、Partition 或 Topic 的日志大小 | `kafka.log:type=Log,name=Size`；Kafka ≥ 1.0 可用 AdminClient `LogDirDescription` |
| P0 | `InSync` | State | 指定 Replica 是否在 ISR 中，通常用 1/0 表示 | Partition 元数据的 ISR 列表 |
| P0 | `BytesIn`、`BytesOut` | Rate，byte/s | Partition 级没有 Kafka 原生精确流量 MBean；KnowStreaming 按 Topic JMX 流量和 Leader 分区数分摊，是估算值 | Broker `BrokerTopicMetrics` + Leader 元数据 |

Replica 维度应至少包含 `clusterId`、`topic`、`partition`、`brokerId`。飞书文档和 KnowStreaming 都把 Replica 指标作为按需查询数据；如果要形成历史曲线，需要明确额外的定时采集策略。

### Consumer Group

| 优先级 | 指标 | 类型/单位 | 维度和采集口径 | 建议来源 |
|---|---|---|---|---|
| P0 | `OffsetConsumed` | Offset | Consumer 已提交的 offset | AdminClient committed offsets |
| P0 | `LogEndOffset` | Offset | Group 对应 Topic/Partition 的最新 offset | AdminClient `ListOffsets` |
| P0 | `Lag` | Count，条 | `max(0, LogEndOffset - OffsetConsumed)`；未知值不能解释为 0 | AdminClient 计算 |
| P0 | `State` | State | Consumer Group 状态 | AdminClient Group 描述或状态存储 |
| P1 | `HealthState`、`HealthCheckPassed`、`HealthCheckTotal` | State/Count | Group 健康状态及检查计数 | 健康检查服务派生 |

Group 时序应支持两种粒度：Group 汇总，以及 `group + topic + partition` 明细。这样既能展示总积压，也能定位具体分区。

### Broker

| 优先级 | 指标 | 类型/单位 | JMX 对象或计算口径 |
|---|---|---|---|
| P0 | `MessagesIn`、`TotalProduceRequests` | Rate，条/s 或个/s | `kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec/TotalProduceRequestsPerSec`；Meter 取 `OneMinuteRate` |
| P0 | `BytesIn`、`BytesOut` | Rate，byte/s | `BrokerTopicMetrics` 的 `BytesInPerSec/BytesOutPerSec`；需要时保存 1、5、15 分钟速率 |
| P0 | `BytesRejected`、`FailedFetchRequests`、`FailedProduceRequests` | Rate，个/s 或 byte/s | `BrokerTopicMetrics` 对应拒绝和失败请求 Meter；以目标 Kafka 暴露的单位为准 |
| P0 | `ReplicationBytesIn`、`ReplicationBytesOut` | Rate，byte/s | `BrokerTopicMetrics` 的复制流量 |
| P0 | `ReassignmentBytesIn`、`ReassignmentBytesOut` | Rate，byte/s | `BrokerTopicMetrics` 的迁移流量 |
| P0 | `TotalRequestQueueSize`、`TotalResponseQueueSize` | Gauge，个 | `kafka.network:type=RequestChannel,name=RequestQueueSize/ResponseQueueSize` 的 `Value` |
| P0 | `NetworkProcessorAvgIdle` | Ratio，% | `kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent` |
| P0 | `RequestHandlerAvgIdle` | Ratio，% | `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent`；飞书按 Gauge 处理，KnowStreaming 源码绑定了 1 分钟属性，接入时需按目标版本验证 |
| P0 | `ConnectionsCount` | Gauge，个 | `kafka.server:type=socket-server-metrics,listener=*,networkProcessor=*` 的 `connection-count` 累加 |
| P0 | `Partitions`、`Leaders` | Count，个 | `kafka.server:type=ReplicaManager,name=PartitionCount/LeaderCount` |
| P0 | `PartitionURP` | Count，个 | `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` |
| P0 | `PartitionMinISR_S`、`PartitionMinISR_E` | Count，个 | `UnderMinIsrPartitionCount` 和 `AtMinIsrPartitionCount` |
| P0 | `ActiveControllerCount` | Gauge，个 | `kafka.controller:type=KafkaController,name=ActiveControllerCount` |
| P0 | `LogSize` | Gauge，byte | Broker 所属本地副本日志大小之和 |
| P1 | `PartitionsSkew`、`LeadersSkew` | Ratio，% | 相对集群平均值的偏差 |
| P1 | `Alive` | State | Broker 是否存活，通常由 Broker 元数据或健康检查判断 |
| P1 | `HealthState`、`HealthCheckPassed`、`HealthCheckTotal` | State/Count | Broker 健康状态及检查计数 |

Broker 指标的主键应包含 `clusterId` 和 `brokerId`，在 EventMesh 中可以映射到已有 Runtime 身份。

### Topic 和 Cluster

| 优先级 | 层级 | 指标 | 采集或计算口径 |
|---|---|---|---|
| P1 | Topic | `TotalProduceRequests`、`BytesRejected`、`FailedFetchRequests`、`FailedProduceRequests` | 按 Topic 标签读取存活 Broker 的 JMX 后求和 |
| P1 | Topic | `ReplicationCount`、`Messages`、`LogSize` | 分别由副本数、各 Partition `Messages`、各副本日志大小聚合 |
| P1 | Topic | `MessagesIn`、`BytesIn`、`BytesOut` | 按 Topic 保存 1/5/15 分钟速率；不要把 Broker 总量直接复制为 Topic 值 |
| P1 | Topic | `PartitionURP` | `kafka.cluster:type=Partition,name=UnderReplicated` 按 Topic/Partition 汇总 |
| P1 | Topic | `MirrorFetchLag` | `kafka.server:type=FetcherLagMetrics,name=ConsumerLag`；仅适用于对应 MirrorMaker/Kafka 版本 |
| P1 | Cluster | `Brokers`、`BrokersAlive`、`BrokersNotAlive`、`Replicas`、`Topics`、`Partitions` | AdminClient 和元数据统计 |
| P1 | Cluster | `PartitionNoLeader`、`PartitionMinISR_S`、`PartitionMinISR_E`、`PartitionURP` | 元数据或存活 Broker JMX 聚合；`PartitionNoLeader` 与 `OfflinePartitionsCount` 语义接近但名称不同 |
| P1 | Cluster | `MessagesIn`、`LeaderMessages`、`BytesIn`、`BytesOut`、`TotalLogSize` | Broker/Topic/Partition 原始数据聚合；`LeaderMessages` 是各分区保留消息数之和 |
| P1 | Cluster | `TotalRequestQueueSize`、`TotalResponseQueueSize`、`EventQueueSize`、`ActiveControllerCount`、`ConnectionsCount` | 存活 Broker 或 Controller JMX 聚合；`EventQueueSize` 仅 Kafka ≥ 2.0 |
| P1 | Cluster | `Groups`、`GroupActives`、`GroupEmptys`、`GroupRebalances`、`GroupDeads` | Consumer Group 状态统计 |
| P1 | Cluster | `HealthState*`、`HealthCheckPassed*`、`HealthCheckTotal*` | Cluster、Topics、Brokers、Groups、Connector、MirrorMaker 的健康派生指标 |

KnowStreaming 源码还定义了 ACL、任务状态和 ZooKeeper 指标。ACL/任务属于平台治理数据；ZooKeeper 会话、连接和选举状态只应在 Kafka ZK 模式下作为只读诊断采集。CPU、主机磁盘、主机网络应由 Node Exporter、主机 Agent 或云监控负责。

## 采集口径和版本约束

1. **Rate 和 Count 分开保存。** `MessagesIn`、`BytesIn/Out`、请求数和复制流量需要保存速率；offset、消息数、异常分区数和连接数属于当前值或累计值。KnowStreaming 的 Meter 通常使用 `OneMinuteRate`、`FiveMinuteRate`、`FifteenMinuteRate`，Gauge 使用 `Value`。
2. **未知值不等于零。** Consumer offset 或 Lag 返回 `-1` 时应保留未知状态，聚合时不能把它当成有效零值。
3. **JMX 连接数要查询通配对象。** `socket-server-metrics` 通常需要遍历 listener 和 networkProcessor，再累加 `connection-count`。
4. **Topic/Partition 流量要标注估算属性。** Kafka 没有原生 Partition 流量计数器，KnowStreaming 的 Partition `BytesIn/Out` 是按 Topic 流量分摊的估算值。
5. **保留版本分支。** `EventQueueSize` 从 Kafka 2.0 起可用；Broker `LogSize` 在 Kafka 1.0 前后使用不同采集方式；`MirrorFetchLag` 仅适用于特定定制 Kafka 版本；健康指标在 KnowStreaming 源码中使用 `HealthState`，旧文档中可能仍出现 `HealthScore`。
6. **维度要统一。** 建议使用 `clusterId`、`brokerId/runtimeId`、`topic`、`partition`、`group` 作为标签或主键。Cluster、Broker、Topic、Partition、Group 和 Replica 不应混用同一粒度的数值。

## 实施顺序

1. 增加 Kafka 指标模型、IoTDB 表结构和指标发现接口，先覆盖 Replica、Partition、Group、Broker 的 P0 原始指标。
2. 为 AdminClient 增加 `bootstrap.servers`、SASL/TLS 和超时配置，实现 offset、ISR、Group 状态、Topic/Partition 元数据读取。
3. 选择直接 JMX 或 JMX Exporter。若使用 JMX Exporter，为每个 Broker 配置 endpoint，并把 JMX ObjectName、属性和 Kafka 版本映射写成明确规则。
4. 在原始事实之上计算 Topic/Cluster 聚合，区分原始样本、滑动速率和聚合结果，避免重复汇总。
5. 最后接入健康、ZooKeeper、ACL、任务、Connect/MM2 等产品扩展指标，并为高基数集群评估采集周期、响应体大小和时序存储容量。
