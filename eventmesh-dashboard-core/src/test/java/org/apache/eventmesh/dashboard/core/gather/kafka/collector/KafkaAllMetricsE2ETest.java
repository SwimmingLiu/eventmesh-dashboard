/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.dashboard.core.gather.kafka.collector;

import org.apache.eventmesh.dashboard.core.gather.jmx.JmxConnectionConfig;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.BrokerMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.GroupMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCatalog;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricDefinition;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricDimension;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricFailure;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricSource;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricSupport;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricType;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.PartitionMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetricSample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Extracts every Kafka metric from a real cluster and writes the samples to CSV.
 */
public class KafkaAllMetricsE2ETest {

    private static final int EXPECTED_METRIC_DEFINITION_COUNT = 110;

    private static final String DEFAULT_ENDPOINTS = "1@127.0.0.1:9999,2@127.0.0.1:9998";

    @Test
    public void testExtractEveryMetricFromRealKafka() throws IOException {
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        List<KafkaBrokerJmxEndpoint> endpoints = parseEndpoints(
            System.getProperty("kafka.metrics.jmx.endpoints", DEFAULT_ENDPOINTS));
        Path output = Path.of(System.getProperty("kafka.metrics.output",
            "target/kafka-metrics/all-kafka-metrics.csv"));

        KafkaMetricCollection collection;
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrapServers))) {
            collection = new KafkaMetricsCollector().collect(admin, endpoints);
        }

        writeCsv(collection, output);
        assertChineseCsv(output);
        printAndAssertConsoleOutput(collection, output);
        assertEveryMetricWasExtracted(collection, output);
    }

    private void printAndAssertConsoleOutput(KafkaMetricCollection collection, Path output) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (PrintStream console = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
            printConsoleReport(collection, output, console);
        }
        String consoleOutput = buffer.toString(StandardCharsets.UTF_8);
        System.out.print(consoleOutput);
        Assertions.assertTrue(consoleOutput.contains("Kafka 指标采集汇总"), "控制台应输出中文汇总标题");
        Assertions.assertTrue(consoleOutput.contains("指标说明"), "控制台应逐条输出指标及其中文说明");
    }

    private void assertChineseCsv(Path output) throws IOException {
        String expectedHeader = "维度,实体,指标名称,指标类型,指标值,指标说明,数据来源,兼容状态,采集时间,采集状态,详情";
        List<String> rows = Files.readAllLines(output, StandardCharsets.UTF_8);
        String actualHeader = rows.get(0);
        Assertions.assertEquals(expectedHeader, actualHeader, "CSV 应包含中文表头和指标说明列");
        Assertions.assertTrue(rows.stream()
                .anyMatch(row -> row.contains("无 Leader 分区数量（PartitionNoLeader）")),
            "CSV 指标名称应采用“中文名称（英文名称）”格式");
        assertMetricsSortedByEnglishName(rows.subList(1, rows.size()));
    }

    private void assertMetricsSortedByEnglishName(List<String> rows) {
        Map<String, String> previousMetricByDimension = new HashMap<>();
        for (String row : rows) {
            String[] columns = row.split("\",\"", 4);
            String dimension = columns[0].substring(1);
            String displayName = columns[2];
            int englishNameStart = displayName.lastIndexOf('（');
            String englishName = displayName.substring(englishNameStart + 1, displayName.length() - 1);
            String previousMetric = previousMetricByDimension.put(dimension, englishName);
            Assertions.assertTrue(previousMetric == null || previousMetric.compareTo(englishName) <= 0,
                () -> dimension + "维度的指标未按英文名称升序排列：" + previousMetric + " 位于 " + englishName + " 之前");
        }
    }

    private List<KafkaBrokerJmxEndpoint> parseEndpoints(String configuredEndpoints) {
        long connectionTimeoutMillis = Long.getLong("kafka.metrics.jmx.connection.timeout.ms", 5_000L);
        long requestTimeoutMillis = Long.getLong("kafka.metrics.jmx.request.timeout.ms", 5_000L);
        List<KafkaBrokerJmxEndpoint> endpoints = new ArrayList<>();
        for (String configuredEndpoint : configuredEndpoints.split(",")) {
            String endpoint = configuredEndpoint.trim();
            int brokerSeparator = endpoint.indexOf('@');
            int portSeparator = endpoint.lastIndexOf(':');
            if (brokerSeparator < 1 || portSeparator <= brokerSeparator + 1 || portSeparator == endpoint.length() - 1) {
                throw new IllegalArgumentException("Invalid JMX endpoint '" + endpoint
                    + "'; expected brokerId@host:port");
            }
            int brokerId = Integer.parseInt(endpoint.substring(0, brokerSeparator));
            String host = endpoint.substring(brokerSeparator + 1, portSeparator);
            int port = Integer.parseInt(endpoint.substring(portSeparator + 1));
            JmxConnectionConfig connectionConfig = JmxConnectionConfig.builder(host, port)
                .connectionTimeoutMillis(connectionTimeoutMillis)
                .requestTimeoutMillis(requestTimeoutMillis)
                .build();
            endpoints.add(new KafkaBrokerJmxEndpoint(brokerId, connectionConfig));
        }
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("At least one Kafka Broker JMX endpoint must be configured");
        }
        return List.copyOf(endpoints);
    }

    private void assertEveryMetricWasExtracted(KafkaMetricCollection collection, Path output) {
        List<KafkaMetricDefinition> catalog = KafkaMetricCatalog.all();
        Assertions.assertEquals(EXPECTED_METRIC_DEFINITION_COUNT, catalog.size(),
            "Unexpected Kafka metric catalog size");
        Assertions.assertTrue(collection.failures().isEmpty(),
            "Metric extraction failures were written to " + output + ": " + collection.failures());
        Assertions.assertFalse(collection.clusters().isEmpty(), "No Cluster metrics were extracted");
        Assertions.assertFalse(collection.brokers().isEmpty(), "No Broker metrics were extracted");
        Assertions.assertFalse(collection.topics().isEmpty(), "No Topic metrics were extracted");
        Assertions.assertFalse(collection.partitions().isEmpty(), "No Partition metrics were extracted");
        Assertions.assertFalse(collection.groups().isEmpty(), "No Group metrics were extracted");
        Assertions.assertFalse(collection.replicas().isEmpty(), "No Replica metrics were extracted");

        Set<KafkaMetricDefinition> expected = new HashSet<>(catalog);
        Set<KafkaMetricDefinition> actual = extractedDefinitions(collection);
        expected.removeAll(actual);
        Assertions.assertTrue(expected.isEmpty(),
            "Missing Kafka metric definitions: " + expected + "; samples were written to " + output);
    }

    private Set<KafkaMetricDefinition> extractedDefinitions(KafkaMetricCollection collection) {
        Set<KafkaMetricDefinition> definitions = new HashSet<>();
        collection.clusters().forEach(sample -> definitions.add(sample.metric()));
        collection.brokers().forEach(sample -> definitions.add(sample.metric()));
        collection.topics().forEach(sample -> definitions.add(sample.metric()));
        collection.partitions().forEach(sample -> definitions.add(sample.metric()));
        collection.groups().forEach(sample -> definitions.add(sample.metric()));
        collection.replicas().forEach(sample -> definitions.add(sample.metric()));
        return definitions;
    }

    private void writeCsv(KafkaMetricCollection collection, Path output) throws IOException {
        List<String> rows = new ArrayList<>();
        rows.add("维度,实体,指标名称,指标类型,指标值,指标说明,数据来源,兼容状态,采集时间,采集状态,详情");
        collection.clusters().stream()
            .sorted(Comparator.comparing(sample -> sample.metric().metricName()))
            .forEach(sample -> rows.add(row(sample.metric(), "cluster", sample.value(), sample.collectedAt())));
        collection.brokers().stream()
            .sorted(Comparator.comparing((BrokerMetricSample sample) -> sample.metric().metricName())
                .thenComparing(BrokerMetricSample::broker))
            .forEach(sample -> rows.add(row(sample.metric(), sample.broker(), sample.value(), sample.collectedAt())));
        collection.topics().stream()
            .sorted(Comparator.comparing((TopicMetricSample sample) -> sample.metric().metricName())
                .thenComparing(TopicMetricSample::topic))
            .forEach(sample -> rows.add(row(sample.metric(), sample.topic(), sample.value(), sample.collectedAt())));
        collection.partitions().stream()
            .sorted(Comparator.comparing((PartitionMetricSample sample) -> sample.metric().metricName())
                .thenComparing(sample -> entity(sample.topicPartition())))
            .forEach(sample -> rows.add(row(sample.metric(), entity(sample.topicPartition()), sample.value(),
                sample.collectedAt())));
        collection.groups().stream()
            .sorted(Comparator.comparing((GroupMetricSample sample) -> sample.metric().metricName())
                .thenComparing(this::groupEntity))
            .forEach(sample -> rows.add(row(sample.metric(), groupEntity(sample), sample.value(), sample.collectedAt())));
        collection.replicas().stream()
            .sorted(Comparator.comparing((ReplicaMetricSample sample) -> sample.metric().metricName())
                .thenComparing(sample -> sample.broker() + "/" + entity(sample.topicPartition())))
            .forEach(sample -> rows.add(row(sample.metric(), sample.broker() + "/" + entity(sample.topicPartition()),
                sample.value(), sample.collectedAt())));
        collection.failures().stream()
            .sorted(Comparator.comparing(KafkaMetricFailure::metricName)
                .thenComparing(KafkaMetricFailure::objectName))
            .forEach(failure -> rows.add(failureRow(failure)));
        Path parent = output.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(output, rows, StandardCharsets.UTF_8);
    }

    private String row(KafkaMetricDefinition metric, String entity, BigDecimal value, Instant collectedAt) {
        return csv(dimensionName(metric.dimension()), entity, metricDisplayName(metric.metricName()),
            typeName(metric.type()), value.toPlainString(), description(metric), sourceName(metric.source()),
            supportName(KafkaMetricCatalog.support(metric)), collectedAt.toString(), "成功", "");
    }

    private String failureRow(KafkaMetricFailure failure) {
        return csv("失败记录", failure.objectName(), metricDisplayName(failure.metricName()), "", "",
            "该指标在本次采集周期中提取失败。", "", "", "", "失败", failure.reason());
    }

    private String metricDisplayName(String metricName) {
        return metricMeaning(metricName) + "（" + metricName + "）";
    }

    private String description(KafkaMetricDefinition metric) {
        String meaning = metricMeaning(metric.metricName());
        char first = meaning.charAt(0);
        String separator = first <= 127 && Character.isLetterOrDigit(first) ? " " : "";
        return dimensionDescription(metric.dimension()) + "的" + separator + meaning + "，数据来自 "
            + sourceName(metric.source()) + "。";
    }

    private String dimensionDescription(KafkaMetricDimension dimension) {
        return switch (dimension) {
            case CLUSTER -> "集群维度";
            case BROKER -> "Broker 维度";
            case TOPIC -> "Topic 维度";
            case PARTITION -> "分区维度";
            case GROUP -> "消费组维度";
            case REPLICA -> "副本维度";
        };
    }

    private String metricMeaning(String metricName) {
        return switch (metricName) {
            case "Alive" -> "节点存活状态";
            case "HealthState", "HealthState_Cluster" -> "健康状态";
            case "HealthCheckPassed", "HealthCheckPassed_Cluster" -> "健康检查通过数量";
            case "HealthCheckTotal", "HealthCheckTotal_Cluster" -> "健康检查总数";
            case "HealthState_Topics" -> "Topic 健康状态";
            case "HealthCheckPassed_Topics" -> "Topic 健康检查通过数量";
            case "HealthCheckTotal_Topics" -> "Topic 健康检查总数";
            case "HealthState_Brokers" -> "Broker 健康状态";
            case "HealthCheckPassed_Brokers" -> "Broker 健康检查通过数量";
            case "HealthCheckTotal_Brokers" -> "Broker 健康检查总数";
            case "HealthState_Groups" -> "消费组健康状态";
            case "HealthCheckPassed_Groups" -> "消费组健康检查通过数量";
            case "HealthCheckTotal_Groups" -> "消费组健康检查总数";
            case "Brokers" -> "Broker 总数";
            case "Zookeepers" -> "ZooKeeper 节点总数";
            case "PartitionNoLeader" -> "无 Leader 分区数量";
            case "LeaderMessages" -> "Leader 副本消息总数";
            case "GroupActives" -> "活跃消费组数量";
            case "GroupEmptys" -> "空消费组数量";
            case "GroupRebalances" -> "正在重平衡的消费组数量";
            case "GroupDeads" -> "失效消费组数量";
            case "Partitions" -> "分区数量";
            case "Leaders" -> "Leader 分区数量";
            case "ActiveControllerCount" -> "活跃 Controller 数量";
            case "EventQueueSize" -> "Controller 事件队列长度";
            case "TotalRequestQueueSize" -> "请求队列总长度";
            case "TotalResponseQueueSize" -> "响应队列总长度";
            case "TotalProduceRequests" -> "生产请求速率";
            case "LoadReBalanceEnable" -> "负载均衡启用状态";
            case "LoadReBalanceNwIn" -> "负载均衡网络流入状态";
            case "LoadReBalanceNwOut" -> "负载均衡网络流出状态";
            case "LoadReBalanceDisk" -> "负载均衡磁盘状态";
            case "NetworkProcessorAvgIdle" -> "网络处理线程平均空闲率";
            case "RequestHandlerAvgIdle" -> "请求处理线程平均空闲率";
            case "PartitionsSkew" -> "分区数量倾斜率";
            case "LeadersSkew" -> "Leader 数量倾斜率";
            case "BytesIn" -> "每分钟平均流入字节速率";
            case "BytesIn_min_5" -> "5 分钟平均流入字节速率";
            case "BytesIn_min_15" -> "15 分钟平均流入字节速率";
            case "BytesOut" -> "每分钟平均流出字节速率";
            case "BytesOut_min_5" -> "5 分钟平均流出字节速率";
            case "BytesOut_min_15" -> "15 分钟平均流出字节速率";
            case "ReplicationBytesIn" -> "副本复制流入字节速率";
            case "ReplicationBytesOut" -> "副本复制流出字节速率";
            case "ReassignmentBytesIn" -> "分区迁移流入字节速率";
            case "ReassignmentBytesOut" -> "分区迁移流出字节速率";
            case "BytesRejected" -> "拒绝字节速率";
            case "FailedFetchRequests" -> "失败拉取请求速率";
            case "FailedProduceRequests" -> "失败生产请求速率";
            case "MirrorFetchLag" -> "镜像拉取延迟";
            case "MessagesIn", "Messages" -> "消息数量";
            case "LogSize", "TotalLogSize" -> "日志数据大小";
            case "PartitionURP" -> "未充分复制的分区数量";
            case "PartitionMinISR_S" -> "低于最小 ISR 要求的分区数量";
            case "PartitionMinISR_E" -> "等于最小 ISR 要求的分区数量";
            case "ConnectionsCount" -> "当前连接数量";
            case "OffsetConsumed" -> "消费组已提交位点";
            case "LogEndOffset" -> "日志结束位点";
            case "LogStartOffset" -> "日志起始位点";
            case "Lag" -> "消费积压量";
            case "InSync" -> "副本是否位于 ISR 集合";
            case "State" -> "消费组状态";
            case "JmxConnection" -> "JMX 连接状态";
            default -> "Kafka 指标";
        };
    }

    private String dimensionName(KafkaMetricDimension dimension) {
        return switch (dimension) {
            case CLUSTER -> "集群";
            case BROKER -> "Broker";
            case TOPIC -> "Topic";
            case PARTITION -> "分区";
            case GROUP -> "消费组";
            case REPLICA -> "副本";
        };
    }

    private String typeName(KafkaMetricType type) {
        return switch (type) {
            case RATE -> "速率";
            case GAUGE -> "瞬时值";
            case COUNT -> "数量";
            case RATIO -> "比率";
            case OFFSET -> "位点";
            case STATE -> "状态";
        };
    }

    private String sourceName(KafkaMetricSource source) {
        return switch (source) {
            case JMX -> "Kafka JMX";
            case ADMIN_CLIENT -> "Kafka AdminClient";
            case DERIVED -> "采集结果计算";
            case HEALTH_CHECK -> "健康检查";
        };
    }

    private String supportName(KafkaMetricSupport support) {
        return switch (support) {
            case STANDARD -> "标准支持";
            case LEGACY_JMX_COMPATIBILITY -> "旧版 JMX 兼容";
            case CUSTOM_KAFKA_COMPATIBILITY -> "定制 Kafka 兼容";
            case ENTERPRISE_COMPATIBILITY -> "企业版兼容";
        };
    }

    private String groupEntity(GroupMetricSample sample) {
        return sample.group() + sample.topicPartition().map(partition -> "/" + entity(partition)).orElse("");
    }

    private String entity(TopicPartition partition) {
        return partition.topic() + "-" + partition.partition();
    }

    private String csv(String... values) {
        List<String> escaped = new ArrayList<>(values.length);
        for (String value : values) {
            escaped.add('"' + value.replace("\"", "\"\"") + '"');
        }
        return String.join(",", escaped);
    }

    private void printConsoleReport(KafkaMetricCollection collection, Path output, PrintStream console) {
        console.println("=== Kafka 指标采集汇总 ===");
        console.printf("总样本数=%d | 集群=%d | Broker=%d | Topic=%d | 分区=%d | 消费组=%d | 副本=%d | 失败=%d%n",
            collection.sampleCount(), collection.clusters().size(), collection.brokers().size(),
            collection.topics().size(), collection.partitions().size(), collection.groups().size(),
            collection.replicas().size(), collection.failures().size());
        console.println("CSV 文件=" + output.toAbsolutePath());
        console.println("=== Kafka 指标明细（指标说明） ===");
        collection.clusters().forEach(sample -> printSample(console, sample.metric(), "cluster", sample.value(),
            sample.collectedAt()));
        collection.brokers().forEach(sample -> printSample(console, sample.metric(), sample.broker(), sample.value(),
            sample.collectedAt()));
        collection.topics().forEach(sample -> printSample(console, sample.metric(), sample.topic(), sample.value(),
            sample.collectedAt()));
        collection.partitions().forEach(sample -> printSample(console, sample.metric(), entity(sample.topicPartition()),
            sample.value(), sample.collectedAt()));
        collection.groups().forEach(sample -> printSample(console, sample.metric(), groupEntity(sample), sample.value(),
            sample.collectedAt()));
        collection.replicas().forEach(sample -> printSample(console, sample.metric(),
            sample.broker() + "/" + entity(sample.topicPartition()), sample.value(), sample.collectedAt()));
        collection.failures().forEach(failure -> console.printf(
            "维度=失败记录 | 实体=%s | 指标名称=%s | 指标说明=该指标在本次采集周期中提取失败。 | 采集状态=失败 | 详情=%s%n",
            failure.objectName(), failure.metricName(), failure.reason()));
    }

    private void printSample(PrintStream console, KafkaMetricDefinition metric, String entity, BigDecimal value,
        Instant collectedAt) {
        console.printf(
            "维度=%s | 实体=%s | 指标名称=%s | 指标说明=%s | 指标类型=%s | 数据来源=%s | 兼容状态=%s | 指标值=%s"
                + " | 采集时间=%s | 采集状态=成功%n",
            dimensionName(metric.dimension()), entity, metricDisplayName(metric.metricName()), description(metric),
            typeName(metric.type()), sourceName(metric.source()), supportName(KafkaMetricCatalog.support(metric)),
            value.toPlainString(), collectedAt);
    }
}
