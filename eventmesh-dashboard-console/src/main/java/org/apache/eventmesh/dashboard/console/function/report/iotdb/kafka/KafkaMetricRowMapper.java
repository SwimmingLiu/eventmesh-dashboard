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

package org.apache.eventmesh.dashboard.console.function.report.iotdb.kafka;

import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaMetricWriteBatch;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.BrokerMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ClusterMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.GroupMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricDefinition;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricFailure;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.PartitionMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetricSample;

import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.Value;
import lombok.experimental.Accessors;

final class KafkaMetricRowMapper {

    List<KafkaMetricRow> map(KafkaMetricWriteBatch batch) {
        Map<RowKey, MutableRow> rows = new LinkedHashMap<>();
        KafkaMetricCollection metrics = batch.metrics();
        metrics.clusters().forEach(sample -> addCluster(rows, batch, sample));
        metrics.brokers().forEach(sample -> addBroker(rows, batch, sample));
        metrics.topics().forEach(sample -> addTopic(rows, batch, sample));
        metrics.partitions().forEach(sample -> addPartition(rows, batch, sample));
        metrics.groups().forEach(sample -> addGroup(rows, batch, sample));
        metrics.replicas().forEach(sample -> addReplica(rows, batch, sample));
        addCollectionRun(rows, batch);
        addFailures(rows, batch);
        return rows.values().stream().map(MutableRow::build).toList();
    }

    private void addCluster(Map<RowKey, MutableRow> rows, KafkaMetricWriteBatch batch,
        ClusterMetricSample sample) {
        addMetric(rows, KafkaIotDBSchema.CLUSTER, batch, sample.collectedAt(), Map.of(), sample.metric(),
            sample.value());
    }

    private void addBroker(Map<RowKey, MutableRow> rows, KafkaMetricWriteBatch batch, BrokerMetricSample sample) {
        addMetric(rows, KafkaIotDBSchema.BROKER, batch, sample.collectedAt(), Map.of("broker_id", sample.broker()),
            sample.metric(), sample.value());
    }

    private void addTopic(Map<RowKey, MutableRow> rows, KafkaMetricWriteBatch batch, TopicMetricSample sample) {
        addMetric(rows, KafkaIotDBSchema.TOPIC, batch, sample.collectedAt(), Map.of("topic", sample.topic()),
            sample.metric(), sample.value());
    }

    private void addPartition(Map<RowKey, MutableRow> rows, KafkaMetricWriteBatch batch,
        PartitionMetricSample sample) {
        addMetric(rows, KafkaIotDBSchema.PARTITION, batch, sample.collectedAt(),
            partitionTags(sample.topicPartition()), sample.metric(), sample.value());
    }

    private void addGroup(Map<RowKey, MutableRow> rows, KafkaMetricWriteBatch batch, GroupMetricSample sample) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("group_id", sample.group());
        KafkaIotDBSchema.TableDefinition table = KafkaIotDBSchema.GROUP;
        if (sample.topicPartition().isPresent()) {
            table = KafkaIotDBSchema.GROUP_PARTITION;
            tags.putAll(partitionTags(sample.topicPartition().orElseThrow()));
        }
        addMetric(rows, table, batch, sample.collectedAt(), tags, sample.metric(), sample.value());
    }

    private void addReplica(Map<RowKey, MutableRow> rows, KafkaMetricWriteBatch batch,
        ReplicaMetricSample sample) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("broker_id", sample.broker());
        tags.putAll(partitionTags(sample.topicPartition()));
        addMetric(rows, KafkaIotDBSchema.REPLICA, batch, sample.collectedAt(), tags, sample.metric(), sample.value());
    }

    private void addMetric(Map<RowKey, MutableRow> rows, KafkaIotDBSchema.TableDefinition table,
        KafkaMetricWriteBatch batch, Instant time, Map<String, String> entityTags, KafkaMetricDefinition metric,
        BigDecimal value) {
        String column = KafkaIotDBSchema.metricColumn(metric);
        KafkaIotDBSchema.ColumnType expectedType = table.fields().get(column);
        if (expectedType == null) {
            throw new IllegalArgumentException(metric.metricName() + " does not belong to " + table.name());
        }
        Object storedValue;
        if (expectedType == KafkaIotDBSchema.ColumnType.DOUBLE) {
            storedValue = value.doubleValue();
        } else {
            storedValue = value.longValueExact();
        }
        row(rows, table, batch, time, entityTags).addField(column, storedValue);
    }

    private void addCollectionRun(Map<RowKey, MutableRow> rows, KafkaMetricWriteBatch batch) {
        MutableRow row = row(rows, KafkaIotDBSchema.COLLECTION_RUN, batch, batch.collectedAt(), Map.of());
        int failures = batch.metrics().failures().size();
        row.addField("status", failures == 0 ? "SUCCESS" : "PARTIAL");
        row.addField("sample_count", (long) batch.metrics().sampleCount());
        row.addField("failure_count", (long) failures);
        row.addField("duration_ms", batch.durationMillis());
    }

    private void addFailures(Map<RowKey, MutableRow> rows, KafkaMetricWriteBatch batch) {
        List<KafkaMetricFailure> failures = batch.metrics().failures();
        for (int index = 0; index < failures.size(); index++) {
            KafkaMetricFailure failure = failures.get(index);
            MutableRow row = row(rows, KafkaIotDBSchema.COLLECTION_FAILURE, batch, batch.collectedAt(),
                Map.of("failure_id", Integer.toString(index)));
            row.addField("metric_name", failure.metricName());
            row.addField("object_name", failure.objectName());
            row.addField("reason", failure.reason());
        }
    }

    private MutableRow row(Map<RowKey, MutableRow> rows, KafkaIotDBSchema.TableDefinition table,
        KafkaMetricWriteBatch batch, Instant time, Map<String, String> entityTags) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("organization_id", batch.organizationId());
        tags.put("cluster_id", batch.clusterId());
        tags.putAll(entityTags);
        if (!tags.keySet().equals(new java.util.LinkedHashSet<>(table.tags()))) {
            throw new IllegalArgumentException("Tags do not match table " + table.name() + ": " + tags.keySet());
        }
        RowKey key = new RowKey(table, time, tags);
        return rows.computeIfAbsent(key,
            ignored -> new MutableRow(table, time, tags, Map.of("cluster_name", batch.clusterName())));
    }

    private Map<String, String> partitionTags(TopicPartition partition) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("topic", partition.topic());
        tags.put("partition_id", Integer.toString(partition.partition()));
        return tags;
    }

    @Value
    @Accessors(fluent = true)
    private static final class RowKey {

        KafkaIotDBSchema.TableDefinition table;

        Instant time;

        Map<String, String> tags;

        private RowKey(KafkaIotDBSchema.TableDefinition table, Instant time, Map<String, String> tags) {
            this.table = table;
            this.time = time;
            this.tags = Map.copyOf(tags);
        }
    }

    private static final class MutableRow {

        private final KafkaIotDBSchema.TableDefinition table;
        private final Instant time;
        private final Map<String, String> tags;
        private final Map<String, String> attributes;
        private final Map<String, Object> fields = new LinkedHashMap<>();

        private MutableRow(KafkaIotDBSchema.TableDefinition table, Instant time, Map<String, String> tags,
            Map<String, String> attributes) {
            this.table = table;
            this.time = time;
            this.tags = new LinkedHashMap<>(tags);
            this.attributes = new LinkedHashMap<>(attributes);
        }

        private void addField(String name, Object value) {
            if (fields.putIfAbsent(name, value) != null) {
                throw new IllegalStateException("Duplicate Kafka metric field " + table.name() + "." + name);
            }
        }

        private KafkaMetricRow build() {
            return new KafkaMetricRow(table, time, tags, attributes, fields);
        }
    }
}
