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

import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.BrokerMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ClusterMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.GroupMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricDefinition;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricType;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.PartitionMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class KafkaIotDBSchema {

    static final String DATABASE = "eventmesh_dashboard";
    static final String INITIALIZATION_SCRIPT = "iotdb/kafka-metrics-schema.sql";

    static final TableDefinition CLUSTER = metricTable("kafka_cluster_metrics",
        List.of("organization_id", "cluster_id"), ClusterMetric.values());
    static final TableDefinition BROKER = metricTable("kafka_broker_metrics",
        List.of("organization_id", "cluster_id", "broker_id"), BrokerMetric.values());
    static final TableDefinition TOPIC = metricTable("kafka_topic_metrics",
        List.of("organization_id", "cluster_id", "topic"), TopicMetric.values());
    static final TableDefinition PARTITION = metricTable("kafka_partition_metrics",
        List.of("organization_id", "cluster_id", "topic", "partition_id"), PartitionMetric.values());
    static final TableDefinition GROUP = metricTable("kafka_group_metrics",
        List.of("organization_id", "cluster_id", "group_id"), GroupMetric.HEALTH_STATE,
        GroupMetric.HEALTH_CHECK_PASSED, GroupMetric.HEALTH_CHECK_TOTAL, GroupMetric.STATE);
    static final TableDefinition GROUP_PARTITION = metricTable("kafka_group_partition_metrics",
        List.of("organization_id", "cluster_id", "group_id", "topic", "partition_id"),
        GroupMetric.OFFSET_CONSUMED, GroupMetric.LOG_END_OFFSET, GroupMetric.LAG);
    static final TableDefinition REPLICA = metricTable("kafka_replica_metrics",
        List.of("organization_id", "cluster_id", "broker_id", "topic", "partition_id"),
        ReplicaMetric.values());
    static final TableDefinition COLLECTION_RUN = table("kafka_collection_runs",
        List.of("organization_id", "cluster_id"), Map.of("cluster_name", ColumnType.STRING),
        orderedFields("status", ColumnType.STRING, "sample_count", ColumnType.INT64,
            "failure_count", ColumnType.INT64, "duration_ms", ColumnType.INT64));
    static final TableDefinition COLLECTION_FAILURE = table("kafka_collection_failures",
        List.of("organization_id", "cluster_id", "failure_id"), Map.of("cluster_name", ColumnType.STRING),
        orderedFields("metric_name", ColumnType.STRING, "object_name", ColumnType.STRING,
            "reason", ColumnType.STRING));

    private static final List<TableDefinition> TABLES = List.of(CLUSTER, BROKER, TOPIC, PARTITION, GROUP,
        GROUP_PARTITION, REPLICA, COLLECTION_RUN, COLLECTION_FAILURE);

    private KafkaIotDBSchema() {
    }

    static List<TableDefinition> tables() {
        return TABLES;
    }

    static String metricColumn(KafkaMetricDefinition metric) {
        String name = metric.metricName().replace('-', '_');
        name = name.replaceAll("([a-z0-9])([A-Z])", "$1_$2");
        return name.replaceAll("_+", "_").toLowerCase(Locale.ROOT);
    }

    static ColumnType metricType(KafkaMetricDefinition metric) {
        return metric.type() == KafkaMetricType.RATE || metric.type() == KafkaMetricType.RATIO
            ? ColumnType.DOUBLE : ColumnType.INT64;
    }

    private static TableDefinition metricTable(String name, List<String> tags,
        KafkaMetricDefinition... metrics) {
        Map<String, ColumnType> fields = new LinkedHashMap<>();
        Arrays.stream(metrics).forEach(metric -> fields.put(metricColumn(metric), metricType(metric)));
        return table(name, tags, Map.of("cluster_name", ColumnType.STRING), fields);
    }

    private static TableDefinition table(String name, List<String> tags, Map<String, ColumnType> attributes,
        Map<String, ColumnType> fields) {
        return new TableDefinition(name, tags, attributes, fields);
    }

    private static Map<String, ColumnType> orderedFields(Object... values) {
        Map<String, ColumnType> result = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            result.put((String) values[index], (ColumnType) values[index + 1]);
        }
        return result;
    }

    enum ColumnType {
        INT64("INT64", Types.BIGINT),
        DOUBLE("DOUBLE", Types.DOUBLE),
        STRING("STRING", Types.VARCHAR);

        private final String sqlType;
        private final int jdbcType;

        ColumnType(String sqlType, int jdbcType) {
            this.sqlType = sqlType;
            this.jdbcType = jdbcType;
        }

        String sqlType() {
            return sqlType;
        }

        int jdbcType() {
            return jdbcType;
        }
    }

    record TableDefinition(String name, List<String> tags, Map<String, ColumnType> attributes,
                           Map<String, ColumnType> fields) {

        TableDefinition {
            tags = List.copyOf(tags);
            attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
            fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        }

        List<String> fixedColumns() {
            List<String> columns = new ArrayList<>();
            columns.add("time");
            columns.addAll(tags);
            columns.addAll(attributes.keySet());
            return List.copyOf(columns);
        }
    }
}
