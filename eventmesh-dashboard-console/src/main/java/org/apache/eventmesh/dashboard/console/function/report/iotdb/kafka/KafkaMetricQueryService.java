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

import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.BrokerMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ClusterMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.GroupMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricDefinition;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.PartitionMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import javax.sql.DataSource;

/**
 * Executes safe, catalog-backed Kafka metric queries against IoTDB's table dialect.
 */
public final class KafkaMetricQueryService {

    private static final int DEFAULT_LIMIT = 1_000;
    private static final int MAX_LIMIT = 10_000;
    private static final String REPORT_PREFIX = "kafka_";
    private static final Map<String, MetricQuery> METRICS = createMetricQueries();

    private final DataSource dataSource;

    public KafkaMetricQueryService(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    public boolean supports(SingleGeneralReportDO report) {
        if (report == null) {
            return false;
        }
        return StringUtils.isNotBlank(report.getKafkaDimension())
            || StringUtils.startsWithIgnoreCase(report.getReportName(), REPORT_PREFIX);
    }

    public List<Map<String, Object>> query(SingleGeneralReportDO report) {
        Objects.requireNonNull(report, "report");
        MetricQuery metric = resolve(report);
        QueryStatement query = buildQuery(report, metric);
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(query.sql())) {
            bind(statement, query.parameters());
            try (ResultSet resultSet = statement.executeQuery()) {
                return rows(resultSet);
            }
        } catch (SQLException e) {
            throw new KafkaMetricStorageException("Unable to query Kafka metric " + report.getReportName(), e);
        }
    }

    private MetricQuery resolve(SingleGeneralReportDO report) {
        String dimension = report.getKafkaDimension();
        String metricName = report.getKafkaMetric();
        if (StringUtils.isBlank(dimension) || StringUtils.isBlank(metricName)) {
            String reportName = StringUtils.defaultString(report.getReportName()).toLowerCase(Locale.ROOT);
            for (String candidate : List.of("cluster", "broker", "topic", "partition", "group", "replica")) {
                String prefix = REPORT_PREFIX + candidate + "_";
                if (reportName.startsWith(prefix)) {
                    dimension = candidate;
                    metricName = reportName.substring(prefix.length());
                    break;
                }
            }
        }
        String key = key(dimension, metricName);
        MetricQuery query = METRICS.get(key);
        if (query == null) {
            throw new IllegalArgumentException("Unknown Kafka metric: dimension=" + dimension + ", metric=" + metricName);
        }
        return query;
    }

    private QueryStatement buildQuery(SingleGeneralReportDO report, MetricQuery metric) {
        if (report.getOrganizationId() == null || report.getClustersId() == null) {
            throw new IllegalArgumentException("organizationId and clustersId are required for Kafka metric queries");
        }
        StringBuilder sql = new StringBuilder("SELECT time");
        for (String tag : metric.table().tags()) {
            sql.append(", ").append(quote(tag));
        }
        sql.append(", cluster_name, ").append(quote(metric.field())).append(" AS value FROM ")
            .append(metric.table().name()).append(" WHERE ").append(quote(metric.field())).append(" IS NOT NULL");

        List<Object> parameters = new ArrayList<>();
        addFilter(sql, parameters, "organization_id", report.getOrganizationId());
        addFilter(sql, parameters, "cluster_id", report.getClustersId());
        addFilter(sql, parameters, "broker_id", report.getBrokerId(), metric.table());
        addFilter(sql, parameters, "topic", report.getTopicName(), metric.table());
        addFilter(sql, parameters, "group_id", report.getKafkaGroupId(), metric.table());
        addFilter(sql, parameters, "partition_id", report.getPartitionId(), metric.table());
        if (report.getStartTime() != null) {
            sql.append(" AND time >= ?");
            parameters.add(Timestamp.valueOf(report.getStartTime()));
        }
        if (report.getEndTime() != null) {
            sql.append(" AND time < ?");
            parameters.add(Timestamp.valueOf(report.getEndTime()));
        }
        sql.append(" ORDER BY time ").append(order(report.getOrder()));
        int limit = report.getLimit() == null ? DEFAULT_LIMIT : report.getLimit();
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("limit must be between 1 and " + MAX_LIMIT);
        }
        int offset = report.getOffset() == null ? 0 : report.getOffset();
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }
        sql.append(" LIMIT ").append(limit);
        if (offset > 0) {
            sql.append(" OFFSET ").append(offset);
        }
        return new QueryStatement(sql.toString(), parameters);
    }

    private static String order(String order) {
        if (StringUtils.isBlank(order)) {
            return "ASC";
        }
        if ("asc".equalsIgnoreCase(order) || "desc".equalsIgnoreCase(order)) {
            return order.toUpperCase(Locale.ROOT);
        }
        throw new IllegalArgumentException("order must be ASC or DESC");
    }

    private static void addFilter(StringBuilder sql, List<Object> parameters, String column, Object value) {
        if (value != null) {
            sql.append(" AND ").append(quote(column)).append(" = ?");
            parameters.add(value.toString());
        }
    }

    private static void addFilter(StringBuilder sql, List<Object> parameters, String column, Object value,
        KafkaIotDBSchema.TableDefinition table) {
        if (value == null) {
            return;
        }
        if (!table.tags().contains(column)) {
            throw new IllegalArgumentException(column + " is not valid for " + table.name());
        }
        addFilter(sql, parameters, column, value);
    }

    private static void bind(PreparedStatement statement, List<Object> parameters) throws SQLException {
        for (int index = 0; index < parameters.size(); index++) {
            Object parameter = parameters.get(index);
            if (parameter instanceof Timestamp timestamp) {
                statement.setTimestamp(index + 1, timestamp);
            } else {
                statement.setString(index + 1, parameter.toString());
            }
        }
    }

    private static List<Map<String, Object>> rows(ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int index = 1; index <= columnCount; index++) {
                row.put(metaData.getColumnLabel(index), resultSet.getObject(index));
            }
            rows.add(row);
        }
        return rows;
    }

    private static Map<String, MetricQuery> createMetricQueries() {
        Map<String, MetricQuery> queries = new HashMap<>();
        register(queries, KafkaIotDBSchema.CLUSTER, ClusterMetric.values());
        register(queries, KafkaIotDBSchema.BROKER, BrokerMetric.values());
        register(queries, KafkaIotDBSchema.TOPIC, TopicMetric.values());
        register(queries, KafkaIotDBSchema.PARTITION, PartitionMetric.values());
        register(queries, KafkaIotDBSchema.GROUP, GroupMetric.HEALTH_STATE, GroupMetric.HEALTH_CHECK_PASSED,
            GroupMetric.HEALTH_CHECK_TOTAL, GroupMetric.STATE);
        register(queries, KafkaIotDBSchema.GROUP_PARTITION, GroupMetric.OFFSET_CONSUMED,
            GroupMetric.LOG_END_OFFSET, GroupMetric.LAG);
        register(queries, KafkaIotDBSchema.REPLICA, ReplicaMetric.values());
        return Map.copyOf(queries);
    }

    private static void register(Map<String, MetricQuery> queries, KafkaIotDBSchema.TableDefinition table,
        KafkaMetricDefinition... metrics) {
        for (KafkaMetricDefinition metric : metrics) {
            String field = KafkaIotDBSchema.metricColumn(metric);
            MetricQuery query = new MetricQuery(table, field);
            queries.put(key(metric.dimension().name(), metric.metricName()), query);
            queries.put(key(metric.dimension().name(), field), query);
        }
    }

    private static String key(String dimension, String metric) {
        return normalize(dimension) + ':' + normalize(metric);
    }

    private static String normalize(String value) {
        return StringUtils.defaultString(value).replace("-", "_").replaceAll("([a-z0-9])([A-Z])", "$1_$2")
            .replaceAll("_+", "_").toLowerCase(Locale.ROOT);
    }

    private static String quote(String identifier) {
        return '"' + identifier + '"';
    }

    private record MetricQuery(KafkaIotDBSchema.TableDefinition table, String field) {
    }

    private record QueryStatement(String sql, List<Object> parameters) {
    }
}
