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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import javax.sql.DataSource;

public final class IotDBKafkaMetricStore implements KafkaMetricStore {

    private static final int DEFAULT_BATCH_SIZE = 500;

    private final DataSource dataSource;
    private final int batchSize;
    private final KafkaMetricRowMapper mapper;
    private final KafkaIotDBSchemaInitializer initializer;

    public IotDBKafkaMetricStore(DataSource dataSource) {
        this(dataSource, DEFAULT_BATCH_SIZE);
    }

    public IotDBKafkaMetricStore(DataSource dataSource, int batchSize) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }
        this.batchSize = batchSize;
        this.mapper = new KafkaMetricRowMapper();
        this.initializer = new KafkaIotDBSchemaInitializer(dataSource);
    }

    @Override
    public void initialize() {
        initializer.initialize();
    }

    @Override
    public void write(KafkaMetricWriteBatch batch) {
        Objects.requireNonNull(batch, "batch");
        Map<InsertShape, List<KafkaMetricRow>> groups = groupRows(mapper.map(batch));
        try (Connection connection = dataSource.getConnection()) {
            for (Map.Entry<InsertShape, List<KafkaMetricRow>> entry : groups.entrySet()) {
                writeRows(connection, entry.getKey(), entry.getValue());
            }
        } catch (SQLException ex) {
            throw new KafkaMetricStorageException("Unable to store Kafka metrics for cluster " + batch.clusterId(), ex);
        }
    }

    private Map<InsertShape, List<KafkaMetricRow>> groupRows(List<KafkaMetricRow> rows) {
        Map<InsertShape, List<KafkaMetricRow>> result = new LinkedHashMap<>();
        for (KafkaMetricRow row : rows) {
            InsertShape shape = new InsertShape(row.table(), List.copyOf(row.fields().keySet()));
            result.computeIfAbsent(shape, ignored -> new ArrayList<>()).add(row);
        }
        return result;
    }

    private void writeRows(Connection connection, InsertShape shape, List<KafkaMetricRow> rows) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(insertSql(shape))) {
            int pending = 0;
            for (KafkaMetricRow row : rows) {
                bind(statement, shape, row);
                statement.addBatch();
                pending++;
                if (pending == batchSize) {
                    statement.executeBatch();
                    pending = 0;
                }
            }
            if (pending > 0) {
                statement.executeBatch();
            }
        }
    }

    private String insertSql(InsertShape shape) {
        List<String> columns = new ArrayList<>(shape.table().fixedColumns());
        columns.addAll(shape.fields());
        StringJoiner placeholders = new StringJoiner(", ");
        columns.forEach(ignored -> placeholders.add("?"));
        return "INSERT INTO " + shape.table().name() + " ("
            + columns.stream().map(this::quoteIdentifier).collect(java.util.stream.Collectors.joining(", "))
            + ") VALUES ("
            + placeholders + ")";
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    private void bind(PreparedStatement statement, InsertShape shape, KafkaMetricRow row) throws SQLException {
        int index = 1;
        statement.setTimestamp(index++, Timestamp.from(row.time()));
        for (String tag : shape.table().tags()) {
            statement.setString(index++, required(row.tags(), tag, shape.table().name()));
        }
        for (String attribute : shape.table().attributes().keySet()) {
            statement.setString(index++, required(row.attributes(), attribute, shape.table().name()));
        }
        for (String field : shape.fields()) {
            Object value = row.fields().get(field);
            KafkaIotDBSchema.ColumnType type = shape.table().fields().get(field);
            if (value == null) {
                statement.setNull(index++, type.jdbcType());
            } else if (type == KafkaIotDBSchema.ColumnType.INT64) {
                statement.setLong(index++, ((Number) value).longValue());
            } else if (type == KafkaIotDBSchema.ColumnType.DOUBLE) {
                statement.setDouble(index++, ((Number) value).doubleValue());
            } else {
                statement.setString(index++, value.toString());
            }
        }
    }

    private String required(Map<String, String> values, String name, String table) {
        String value = values.get(name);
        if (value == null) {
            throw new IllegalArgumentException("Missing " + name + " for " + table);
        }
        return value;
    }

    private record InsertShape(KafkaIotDBSchema.TableDefinition table, List<String> fields) {
    }
}
