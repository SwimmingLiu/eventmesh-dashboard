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
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.BrokerMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ClusterMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.GroupMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricFailure;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.PartitionMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import org.apache.iotdb.jdbc.IoTDBDataSource;

import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class IotDBKafkaMetricStoreIntegrationTest {

    @Test
    public void testInitializeWriteAndRead() throws Exception {
        Assume.assumeTrue("Set -Dkafka.iotdb.integration=true to run with IoTDB",
            Boolean.getBoolean("kafka.iotdb.integration"));
        String address = System.getProperty("kafka.iotdb.address", "127.0.0.1:6667");
        String username = System.getProperty("kafka.iotdb.username", "root");
        String password = System.getProperty("kafka.iotdb.password", "root");
        IoTDBDataSource root = dataSource(address, username, password, null);
        try (Connection connection = root.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS eventmesh_dashboard WITH (TTL=31536000000)");
        }
        IoTDBDataSource database = dataSource(address, username, password, KafkaIotDBSchema.DATABASE);
        IotDBKafkaMetricStore store = new IotDBKafkaMetricStore(database);
        store.initialize();

        Instant collectedAt = Instant.now();
        String clusterId = "integration-" + UUID.randomUUID();
        TopicPartition partition = new TopicPartition("orders", 0);
        KafkaMetricCollection metrics = KafkaMetricCollection.builder()
            .add(ClusterMetric.BYTES_IN, new BigDecimal("42.5"), collectedAt)
            .add(BrokerMetric.LOG_SIZE, "1", BigDecimal.TEN, collectedAt)
            .add(TopicMetric.MESSAGES, "orders", "1", BigDecimal.ONE, collectedAt)
            .add(PartitionMetric.LOG_SIZE, partition, BigDecimal.TEN, collectedAt)
            .add(GroupMetric.STATE, "shipping", null, BigDecimal.ONE, collectedAt)
            .add(GroupMetric.LAG, "shipping", partition, BigDecimal.TEN, collectedAt)
            .add(ReplicaMetric.IN_SYNC, "1", partition, BigDecimal.ONE, collectedAt)
            .failure(new KafkaMetricFailure("UnavailableMetric", "broker-1", "integration failure"))
            .build();
        store.write(new KafkaMetricWriteBatch("integration", clusterId, "integration", collectedAt, 1L, metrics));

        String query = "SELECT bytes_in FROM kafka_cluster_metrics WHERE organization_id = ? AND \"cluster_id\" = ? "
            + "AND time = ?";
        try (Connection connection = database.getConnection(); PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, "integration");
            statement.setString(2, clusterId);
            statement.setTimestamp(3, java.sql.Timestamp.from(collectedAt));
            try (ResultSet result = statement.executeQuery()) {
                Assert.assertTrue(result.next());
                Assert.assertEquals(42.5D, result.getDouble("bytes_in"), 0.0001D);
            }
        }
        for (KafkaIotDBSchema.TableDefinition table : KafkaIotDBSchema.tables()) {
            assertStored(database, table.name(), clusterId, collectedAt);
        }
    }

    private void assertStored(IoTDBDataSource dataSource, String table, String clusterId, Instant collectedAt)
        throws Exception {
        String query = "SELECT * FROM " + table + " WHERE \"cluster_id\" = ? AND time = ?";
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, clusterId);
            statement.setTimestamp(2, java.sql.Timestamp.from(collectedAt));
            try (ResultSet result = statement.executeQuery()) {
                Assert.assertTrue("Expected a row in " + table, result.next());
            }
        }
    }

    private IoTDBDataSource dataSource(String address, String username, String password, String database) {
        IoTDBDataSource dataSource = new IoTDBDataSource();
        String suffix = database == null ? "/?sql_dialect=table" : "/" + database + "?sql_dialect=table";
        dataSource.setUrl("jdbc:iotdb://" + address + suffix);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        return dataSource;
    }
}
