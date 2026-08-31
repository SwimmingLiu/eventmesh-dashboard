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

import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaMetricPersistenceCoordinator;
import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;
import org.apache.eventmesh.dashboard.core.gather.jmx.JmxConnectionConfig;
import org.apache.eventmesh.dashboard.core.gather.kafka.collector.KafkaBrokerJmxEndpoint;

import org.apache.iotdb.jdbc.IoTDBDataSource;

import org.apache.kafka.clients.admin.Admin;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class KafkaMetricPipelineIntegrationTest {

    @Test
    public void testCollectStoreAndQuery() throws Exception {
        Assume.assumeTrue("Set -Dkafka.pipeline.integration=true to run with Kafka and IoTDB",
            Boolean.getBoolean("kafka.pipeline.integration"));
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "127.0.0.1:9092");
        String iotdbAddress = System.getProperty("kafka.iotdb.address", "127.0.0.1:6667");
        String jmxHost = System.getProperty("kafka.jmx.host", "127.0.0.1");
        int jmxPort = Integer.getInteger("kafka.jmx.port", 9998);
        int brokerId = Integer.getInteger("kafka.broker.id", 2);
        long clusterId = positiveId(UUID.randomUUID().getMostSignificantBits());
        long organizationId = positiveId(UUID.randomUUID().getLeastSignificantBits());

        IoTDBDataSource root = dataSource(iotdbAddress, null);
        try (Connection connection = root.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS eventmesh_dashboard WITH (TTL=31536000000)");
        }
        IoTDBDataSource database = dataSource(iotdbAddress, KafkaIotDBSchema.DATABASE);
        IotDBKafkaMetricStore store = new IotDBKafkaMetricStore(database);
        store.initialize();
        KafkaBrokerJmxEndpoint endpoint = new KafkaBrokerJmxEndpoint(brokerId,
            JmxConnectionConfig.builder(jmxHost, jmxPort).build());

        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrapServers))) {
            new KafkaMetricPersistenceCoordinator(store).collectAndStore(
                Long.toString(organizationId), Long.toString(clusterId), "pipeline", admin, List.of(endpoint));
        }

        SingleGeneralReportDO query = new SingleGeneralReportDO();
        query.setReportName("kafka_cluster_alive");
        query.setOrganizationId(organizationId);
        query.setClustersId(clusterId);
        query.setKafkaDimension("cluster");
        query.setKafkaMetric("Alive");
        query.setLimit(1);
        query.setOrder("desc");
        List<Map<String, Object>> rows = new KafkaMetricQueryService(database).query(query);
        Assert.assertFalse(rows.isEmpty());
        Assert.assertEquals(1L, ((Number) rows.get(0).get("value")).longValue());
    }

    private long positiveId(long value) {
        return value == Long.MIN_VALUE ? Long.MAX_VALUE : Math.max(1L, Math.abs(value));
    }

    private IoTDBDataSource dataSource(String address, String database) {
        IoTDBDataSource dataSource = new IoTDBDataSource();
        String suffix = database == null ? "/?sql_dialect=table" : "/" + database + "?sql_dialect=table";
        dataSource.setUrl("jdbc:iotdb://" + address + suffix);
        dataSource.setUser("root");
        dataSource.setPassword("root");
        return dataSource;
    }
}
