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

package org.apache.eventmesh.dashboard.console.function.report.iotdb;

import org.apache.eventmesh.dashboard.console.controller.ReportController;
import org.apache.eventmesh.dashboard.console.function.report.ReportConfig;
import org.apache.eventmesh.dashboard.console.function.report.ReportConfig.KafkaBrokerJmxConfig;
import org.apache.eventmesh.dashboard.console.function.report.ReportConfig.KafkaCollectConfig;
import org.apache.eventmesh.dashboard.console.function.report.ReportHandlerManage;
import org.apache.eventmesh.dashboard.console.function.report.collect.ManagedCollect;
import org.apache.eventmesh.dashboard.console.function.report.iotdb.kafka.IotDBKafkaMetricModule;

import org.apache.iotdb.jdbc.IoTDBDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import org.junit.Assume;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Real Kafka-to-IoTDB pipeline test whose assertion is made through ReportController's HTTP API.
 */
public class ReportControllerKafkaPipelineIntegrationTest {

    private static final String DATABASE = "eventmesh_dashboard";

    private static final List<String> KAFKA_TABLES = List.of(
        "kafka_cluster_metrics",
        "kafka_broker_metrics",
        "kafka_topic_metrics",
        "kafka_partition_metrics",
        "kafka_group_metrics",
        "kafka_group_partition_metrics",
        "kafka_replica_metrics",
        "kafka_collection_runs",
        "kafka_collection_failures");

    @Test
    public void testCollectStoreAndQueryThroughReportController() throws Throwable {
        Assume.assumeTrue("Set -Dkafka.pipeline.integration=true to run with Kafka and IoTDB",
            Boolean.getBoolean("kafka.pipeline.integration"));
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "127.0.0.1:9092");
        String iotdbAddress = System.getProperty("kafka.iotdb.address", "127.0.0.1:6667");
        String iotdbUsername = System.getProperty("kafka.iotdb.username", "root");
        String iotdbPassword = System.getProperty("kafka.iotdb.password", "root");
        String jmxHost = System.getProperty("kafka.jmx.host", "127.0.0.1");
        int jmxPort = Integer.getInteger("kafka.jmx.port", 9998);
        int brokerId = Integer.getInteger("kafka.broker.id", 2);
        long clusterId = positiveId(UUID.randomUUID().getMostSignificantBits());
        long organizationId = positiveId(UUID.randomUUID().getLeastSignificantBits());

        IoTDBDataSource root = dataSource(iotdbAddress, iotdbUsername, iotdbPassword, null);
        try (Connection connection = root.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS eventmesh_dashboard WITH (TTL=31536000000)");
        }
        IoTDBDataSource database = dataSource(iotdbAddress, iotdbUsername, iotdbPassword, DATABASE);
        Throwable primaryFailure = null;
        try {
            IotDBMetricModuleRegistry registry = new IotDBMetricModuleRegistry();
            registry.register(new IotDBKafkaMetricModule(database));
            registry.initialize();

            ReportConfig reportConfig = reportConfig(organizationId, clusterId, bootstrapServers, brokerId, jmxHost, jmxPort);
            List<ManagedCollect> collects = registry.createCollects(reportConfig);
            Throwable collectionFailure = null;
            try {
                org.junit.Assert.assertEquals(1, collects.size());
                collects.get(0).request();
            } catch (Throwable failure) {
                collectionFailure = failure;
                throw failure;
            } finally {
                Throwable closeFailure = null;
                for (ManagedCollect collect : collects) {
                    try {
                        collect.close();
                    } catch (Throwable failure) {
                        if (closeFailure == null) {
                            closeFailure = failure;
                        } else {
                            closeFailure.addSuppressed(failure);
                        }
                    }
                }
                if (closeFailure != null) {
                    if (collectionFailure == null) {
                        throw closeFailure;
                    }
                    collectionFailure.addSuppressed(closeFailure);
                }
            }

            IotDBReportEngine reportEngine = new IotDBReportEngine(registry);
            ReportHandlerManage reportManage = new ReportHandlerManage();
            reportManage.setReportEngine(reportEngine);
            reportManage.setReportConfig(reportConfig);
            ReportController controller = new ReportController();
            ReflectionTestUtils.setField(controller, "reportHandlerManage", reportManage);
            MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
            Throwable queryFailure = null;
            try {
                mockMvc.perform(post("/report/reportBySingle")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {
                              "reportName": "kafka_broker_messages_in",
                              "organizationId": %d,
                              "clustersId": %d,
                              "brokerId": "%d",
                              "order": "desc",
                              "limit": 1
                            }
                            """.formatted(organizationId, clusterId, brokerId)))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$[0].organization_id").value(Long.toString(organizationId)))
                    .andExpect(jsonPath("$[0].cluster_id").value(Long.toString(clusterId)))
                    .andExpect(jsonPath("$[0].broker_id").value(Integer.toString(brokerId)))
                    .andExpect(jsonPath("$[0].value").isNumber());
            } catch (Throwable failure) {
                queryFailure = failure;
                throw failure;
            } finally {
                try {
                    reportManage.close();
                } catch (Throwable closeFailure) {
                    if (queryFailure == null) {
                        throw closeFailure;
                    }
                    queryFailure.addSuppressed(closeFailure);
                }
            }
        } catch (Throwable failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            try {
                deleteTestRows(database, organizationId, clusterId);
            } catch (Throwable cleanupFailure) {
                if (primaryFailure == null) {
                    throw cleanupFailure;
                }
                primaryFailure.addSuppressed(cleanupFailure);
            }
        }
    }

    private ReportConfig reportConfig(long organizationId, long clusterId, String bootstrapServers,
        int brokerId, String jmxHost, int jmxPort) {
        KafkaBrokerJmxConfig broker = new KafkaBrokerJmxConfig();
        broker.setBrokerId(brokerId);
        broker.setHost(jmxHost);
        broker.setPort(jmxPort);

        KafkaCollectConfig collect = new KafkaCollectConfig();
        collect.setOrganizationId(organizationId);
        collect.setClusterId(clusterId);
        collect.setClusterName("report-controller-pipeline");
        collect.setBootstrapServers(bootstrapServers);
        collect.setBrokers(List.of(broker));

        ReportConfig reportConfig = new ReportConfig();
        reportConfig.setKafkaCollectConfigList(List.of(collect));
        return reportConfig;
    }

    private long positiveId(long value) {
        return value == Long.MIN_VALUE ? Long.MAX_VALUE : Math.max(1L, Math.abs(value));
    }

    private void deleteTestRows(IoTDBDataSource dataSource, long organizationId, long clusterId) throws Throwable {
        Throwable failure = null;
        for (String table : KAFKA_TABLES) {
            try (Connection connection = dataSource.getConnection()) {
                String sql = "DELETE FROM " + table + " WHERE \"organization_id\" = ? AND \"cluster_id\" = ?";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, Long.toString(organizationId));
                    statement.setString(2, Long.toString(clusterId));
                    statement.executeUpdate();
                }
                String verification = "SELECT * FROM " + table
                    + " WHERE \"organization_id\" = ? AND \"cluster_id\" = ? LIMIT 1";
                try (PreparedStatement statement = connection.prepareStatement(verification)) {
                    statement.setString(1, Long.toString(organizationId));
                    statement.setString(2, Long.toString(clusterId));
                    try (java.sql.ResultSet result = statement.executeQuery()) {
                        org.junit.Assert.assertFalse("Test rows remain in " + table, result.next());
                    }
                }
            } catch (Throwable tableFailure) {
                if (failure == null) {
                    failure = tableFailure;
                } else {
                    failure.addSuppressed(tableFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
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
