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
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaCollectConfig.KafkaBrokerJmxConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaCollectConfig;
import org.apache.eventmesh.dashboard.console.function.report.ReportHandlerManage;
import org.apache.eventmesh.dashboard.console.function.report.iotdb.kafka.KafkaReportService;
import org.apache.eventmesh.dashboard.console.spring.support.FunctionConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaMetricCollect;
import org.apache.eventmesh.dashboard.console.function.report.iotdb.kafka.IotDBKafkaMetricModule;

import org.apache.iotdb.jdbc.IoTDBDataSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
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
            IotDBKafkaMetricModule module = new IotDBKafkaMetricModule(database);
            module.initialize();

            KafkaCollectConfig collectConfig = reportConfig(organizationId, clusterId, bootstrapServers, brokerId, jmxHost, jmxPort);
            List<KafkaMetricCollect> collects = List.of(module.createCollect(collectConfig));
            Throwable collectionFailure = null;
            try {
                org.junit.Assert.assertEquals(1, collects.size());
                collects.get(0).request();
            } catch (Throwable failure) {
                collectionFailure = failure;
                throw failure;
            } finally {
                Throwable closeFailure = null;
                for (KafkaMetricCollect collect : collects) {
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

            ReportHandlerManage reportManage = new ReportHandlerManage();
                reportManage.setReportConfig(new ReportConfig());
            FunctionConfig functionConfig = new FunctionConfig();
            functionConfig.setReportConfig(new ReportConfig());
            KafkaReportService kafkaService = new KafkaReportService(functionConfig);
            ReflectionTestUtils.setField(kafkaService, "module", module);
            org.apache.eventmesh.dashboard.console.service.cluster.ClusterService clusters =
                org.mockito.Mockito.mock(org.apache.eventmesh.dashboard.console.service.cluster.ClusterService.class);
            org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity cluster =
                new org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity();
            cluster.setId(clusterId);
            cluster.setOrganizationId(organizationId);
            cluster.setStatus(1L);
            cluster.setClusterType(org.apache.eventmesh.dashboard.common.enums.ClusterType.STORAGE_KAFKA_CLUSTER);
            org.mockito.Mockito.when(clusters.queryClusterById(org.mockito.Mockito.any())).thenReturn(cluster);
            ReflectionTestUtils.setField(kafkaService, "clusterService", clusters);
            ReportController controller = new ReportController();
            ReflectionTestUtils.setField(controller, "reportHandlerManage", reportManage);
            ReflectionTestUtils.setField(controller, "kafkaReportService", kafkaService);
            MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
            Throwable queryFailure = null;
            try {
                MvcResult result = mockMvc.perform(post("/report/reportByHome")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {
                              "reportNameList": [
                                "kafka_broker_log_size",
                                "kafka_broker_messages_in"
                              ],
                              "organizationId": %d,
                              "clustersId": %d
                            }
                            """.formatted(organizationId, clusterId)))
                    .andDo(print())
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.kafka_broker_log_size[0].organization_id")
                        .value(Long.toString(organizationId)))
                    .andExpect(jsonPath("$.kafka_broker_log_size[0].cluster_id")
                        .value(Long.toString(clusterId)))
                    .andExpect(jsonPath("$.kafka_broker_log_size[0].broker_id")
                        .value(Integer.toString(brokerId)))
                    .andExpect(jsonPath("$.kafka_broker_log_size[0].value").isNumber())
                    .andExpect(jsonPath("$.kafka_broker_messages_in[0].value").isNumber())
                    .andReturn();
                JsonNode response = new ObjectMapper().readTree(result.getResponse().getContentAsString());
                long logSize = response.path("kafka_broker_log_size").path(0).path("value").asLong();
                org.junit.Assert.assertTrue("Expected a positive Kafka Broker log size, got " + logSize,
                    logSize > 0L);
            } catch (Throwable failure) {
                queryFailure = failure;
                throw failure;
            } finally {
                try {
                    kafkaService.close();
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

    private KafkaCollectConfig reportConfig(long organizationId, long clusterId, String bootstrapServers,
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

        return collect;
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
