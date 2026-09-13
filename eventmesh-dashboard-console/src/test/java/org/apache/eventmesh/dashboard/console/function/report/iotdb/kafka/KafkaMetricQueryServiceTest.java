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

import org.apache.iotdb.jdbc.IoTDBPreparedStatement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.LocalDateTime;

import javax.sql.DataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class KafkaMetricQueryServiceTest {

    private final DataSource dataSource = Mockito.mock(DataSource.class);
    private final Connection connection = Mockito.mock(Connection.class);
    private final PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    private final ResultSet resultSet = Mockito.mock(ResultSet.class);
    private final ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);

    @BeforeEach
    public void setUp() throws Exception {
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
        Mockito.when(metaData.getColumnCount()).thenReturn(0);
        Mockito.when(resultSet.next()).thenReturn(false);
    }

    @Test
    public void testQueriesCanonicalBrokerMetricWithBoundFilters() throws Exception {
        SingleGeneralReportDO report = new SingleGeneralReportDO();
        report.setReportName("kafka_broker_bytes_in");
        report.setOrganizationId(7L);
        report.setClustersId(9L);
        report.setBrokerId("2");
        report.setStartTime(LocalDateTime.of(2026, 8, 30, 0, 0));
        report.setEndTime(LocalDateTime.of(2026, 8, 31, 0, 0));
        report.setOrder("desc");
        report.setLimit(50);

        new KafkaMetricQueryService(dataSource).query(report);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connection).prepareStatement(sql.capture());
        Assertions.assertTrue(sql.getValue().contains("FROM kafka_broker_metrics"));
        Assertions.assertTrue(sql.getValue().contains("\"bytes_in\" AS value"));
        Assertions.assertTrue(sql.getValue().contains("WHERE \"bytes_in\" IS NOT NULL"));
        Assertions.assertTrue(sql.getValue().endsWith("ORDER BY time DESC LIMIT 50"));
        Mockito.verify(statement).setString(1, "7");
        Mockito.verify(statement).setString(2, "9");
        Mockito.verify(statement).setString(3, "2");
        Mockito.verify(statement).setTimestamp(Mockito.eq(4), Mockito.any());
        Mockito.verify(statement).setTimestamp(Mockito.eq(5), Mockito.any());
    }

    @Test
    public void testResolvesGroupPartitionMetricFromExplicitFields() throws Exception {
        SingleGeneralReportDO report = new SingleGeneralReportDO();
        report.setReportName("consumer-lag");
        report.setKafkaDimension("group");
        report.setKafkaMetric("Lag");
        report.setOrganizationId(7L);
        report.setClustersId(9L);
        report.setKafkaGroupId("orders-consumer");
        report.setTopicName("orders");
        report.setPartitionId(3);

        new KafkaMetricQueryService(dataSource).query(report);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connection).prepareStatement(sql.capture());
        Assertions.assertTrue(sql.getValue().contains("FROM kafka_group_partition_metrics"));
        Mockito.verify(statement).setString(1, "7");
        Mockito.verify(statement).setString(2, "9");
        Mockito.verify(statement).setString(3, "orders");
        Mockito.verify(statement).setString(4, "orders-consumer");
        Mockito.verify(statement).setString(5, "3");
    }

    @Test
    public void testEscapesQuotedGroupFilterForWrappedIotDBStatement() throws Exception {
        Mockito.when(statement.isWrapperFor(IoTDBPreparedStatement.class)).thenReturn(true);
        SingleGeneralReportDO report = new SingleGeneralReportDO();
        report.setReportName("kafka_group_lag");
        report.setOrganizationId(7L);
        report.setClustersId(9L);
        report.setKafkaGroupId("consumer' OR '1'='1");

        new KafkaMetricQueryService(dataSource).query(report);

        Mockito.verify(statement).setString(3, "consumer'' OR ''1''=''1");
        Mockito.verify(connection).prepareStatement(Mockito.argThat(sql -> !sql.contains("consumer")));
    }

    @Test
    public void testKeepsQuotedValueUnchangedForServerSideJdbcParameters() throws Exception {
        KafkaJdbcParameters.setString(statement, 1, "consumer's");
        Mockito.verify(statement).setString(1, "consumer's");
    }

    @Test
    public void testRejectsUnknownMetricInsteadOfInterpolatingIt() {
        SingleGeneralReportDO report = new SingleGeneralReportDO();
        report.setReportName("kafka_broker_bytes_in_from_users");

        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new KafkaMetricQueryService(dataSource).query(report));
    }

    @Test
    public void testRejectsQueryWithoutTenantScope() {
        SingleGeneralReportDO report = new SingleGeneralReportDO();
        report.setReportName("kafka_cluster_alive");

        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new KafkaMetricQueryService(dataSource).query(report));
    }
}
