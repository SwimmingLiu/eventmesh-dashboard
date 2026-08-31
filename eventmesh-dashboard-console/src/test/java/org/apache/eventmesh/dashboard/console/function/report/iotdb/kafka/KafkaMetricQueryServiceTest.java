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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.LocalDateTime;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class KafkaMetricQueryServiceTest {

    private final DataSource dataSource = Mockito.mock(DataSource.class);
    private final Connection connection = Mockito.mock(Connection.class);
    private final PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    private final ResultSet resultSet = Mockito.mock(ResultSet.class);
    private final ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);

    @Before
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
        Assert.assertTrue(sql.getValue().contains("FROM kafka_broker_metrics"));
        Assert.assertTrue(sql.getValue().contains("\"bytes_in\" AS value"));
        Assert.assertTrue(sql.getValue().endsWith("ORDER BY time DESC LIMIT 50"));
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
        report.setKafkaGroupId("orders-consumer");
        report.setTopicName("orders");
        report.setPartitionId(3);

        new KafkaMetricQueryService(dataSource).query(report);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connection).prepareStatement(sql.capture());
        Assert.assertTrue(sql.getValue().contains("FROM kafka_group_partition_metrics"));
        Mockito.verify(statement).setString(1, "orders");
        Mockito.verify(statement).setString(2, "orders-consumer");
        Mockito.verify(statement).setString(3, "3");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsUnknownMetricInsteadOfInterpolatingIt() {
        SingleGeneralReportDO report = new SingleGeneralReportDO();
        report.setReportName("kafka_broker_bytes_in_from_users");

        new KafkaMetricQueryService(dataSource).query(report);
    }
}
