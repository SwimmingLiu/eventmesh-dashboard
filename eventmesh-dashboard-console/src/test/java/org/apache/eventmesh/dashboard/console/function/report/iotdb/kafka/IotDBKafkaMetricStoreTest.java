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

import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Instant;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class IotDBKafkaMetricStoreTest {

    @Test
    public void testUsesPreparedBatchesForMetricRows() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
        Instant collectedAt = Instant.parse("2026-08-30T00:00:00Z");
        KafkaMetricCollection metrics = KafkaMetricCollection.builder()
            .add(TopicMetric.MESSAGES, "orders'quoted", null, BigDecimal.ONE, collectedAt)
            .build();

        new IotDBKafkaMetricStore(dataSource, 10).write(
            new KafkaMetricWriteBatch("org-1", "cluster-1", "primary", collectedAt, 5L, metrics));

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connection, Mockito.times(2)).prepareStatement(sql.capture());
        Assert.assertTrue(sql.getAllValues().stream().anyMatch(value -> value.startsWith("INSERT INTO kafka_topic_metrics")));
        Mockito.verify(statement, Mockito.atLeastOnce()).setString(Mockito.anyInt(), Mockito.eq("orders'quoted"));
        Mockito.verify(statement, Mockito.times(2)).addBatch();
        Mockito.verify(statement, Mockito.times(2)).executeBatch();
    }

    @Test
    public void testInitializesEveryStatementFromResource() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.createStatement()).thenReturn(statement);

        new IotDBKafkaMetricStore(dataSource).initialize();

        Mockito.verify(statement, Mockito.times(11)).execute(Mockito.anyString());
    }
}
