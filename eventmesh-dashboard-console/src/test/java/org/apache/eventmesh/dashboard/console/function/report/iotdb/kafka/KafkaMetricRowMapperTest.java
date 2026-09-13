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

import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaMetricRowMapperTest {

    private static final Instant COLLECTED_AT = Instant.parse("2026-08-30T00:00:00Z");

    @Test
    public void testMapsAllKafkaGrainsAndOperationalRows() {
        TopicPartition partition = new TopicPartition("orders", 0);
        KafkaMetricCollection metrics = KafkaMetricCollection.builder()
            .add(ClusterMetric.BYTES_IN, new BigDecimal("12.5"), COLLECTED_AT)
            .add(BrokerMetric.LOG_SIZE, "1", BigDecimal.valueOf(100L), COLLECTED_AT)
            .add(TopicMetric.MESSAGES, "orders", null, BigDecimal.valueOf(9L), COLLECTED_AT)
            .add(PartitionMetric.BYTES_OUT, partition, new BigDecimal("3.25"), COLLECTED_AT)
            .add(GroupMetric.STATE, "reader", null, BigDecimal.valueOf(3L), COLLECTED_AT)
            .add(GroupMetric.LAG, "reader", partition, BigDecimal.valueOf(4L), COLLECTED_AT)
            .add(ReplicaMetric.IN_SYNC, "1", partition, BigDecimal.ONE, COLLECTED_AT)
            .failure(new KafkaMetricFailure("JmxConnection", "2", "timeout"))
            .build();
        KafkaMetricWriteBatch batch = new KafkaMetricWriteBatch("org-1", "cluster-1", "primary", COLLECTED_AT,
            25L, metrics);

        List<KafkaMetricRow> rows = new KafkaMetricRowMapper().map(batch);

        Assertions.assertEquals(9, rows.size());
        assertField(rows, KafkaIotDBSchema.CLUSTER, "bytes_in", 12.5D);
        assertField(rows, KafkaIotDBSchema.BROKER, "log_size", 100L);
        assertField(rows, KafkaIotDBSchema.TOPIC, "messages", 9L);
        assertField(rows, KafkaIotDBSchema.PARTITION, "bytes_out", 3.25D);
        assertField(rows, KafkaIotDBSchema.GROUP, "state", 3L);
        assertField(rows, KafkaIotDBSchema.GROUP_PARTITION, "lag", 4L);
        assertField(rows, KafkaIotDBSchema.REPLICA, "in_sync", 1L);
        assertField(rows, KafkaIotDBSchema.COLLECTION_RUN, "status", "PARTIAL");
        KafkaMetricRow failure = row(rows, KafkaIotDBSchema.COLLECTION_FAILURE);
        Assertions.assertEquals("JmxConnection", failure.fields().get("metric_name"));
        Assertions.assertEquals("orders", row(rows, KafkaIotDBSchema.GROUP_PARTITION).tags().get("topic"));
    }

    private void assertField(List<KafkaMetricRow> rows, KafkaIotDBSchema.TableDefinition table, String field,
        Object expected) {
        Assertions.assertEquals(expected, row(rows, table).fields().get(field));
    }

    private KafkaMetricRow row(List<KafkaMetricRow> rows, KafkaIotDBSchema.TableDefinition table) {
        return rows.stream().filter(row -> row.table() == table).findFirst().orElseThrow();
    }
}
