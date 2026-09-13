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

package org.apache.eventmesh.dashboard.core.gather.kafka.metrics;

import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.Assert;
import org.junit.Test;

public class KafkaMetricCollectionTest {

    private static final Instant COLLECTED_AT = Instant.parse("2026-08-30T00:00:00Z");

    @Test
    public void testSamplesAreSeparatedByDimension() {
        TopicPartition partition = new TopicPartition("orders", 0);
        KafkaMetricCollection result = KafkaMetricCollection.builder()
            .add(ClusterMetric.ALIVE, BigDecimal.ONE, COLLECTED_AT)
            .add(BrokerMetric.ALIVE, "broker-1", BigDecimal.ONE, COLLECTED_AT)
            .add(TopicMetric.MESSAGES, "orders", null, BigDecimal.TEN, COLLECTED_AT)
            .add(PartitionMetric.MESSAGES, partition, BigDecimal.TEN, COLLECTED_AT)
            .add(GroupMetric.LAG, "orders-reader", partition, BigDecimal.ONE, COLLECTED_AT)
            .add(ReplicaMetric.LOG_SIZE, "broker-1", partition, BigDecimal.TEN, COLLECTED_AT)
            .build();

        Assert.assertEquals(6, result.sampleCount());
        Assert.assertEquals(ClusterMetric.ALIVE, result.clusters().get(0).metric());
        Assert.assertEquals(BrokerMetric.ALIVE, result.brokers().get(0).metric());
        Assert.assertEquals(TopicMetric.MESSAGES, result.topics().get(0).metric());
        Assert.assertEquals(PartitionMetric.MESSAGES, result.partitions().get(0).metric());
        Assert.assertEquals(GroupMetric.LAG, result.groups().get(0).metric());
        Assert.assertEquals(ReplicaMetric.LOG_SIZE, result.replicas().get(0).metric());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGroupLagRequiresPartition() {
        KafkaMetricCollection.builder().add(GroupMetric.LAG, "orders-reader", null, BigDecimal.ONE, COLLECTED_AT);
    }
}
