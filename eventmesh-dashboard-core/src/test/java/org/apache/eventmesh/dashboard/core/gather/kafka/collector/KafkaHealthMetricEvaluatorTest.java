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

package org.apache.eventmesh.dashboard.core.gather.kafka.collector;

import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.BrokerMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ClusterMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.GroupMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class KafkaHealthMetricEvaluatorTest {

    private static final Instant COLLECTED_AT = Instant.parse("2026-08-30T00:00:00Z");

    @Test
    public void testUnavailableDimensionsProduceDeadOrPoorHealth() {
        TopicPartition partition = new TopicPartition("orders", 0);
        KafkaMetricCollection base = KafkaMetricCollection.builder()
            .add(BrokerMetric.ALIVE, "1", BigDecimal.ZERO, COLLECTED_AT)
            .add(BrokerMetric.PARTITION_URP, "1", BigDecimal.ONE, COLLECTED_AT)
            .add(BrokerMetric.PARTITION_MIN_ISR_S, "1", BigDecimal.ONE, COLLECTED_AT)
            .add(TopicMetric.PARTITION_URP, "orders", null, BigDecimal.ONE, COLLECTED_AT)
            .add(GroupMetric.STATE, "orders-reader", null, BigDecimal.valueOf(4L), COLLECTED_AT)
            .add(ClusterMetric.PARTITION_NO_LEADER, BigDecimal.ONE, COLLECTED_AT)
            .add(ClusterMetric.PARTITION_URP, BigDecimal.ONE, COLLECTED_AT)
            .add(ClusterMetric.PARTITION_MIN_ISR_S, BigDecimal.ONE, COLLECTED_AT)
            .build();
        KafkaClusterMetadata metadata = new KafkaClusterMetadata(
            Set.of(), null, Map.of(partition,
            new KafkaClusterMetadata.PartitionMetadata(null, Set.of(1), Set.of())), List.of(), COLLECTED_AT);

        KafkaMetricCollection result = new KafkaHealthMetricEvaluator().evaluate(base, metadata, COLLECTED_AT);

        assertBrokerMetric(result, BrokerMetric.HEALTH_STATE, 3L);
        assertTopicMetric(result, TopicMetric.HEALTH_STATE, 2L);
        assertGroupMetric(result, GroupMetric.HEALTH_STATE, 3L);
        assertClusterMetric(result, ClusterMetric.HEALTH_STATE_CLUSTER, 3L);
        assertClusterMetric(result, ClusterMetric.HEALTH_STATE, 3L);
        assertClusterMetric(result, ClusterMetric.ALIVE, 0L);
    }

    @Test
    public void testHealthyDimensionsExposePassedAndTotalCounts() {
        TopicPartition partition = new TopicPartition("orders", 0);
        KafkaMetricCollection base = KafkaMetricCollection.builder()
            .add(BrokerMetric.ALIVE, "1", BigDecimal.ONE, COLLECTED_AT)
            .add(BrokerMetric.PARTITION_URP, "1", BigDecimal.ZERO, COLLECTED_AT)
            .add(BrokerMetric.PARTITION_MIN_ISR_S, "1", BigDecimal.ZERO, COLLECTED_AT)
            .add(TopicMetric.PARTITION_URP, "orders", null, BigDecimal.ZERO, COLLECTED_AT)
            .add(GroupMetric.STATE, "orders-reader", null, BigDecimal.valueOf(3L), COLLECTED_AT)
            .add(ClusterMetric.PARTITION_NO_LEADER, BigDecimal.ZERO, COLLECTED_AT)
            .add(ClusterMetric.PARTITION_URP, BigDecimal.ZERO, COLLECTED_AT)
            .add(ClusterMetric.PARTITION_MIN_ISR_S, BigDecimal.ZERO, COLLECTED_AT)
            .build();
        KafkaClusterMetadata metadata = new KafkaClusterMetadata(
            Set.of(1), 1, Map.of(partition,
            new KafkaClusterMetadata.PartitionMetadata(1, Set.of(1), Set.of(1))), List.of(), COLLECTED_AT);

        KafkaMetricCollection result = new KafkaHealthMetricEvaluator().evaluate(base, metadata, COLLECTED_AT);

        assertClusterMetric(result, ClusterMetric.HEALTH_STATE, 0L);
        assertClusterMetric(result, ClusterMetric.HEALTH_CHECK_PASSED, 10L);
        assertClusterMetric(result, ClusterMetric.HEALTH_CHECK_TOTAL, 10L);
        assertBrokerMetric(result, BrokerMetric.HEALTH_CHECK_PASSED, 3L);
        assertTopicMetric(result, TopicMetric.HEALTH_CHECK_PASSED, 2L);
        assertGroupMetric(result, GroupMetric.HEALTH_CHECK_PASSED, 1L);
    }

    @Test
    public void testMissingJmxChecksAreNotCountedAsPassed() {
        TopicPartition partition = new TopicPartition("orders", 0);
        KafkaMetricCollection base = KafkaMetricCollection.builder()
            .add(BrokerMetric.ALIVE, "2", BigDecimal.ZERO, COLLECTED_AT)
            .add(ClusterMetric.PARTITION_NO_LEADER, BigDecimal.ZERO, COLLECTED_AT)
            .build();
        KafkaClusterMetadata metadata = new KafkaClusterMetadata(Set.of(1), 1,
            Map.of(partition, new KafkaClusterMetadata.PartitionMetadata(1, Set.of(1, 2), Set.of(1))),
            List.of(), COLLECTED_AT);

        KafkaMetricCollection result = new KafkaHealthMetricEvaluator().evaluate(base, metadata, COLLECTED_AT);

        assertBrokerMetric(result, BrokerMetric.HEALTH_STATE, 3L);
        assertBrokerMetric(result, BrokerMetric.HEALTH_CHECK_PASSED, 0L);
        assertTopicMetric(result, TopicMetric.HEALTH_CHECK_PASSED, 1L);
        assertClusterMetric(result, ClusterMetric.HEALTH_CHECK_PASSED_CLUSTER, 2L);
    }

    private void assertClusterMetric(KafkaMetricCollection result, ClusterMetric metric, long expected) {
        Assert.assertTrue(result.clusters().stream().anyMatch(sample -> sample.metric() == metric
            && sample.value().compareTo(BigDecimal.valueOf(expected)) == 0));
    }

    private void assertBrokerMetric(KafkaMetricCollection result, BrokerMetric metric, long expected) {
        Assert.assertTrue(result.brokers().stream().anyMatch(sample -> sample.metric() == metric
            && sample.value().compareTo(BigDecimal.valueOf(expected)) == 0));
    }

    private void assertTopicMetric(KafkaMetricCollection result, TopicMetric metric, long expected) {
        Assert.assertTrue(result.topics().stream().anyMatch(sample -> sample.metric() == metric
            && sample.value().compareTo(BigDecimal.valueOf(expected)) == 0));
    }

    private void assertGroupMetric(KafkaMetricCollection result, GroupMetric metric, long expected) {
        Assert.assertTrue(result.groups().stream().anyMatch(sample -> sample.metric() == metric
            && sample.value().compareTo(BigDecimal.valueOf(expected)) == 0));
    }
}
