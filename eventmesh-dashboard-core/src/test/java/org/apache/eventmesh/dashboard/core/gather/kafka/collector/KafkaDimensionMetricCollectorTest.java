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

import org.apache.eventmesh.dashboard.core.gather.jmx.JmxConnectionConfig;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.BrokerMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricDimension;

import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

public class KafkaDimensionMetricCollectorTest {

    private static final Instant COLLECTED_AT = Instant.parse("2026-08-30T00:00:00Z");

    @Test
    public void testHasExactlyOneCollectorForEveryDimension() {
        List<KafkaDimensionMetricCollector> collectors = List.of(new KafkaClusterMetricCollector(),
            new KafkaBrokerMetricCollector(), new KafkaTopicMetricCollector(), new KafkaPartitionMetricCollector(),
            new KafkaGroupMetricCollector(), new KafkaReplicaMetricCollector());

        Set<KafkaMetricDimension> dimensions = collectors.stream().map(KafkaDimensionMetricCollector::dimension)
            .collect(Collectors.toSet());
        Assert.assertEquals(EnumSet.allOf(KafkaMetricDimension.class), dimensions);
        Assert.assertEquals(dimensions.size(), collectors.size());
    }

    @Test
    public void testOfflineBrokerTopologyMetricsDoNotDependOnJmx() {
        TopicPartition topicPartition = new TopicPartition("orders", 0);
        KafkaClusterMetadata metadata = new KafkaClusterMetadata(Set.of(1), 1,
            Map.of(topicPartition, new KafkaClusterMetadata.PartitionMetadata(1, Set.of(1, 2), Set.of(1))),
            List.of(), COLLECTED_AT);
        List<KafkaBrokerJmxEndpoint> endpoints = List.of(endpoint(1, 9999), endpoint(2, 9998));
        KafkaMetricCollectionContext context = KafkaMetricCollectionContext.create(KafkaMetricCollection.builder().build(),
            KafkaMetricCollection.builder().build(), metadata, endpoints, COLLECTED_AT);
        KafkaMetricCollection.Builder result = KafkaMetricCollection.builder();

        new KafkaBrokerMetricCollector().collect(context, result);
        KafkaMetricCollection metrics = result.build();

        assertBrokerMetric(metrics, "2", BrokerMetric.ALIVE, BigDecimal.ZERO);
        assertBrokerMetric(metrics, "2", BrokerMetric.PARTITIONS, BigDecimal.ONE);
        assertBrokerMetric(metrics, "2", BrokerMetric.LEADERS, BigDecimal.ZERO);
    }

    private KafkaBrokerJmxEndpoint endpoint(int broker, int port) {
        return new KafkaBrokerJmxEndpoint(broker, JmxConnectionConfig.builder("127.0.0.1", port).build());
    }

    private void assertBrokerMetric(KafkaMetricCollection collection, String broker, BrokerMetric metric,
        BigDecimal expected) {
        Assert.assertTrue(collection.brokers().stream().anyMatch(sample -> sample.broker().equals(broker)
            && sample.metric() == metric && sample.value().compareTo(expected) == 0));
    }
}
