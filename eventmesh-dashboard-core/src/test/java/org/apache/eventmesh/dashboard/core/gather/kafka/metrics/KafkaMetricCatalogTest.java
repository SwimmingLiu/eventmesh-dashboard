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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class KafkaMetricCatalogTest {

    @Test
    public void testDefinitionsAreSeparatedByDimension() {
        assertDimension(ClusterMetric.values(), KafkaMetricDimension.CLUSTER);
        assertDimension(BrokerMetric.values(), KafkaMetricDimension.BROKER);
        assertDimension(TopicMetric.values(), KafkaMetricDimension.TOPIC);
        assertDimension(PartitionMetric.values(), KafkaMetricDimension.PARTITION);
        assertDimension(GroupMetric.values(), KafkaMetricDimension.GROUP);
        assertDimension(ReplicaMetric.values(), KafkaMetricDimension.REPLICA);
    }

    @Test
    public void testPartitionAndReplicaOffsetsHaveDifferentSources() {
        Assert.assertEquals(KafkaMetricSource.ADMIN_CLIENT, PartitionMetric.LOG_END_OFFSET.source());
        Assert.assertTrue(PartitionMetric.LOG_END_OFFSET.jmx().isEmpty());
        Assert.assertEquals(KafkaMetricSource.JMX, ReplicaMetric.LOG_END_OFFSET.source());
        Assert.assertTrue(ReplicaMetric.LOG_END_OFFSET.jmx().isPresent());
    }

    @Test
    public void testJmxSourceAlwaysHasDescriptor() {
        Assert.assertTrue(KafkaMetricCatalog.all().stream()
            .allMatch(metric -> (metric.source() == KafkaMetricSource.JMX) == metric.jmx().isPresent()));
    }

    @Test
    public void testKafka45CompatibilityMetricsAreExplicit() {
        Assert.assertEquals(KafkaMetricSupport.LEGACY_JMX_COMPATIBILITY,
            KafkaMetricCatalog.support(BrokerMetric.EVENT_QUEUE_SIZE));
        Assert.assertEquals(KafkaMetricSupport.CUSTOM_KAFKA_COMPATIBILITY,
            KafkaMetricCatalog.support(TopicMetric.MIRROR_FETCH_LAG));
        Assert.assertEquals(KafkaMetricSupport.ENTERPRISE_COMPATIBILITY,
            KafkaMetricCatalog.support(ClusterMetric.LOAD_REBALANCE_ENABLE));
        Assert.assertEquals(104L, KafkaMetricCatalog.all().stream()
            .filter(metric -> KafkaMetricCatalog.support(metric) == KafkaMetricSupport.STANDARD).count());
    }

    private void assertDimension(KafkaMetricDefinition[] metrics, KafkaMetricDimension dimension) {
        Assert.assertTrue(Arrays.stream(metrics).allMatch(metric -> metric.dimension() == dimension));
    }
}
