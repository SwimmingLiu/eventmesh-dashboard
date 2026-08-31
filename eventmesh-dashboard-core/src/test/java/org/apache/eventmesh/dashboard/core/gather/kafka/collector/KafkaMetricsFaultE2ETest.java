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
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ClusterMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import org.apache.kafka.clients.admin.Admin;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/** Real-service assertions for a two-Broker cluster while Broker 2 is unavailable. */
public class KafkaMetricsFaultE2ETest {

    @Test
    public void testOfflineFollowerProducesPartialButTruthfulMetrics() {
        Assume.assumeTrue("Set -Dkafka.metrics.fault.e2e=true to run the fault test",
            Boolean.getBoolean("kafka.metrics.fault.e2e"));
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String topic = System.getProperty("kafka.metrics.fault.topic");
        Assume.assumeNotNull(topic);
        KafkaBrokerJmxEndpoint broker1 = endpoint(1,
            System.getProperty("kafka.broker1.jmx.host", "127.0.0.1"),
            Integer.getInteger("kafka.broker1.jmx.port", 9999));
        KafkaBrokerJmxEndpoint broker2 = endpoint(2,
            System.getProperty("kafka.broker2.jmx.host", "127.0.0.1"),
            Integer.getInteger("kafka.broker2.jmx.port", 9998));

        Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrapServers));
        KafkaMetricCollection metrics;
        long startNanos = System.nanoTime();
        try {
            metrics = new KafkaMetricsCollector().collect(admin, List.of(broker1, broker2));
        } finally {
            admin.close(Duration.ofSeconds(1));
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        Assert.assertTrue("Fault collection took " + elapsedMillis + " ms", elapsedMillis < 20_000L);
        Assert.assertTrue(metrics.failures().stream().anyMatch(failure -> failure.metricName().equals("JmxConnection")
            && failure.objectName().equals("2")));
        assertBroker(metrics, "1", BrokerMetric.ALIVE, BigDecimal.ONE);
        assertBroker(metrics, "1", BrokerMetric.PARTITION_URP, BigDecimal.valueOf(6L));
        assertBroker(metrics, "1", BrokerMetric.PARTITION_MIN_ISR_S, BigDecimal.valueOf(6L));
        assertBroker(metrics, "2", BrokerMetric.ALIVE, BigDecimal.ZERO);
        assertBroker(metrics, "2", BrokerMetric.HEALTH_STATE, BigDecimal.valueOf(3L));
        assertBroker(metrics, "2", BrokerMetric.HEALTH_CHECK_PASSED, BigDecimal.ZERO);
        Assert.assertTrue(brokerValue(metrics, "2", BrokerMetric.PARTITIONS).longValue() >= 6L);
        assertCluster(metrics, ClusterMetric.PARTITION_URP, BigDecimal.valueOf(6L));
        assertCluster(metrics, ClusterMetric.PARTITION_MIN_ISR_S, BigDecimal.valueOf(6L));
        assertTopic(metrics, topic, TopicMetric.PARTITION_URP, BigDecimal.valueOf(6L));
        Assert.assertTrue(topicValue(metrics, topic, TopicMetric.FAILED_PRODUCE_REQUESTS).signum() > 0);
        long offlineReplicas = metrics.replicas().stream().filter(sample -> sample.broker().equals("2")
            && sample.topicPartition().topic().equals(topic) && sample.metric() == ReplicaMetric.IN_SYNC
            && sample.value().compareTo(BigDecimal.ZERO) == 0).count();
        Assert.assertEquals(6L, offlineReplicas);
    }

    private KafkaBrokerJmxEndpoint endpoint(int broker, String host, int port) {
        return new KafkaBrokerJmxEndpoint(broker, JmxConnectionConfig.builder(host, port)
            .connectionTimeoutMillis(500L).requestTimeoutMillis(2_000L).build());
    }

    private void assertBroker(KafkaMetricCollection metrics, String broker, BrokerMetric metric, BigDecimal expected) {
        Assert.assertEquals(0, brokerValue(metrics, broker, metric).compareTo(expected));
    }

    private BigDecimal brokerValue(KafkaMetricCollection metrics, String broker, BrokerMetric metric) {
        return metrics.brokers().stream().filter(sample -> sample.broker().equals(broker)
            && sample.metric() == metric).findFirst().orElseThrow().value();
    }

    private void assertCluster(KafkaMetricCollection metrics, ClusterMetric metric, BigDecimal expected) {
        BigDecimal actual = metrics.clusters().stream().filter(sample -> sample.metric() == metric)
            .findFirst().orElseThrow().value();
        Assert.assertEquals(0, actual.compareTo(expected));
    }

    private void assertTopic(KafkaMetricCollection metrics, String topic, TopicMetric metric, BigDecimal expected) {
        Assert.assertEquals(0, topicValue(metrics, topic, metric).compareTo(expected));
    }

    private BigDecimal topicValue(KafkaMetricCollection metrics, String topic, TopicMetric metric) {
        return metrics.topics().stream().filter(sample -> sample.topic().equals(topic)
            && sample.metric() == metric).findFirst().orElseThrow().value();
    }
}
