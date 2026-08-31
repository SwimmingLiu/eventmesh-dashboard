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
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.BrokerMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ClusterMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ClusterMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.GroupMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.GroupMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.PartitionMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.PartitionMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetricSample;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetricSample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class KafkaMetricsCollectorE2ETest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    public void testCollectEveryDimensionFromRealKafka() throws Exception {
        Assume.assumeTrue("Set -Dkafka.metrics.e2e=true to run the real-service test",
            Boolean.getBoolean("kafka.metrics.e2e"));
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String jmxHost = System.getProperty("kafka.jmx.host", "127.0.0.1");
        int jmxPort = Integer.getInteger("kafka.jmx.port", 9999);
        String topic = "eventmesh-dashboard-metrics-e2e-" + UUID.randomUUID();
        String group = topic + "-group";

        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrapServers))) {
            int brokerId = onlyBrokerId(admin);
            try {
                admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all()
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                produce(bootstrapServers, topic);
                commitPartialOffset(bootstrapServers, topic, group);

                KafkaBrokerJmxEndpoint endpoint = new KafkaBrokerJmxEndpoint(brokerId,
                    JmxConnectionConfig.builder(jmxHost, jmxPort).build());
                KafkaMetricCollection metrics = awaitCompleteMetrics(admin, endpoint, topic, group);

                Assert.assertTrue(metrics.failures().toString(), metrics.failures().isEmpty());
                assertCoverage(metrics);
                assertExactValues(metrics, topic, group, brokerId);
            } finally {
                admin.deleteTopics(List.of(topic)).all().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                try {
                    admin.deleteConsumerGroups(List.of(group)).all().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                } catch (Exception ignored) {
                    // Kafka may remove the empty test group together with its deleted Topic.
                }
            }
        }
    }

    private int onlyBrokerId(Admin admin) throws Exception {
        List<Integer> brokerIds = admin.describeCluster().nodes().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
            .stream().map(node -> node.id()).toList();
        Assert.assertEquals("The local E2E fixture is expected to contain one Broker", 1, brokerIds.size());
        return brokerIds.get(0);
    }

    private void produce(String bootstrapServers, String topic) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            for (int partition = 0; partition < 3; partition++) {
                for (int index = 0; index < 5; index++) {
                    producer.send(new ProducerRecord<>(topic, partition, null,
                        ("record-" + partition + "-" + index).getBytes())).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }
            }
            producer.flush();
        }
    }

    private void commitPartialOffset(String bootstrapServers, String topic, String group) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            consumer.poll(Duration.ofSeconds(5));
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(3L)));
        }
    }

    private KafkaMetricCollection awaitCompleteMetrics(Admin admin, KafkaBrokerJmxEndpoint endpoint,
        String topic, String group) throws InterruptedException {
        KafkaMetricsCollector collector = new KafkaMetricsCollector();
        KafkaMetricCollection latest = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            latest = collector.collect(admin, List.of(endpoint));
            if (hasExactLag(latest, topic, group) && hasTopicLogSize(latest, topic)) {
                return latest;
            }
            Thread.sleep(500L);
        }
        return latest;
    }

    private boolean hasExactLag(KafkaMetricCollection metrics, String topic, String group) {
        TopicPartition partition = new TopicPartition(topic, 0);
        return metrics.groups().stream().anyMatch(sample -> sample.metric() == GroupMetric.LAG
            && sample.group().equals(group) && sample.topicPartition().orElse(null).equals(partition)
            && sample.value().compareTo(BigDecimal.valueOf(2L)) == 0);
    }

    private boolean hasTopicLogSize(KafkaMetricCollection metrics, String topic) {
        return metrics.topics().stream().anyMatch(sample -> sample.metric() == TopicMetric.LOG_SIZE
            && sample.topic().equals(topic) && sample.value().signum() > 0);
    }

    private void assertCoverage(KafkaMetricCollection metrics) {
        assertEmpty(ClusterMetric.class, metrics.clusters().stream().map(ClusterMetricSample::metric).toList());
        assertEmpty(BrokerMetric.class, metrics.brokers().stream().map(BrokerMetricSample::metric).toList());
        assertEmpty(TopicMetric.class, metrics.topics().stream().map(TopicMetricSample::metric).toList());
        assertEmpty(PartitionMetric.class, metrics.partitions().stream().map(PartitionMetricSample::metric).toList());
        assertEmpty(GroupMetric.class, metrics.groups().stream().map(GroupMetricSample::metric).toList());
        assertEmpty(ReplicaMetric.class, metrics.replicas().stream().map(ReplicaMetricSample::metric).toList());
    }

    private <E extends Enum<E>> void assertEmpty(Class<E> metricClass, List<E> actual) {
        EnumSet<E> missing = EnumSet.allOf(metricClass);
        missing.removeAll(actual);
        Assert.assertTrue("Missing " + metricClass.getSimpleName() + " values: " + missing, missing.isEmpty());
    }

    private void assertExactValues(KafkaMetricCollection metrics, String topic, String group, int brokerId) {
        Map<TopicPartition, BigDecimal> messages = metrics.partitions().stream()
            .filter(sample -> sample.metric() == PartitionMetric.MESSAGES && sample.topicPartition().topic().equals(topic))
            .collect(Collectors.toMap(PartitionMetricSample::topicPartition, PartitionMetricSample::value));
        Assert.assertEquals(3, messages.size());
        messages.values().forEach(value -> Assert.assertEquals(0, BigDecimal.valueOf(5L).compareTo(value)));

        BigDecimal topicMessages = metricValue(metrics.topics().stream()
            .filter(sample -> sample.metric() == TopicMetric.MESSAGES && sample.topic().equals(topic))
            .collect(Collectors.toList()), TopicMetricSample::value);
        Assert.assertEquals(0, BigDecimal.valueOf(15L).compareTo(topicMessages));

        TopicPartition partition = new TopicPartition(topic, 0);
        assertGroupValue(metrics, group, partition, GroupMetric.OFFSET_CONSUMED, 3L);
        assertGroupValue(metrics, group, partition, GroupMetric.LOG_END_OFFSET, 5L);
        assertGroupValue(metrics, group, partition, GroupMetric.LAG, 2L);

        String broker = Integer.toString(brokerId);
        for (int partitionId = 0; partitionId < 3; partitionId++) {
            TopicPartition replica = new TopicPartition(topic, partitionId);
            Assert.assertTrue(metrics.replicas().stream().anyMatch(sample -> sample.metric() == ReplicaMetric.MESSAGES
                && sample.broker().equals(broker) && sample.topicPartition().equals(replica)
                && sample.value().compareTo(BigDecimal.valueOf(5L)) == 0));
            Assert.assertTrue(metrics.replicas().stream().anyMatch(sample -> sample.metric() == ReplicaMetric.IN_SYNC
                && sample.broker().equals(broker) && sample.topicPartition().equals(replica)
                && sample.value().compareTo(BigDecimal.ONE) == 0));
        }
    }

    private void assertGroupValue(KafkaMetricCollection metrics, String group, TopicPartition partition,
        GroupMetric metric, long expected) {
        Assert.assertTrue(metrics.groups().stream().anyMatch(sample -> sample.metric() == metric
            && sample.group().equals(group) && sample.topicPartition().orElse(null).equals(partition)
            && sample.value().compareTo(BigDecimal.valueOf(expected)) == 0));
    }

    private <T> BigDecimal metricValue(List<T> samples, Function<T, BigDecimal> value) {
        Assert.assertEquals(1, samples.size());
        return value.apply(samples.get(0));
    }
}
