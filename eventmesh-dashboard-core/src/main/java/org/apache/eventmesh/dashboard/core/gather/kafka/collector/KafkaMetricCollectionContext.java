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
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Immutable inputs shared by the six dimension collectors during one collection cycle.
 */
final class KafkaMetricCollectionContext {

    private final KafkaMetricCollection adminMetrics;
    private final KafkaMetricCollection jmxMetrics;
    private final KafkaClusterMetadata metadata;
    private final Set<String> brokers;
    private final Set<String> topics;
    private final Map<BrokerMetricKey, BigDecimal> brokerValues;
    private final Map<TopicMetricKey, BigDecimal> topicContributions;
    private final Map<ReplicaMetricKey, BigDecimal> replicaValues;
    private final Instant collectedAt;

    private KafkaMetricCollectionContext(KafkaMetricCollection adminMetrics, KafkaMetricCollection jmxMetrics,
        KafkaClusterMetadata metadata, Set<String> brokers, Set<String> topics,
        Map<BrokerMetricKey, BigDecimal> brokerValues, Map<TopicMetricKey, BigDecimal> topicContributions,
        Map<ReplicaMetricKey, BigDecimal> replicaValues, Instant collectedAt) {
        this.adminMetrics = adminMetrics;
        this.jmxMetrics = jmxMetrics;
        this.metadata = metadata;
        this.brokers = Collections.unmodifiableSet(new HashSet<>(brokers));
        this.topics = Collections.unmodifiableSet(new HashSet<>(topics));
        this.brokerValues = Collections.unmodifiableMap(new LinkedHashMap<>(brokerValues));
        this.topicContributions = Collections.unmodifiableMap(new LinkedHashMap<>(topicContributions));
        this.replicaValues = Collections.unmodifiableMap(new LinkedHashMap<>(replicaValues));
        this.collectedAt = collectedAt;
    }

    static KafkaMetricCollectionContext create(KafkaMetricCollection adminMetrics, KafkaMetricCollection jmxMetrics,
        KafkaClusterMetadata metadata, Collection<KafkaBrokerJmxEndpoint> endpoints, Instant collectedAt) {
        Set<String> brokers = new HashSet<>();
        metadata.brokerIds().stream().map(String::valueOf).forEach(brokers::add);
        endpoints.stream().map(KafkaBrokerJmxEndpoint::broker).forEach(brokers::add);
        Set<String> topics = metadata.partitions().keySet().stream().map(TopicPartition::topic)
            .collect(Collectors.toSet());

        Map<BrokerMetricKey, BigDecimal> brokerValues = new LinkedHashMap<>();
        jmxMetrics.brokers().forEach(sample -> brokerValues.merge(
            new BrokerMetricKey(sample.broker(), sample.metric()), sample.value(), BigDecimal::add));
        Map<TopicMetricKey, BigDecimal> topicContributions = new LinkedHashMap<>();
        jmxMetrics.topics().forEach(sample -> topicContributions.merge(
            new TopicMetricKey(sample.sourceBroker().orElse(""), sample.topic(), sample.metric()), sample.value(),
            BigDecimal::add));
        Map<ReplicaMetricKey, BigDecimal> replicaValues = new LinkedHashMap<>();
        jmxMetrics.replicas().forEach(sample -> replicaValues.merge(
            new ReplicaMetricKey(sample.broker(), sample.topicPartition(), sample.metric()), sample.value(),
            BigDecimal::add));
        replaceBrokerLogSizes(brokerValues, replicaValues, metadata);
        return new KafkaMetricCollectionContext(adminMetrics, jmxMetrics, metadata, brokers, topics, brokerValues,
            topicContributions, replicaValues, collectedAt);
    }

    private static void replaceBrokerLogSizes(Map<BrokerMetricKey, BigDecimal> brokerValues,
        Map<ReplicaMetricKey, BigDecimal> replicaValues, KafkaClusterMetadata metadata) {
        brokerValues.keySet().removeIf(key -> key.metric() == BrokerMetric.LOG_SIZE);
        replicaValues.forEach((key, value) -> {
            KafkaClusterMetadata.PartitionMetadata partition = metadata.partitions().get(key.topicPartition());
            if (key.metric() == ReplicaMetric.LOG_SIZE && partition != null
                && partition.replicas().contains(Integer.parseInt(key.broker()))) {
                brokerValues.merge(new BrokerMetricKey(key.broker(), BrokerMetric.LOG_SIZE), value, BigDecimal::add);
            }
        });
    }

    KafkaMetricCollection adminMetrics() {
        return adminMetrics;
    }

    KafkaMetricCollection jmxMetrics() {
        return jmxMetrics;
    }

    KafkaClusterMetadata metadata() {
        return metadata;
    }

    Set<String> brokers() {
        return brokers;
    }

    Set<String> topics() {
        return topics;
    }

    Map<BrokerMetricKey, BigDecimal> brokerValues() {
        return brokerValues;
    }

    Map<TopicMetricKey, BigDecimal> topicContributions() {
        return topicContributions;
    }

    Map<ReplicaMetricKey, BigDecimal> replicaValues() {
        return replicaValues;
    }

    Instant collectedAt() {
        return collectedAt;
    }

    static final class BrokerMetricKey {
        private final String broker;
        private final BrokerMetric metric;

        BrokerMetricKey(String broker, BrokerMetric metric) {
            this.broker = broker;
            this.metric = metric;
        }

        String broker() {
            return broker;
        }

        BrokerMetric metric() {
            return metric;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof BrokerMetricKey)) {
                return false;
            }
            BrokerMetricKey that = (BrokerMetricKey) object;
            return Objects.equals(broker, that.broker) && metric == that.metric;
        }

        @Override
        public int hashCode() {
            return Objects.hash(broker, metric);
        }
    }

    static final class TopicMetricKey {
        private final String sourceBroker;
        private final String topic;
        private final TopicMetric metric;

        TopicMetricKey(String sourceBroker, String topic, TopicMetric metric) {
            this.sourceBroker = sourceBroker;
            this.topic = topic;
            this.metric = metric;
        }

        String sourceBroker() {
            return sourceBroker;
        }

        String topic() {
            return topic;
        }

        TopicMetric metric() {
            return metric;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof TopicMetricKey)) {
                return false;
            }
            TopicMetricKey that = (TopicMetricKey) object;
            return Objects.equals(sourceBroker, that.sourceBroker) && Objects.equals(topic, that.topic)
                && metric == that.metric;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceBroker, topic, metric);
        }
    }

    static final class ReplicaMetricKey {
        private final String broker;
        private final TopicPartition topicPartition;
        private final ReplicaMetric metric;

        ReplicaMetricKey(String broker, TopicPartition topicPartition, ReplicaMetric metric) {
            this.broker = broker;
            this.topicPartition = topicPartition;
            this.metric = metric;
        }

        String broker() {
            return broker;
        }

        TopicPartition topicPartition() {
            return topicPartition;
        }

        ReplicaMetric metric() {
            return metric;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof ReplicaMetricKey)) {
                return false;
            }
            ReplicaMetricKey that = (ReplicaMetricKey) object;
            return Objects.equals(broker, that.broker) && Objects.equals(topicPartition, that.topicPartition)
                && metric == that.metric;
        }

        @Override
        public int hashCode() {
            return Objects.hash(broker, topicPartition, metric);
        }
    }
}
