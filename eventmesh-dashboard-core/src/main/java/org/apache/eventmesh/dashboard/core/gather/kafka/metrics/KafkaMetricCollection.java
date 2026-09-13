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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * One collection cycle, separated by Kafka business dimension.
 */
public final class KafkaMetricCollection {

    private final List<ClusterMetricSample> clusters;
    private final List<BrokerMetricSample> brokers;
    private final List<TopicMetricSample> topics;
    private final List<PartitionMetricSample> partitions;
    private final List<GroupMetricSample> groups;
    private final List<ReplicaMetricSample> replicas;
    private final List<KafkaMetricFailure> failures;

    public KafkaMetricCollection(List<ClusterMetricSample> clusters, List<BrokerMetricSample> brokers,
        List<TopicMetricSample> topics, List<PartitionMetricSample> partitions, List<GroupMetricSample> groups,
        List<ReplicaMetricSample> replicas, List<KafkaMetricFailure> failures) {
        this.clusters = immutableCopy(clusters);
        this.brokers = immutableCopy(brokers);
        this.topics = immutableCopy(topics);
        this.partitions = immutableCopy(partitions);
        this.groups = immutableCopy(groups);
        this.replicas = immutableCopy(replicas);
        this.failures = immutableCopy(failures);
    }

    private static <T> List<T> immutableCopy(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    public List<ClusterMetricSample> clusters() {
        return clusters;
    }

    public List<BrokerMetricSample> brokers() {
        return brokers;
    }

    public List<TopicMetricSample> topics() {
        return topics;
    }

    public List<PartitionMetricSample> partitions() {
        return partitions;
    }

    public List<GroupMetricSample> groups() {
        return groups;
    }

    public List<ReplicaMetricSample> replicas() {
        return replicas;
    }

    public List<KafkaMetricFailure> failures() {
        return failures;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int sampleCount() {
        return clusters.size() + brokers.size() + topics.size() + partitions.size() + groups.size() + replicas.size();
    }

    public static final class Builder {

        private final List<ClusterMetricSample> clusters = new ArrayList<>();

        private final List<BrokerMetricSample> brokers = new ArrayList<>();

        private final List<TopicMetricSample> topics = new ArrayList<>();

        private final List<PartitionMetricSample> partitions = new ArrayList<>();

        private final List<GroupMetricSample> groups = new ArrayList<>();

        private final List<ReplicaMetricSample> replicas = new ArrayList<>();

        private final List<KafkaMetricFailure> failures = new ArrayList<>();

        public Builder add(ClusterMetric metric, BigDecimal value, Instant collectedAt) {
            clusters.add(new ClusterMetricSample(metric, value, collectedAt));
            return this;
        }

        public Builder add(BrokerMetric metric, String broker, BigDecimal value, Instant collectedAt) {
            brokers.add(new BrokerMetricSample(metric, broker, value, collectedAt));
            return this;
        }

        public Builder add(TopicMetric metric, String topic, String sourceBroker, BigDecimal value, Instant collectedAt) {
            topics.add(new TopicMetricSample(metric, topic, Optional.ofNullable(sourceBroker), value, collectedAt));
            return this;
        }

        public Builder add(PartitionMetric metric, TopicPartition topicPartition, BigDecimal value, Instant collectedAt) {
            partitions.add(new PartitionMetricSample(metric, topicPartition, value, collectedAt));
            return this;
        }

        public Builder add(GroupMetric metric, String group, TopicPartition topicPartition, BigDecimal value,
            Instant collectedAt) {
            groups.add(new GroupMetricSample(metric, group, Optional.ofNullable(topicPartition), value, collectedAt));
            return this;
        }

        public Builder add(ReplicaMetric metric, String broker, TopicPartition topicPartition, BigDecimal value,
            Instant collectedAt) {
            replicas.add(new ReplicaMetricSample(metric, broker, topicPartition, value, collectedAt));
            return this;
        }

        public Builder failure(KafkaMetricFailure failure) {
            failures.add(failure);
            return this;
        }

        public Builder addAll(KafkaMetricCollection collection) {
            clusters.addAll(collection.clusters());
            brokers.addAll(collection.brokers());
            topics.addAll(collection.topics());
            partitions.addAll(collection.partitions());
            groups.addAll(collection.groups());
            replicas.addAll(collection.replicas());
            failures.addAll(collection.failures());
            return this;
        }

        public KafkaMetricCollection build() {
            return new KafkaMetricCollection(clusters, brokers, topics, partitions, groups, replicas, failures);
        }
    }
}
