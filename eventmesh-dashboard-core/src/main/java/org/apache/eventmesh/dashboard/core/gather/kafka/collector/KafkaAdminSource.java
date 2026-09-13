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

import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ClusterMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.GroupMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricFailure;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.PartitionMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.Value;
import lombok.experimental.Accessors;

final class KafkaAdminSource {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(15);

    private final Duration timeout;

    KafkaAdminSource() {
        this(DEFAULT_TIMEOUT);
    }

    KafkaAdminSource(Duration timeout) {
        this.timeout = Objects.requireNonNull(timeout, "timeout");
    }

    KafkaAdminSnapshot collect(Admin admin, Instant collectedAt) {
        Objects.requireNonNull(admin, "admin");
        Objects.requireNonNull(collectedAt, "collectedAt");
        KafkaMetricCollection.Builder result = KafkaMetricCollection.builder();
        ClusterIdentity identity = collectClusterIdentity(admin);
        PartitionSnapshot partitions = collectPartitions(admin, collectedAt, result);
        collectGroups(admin, partitions.latestOffsets(), collectedAt, result);
        KafkaClusterMetadata metadata = new KafkaClusterMetadata(identity.brokerIds(), identity.controllerId(),
            partitions.metadata(), identity.failures(), collectedAt);
        return new KafkaAdminSnapshot(result.build(), metadata);
    }

    private ClusterIdentity collectClusterIdentity(Admin admin) {
        try {
            DescribeClusterResult cluster = admin.describeCluster();
            Collection<Node> nodes = cluster.nodes().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Node controller = cluster.controller().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Set<Integer> brokerIds = nodes.stream().map(Node::id).collect(Collectors.toSet());
            return new ClusterIdentity(brokerIds, controller == null ? null : controller.id(),
                Collections.<KafkaMetricFailure>emptyList());
        } catch (Exception ex) {
            restoreInterrupt(ex);
            return new ClusterIdentity(Collections.<Integer>emptySet(), null,
                Collections.singletonList(failure("ClusterMetadata", "Kafka AdminClient", ex)));
        }
    }

    private PartitionSnapshot collectPartitions(Admin admin, Instant collectedAt,
        KafkaMetricCollection.Builder result) {
        PartitionTopology topology = collectPartitionTopology(admin, collectedAt, result);
        if (!topology.available()) {
            return new PartitionSnapshot(Map.of(), topology.metadata());
        }
        return new PartitionSnapshot(collectPartitionOffsets(admin, topology, collectedAt, result),
            topology.metadata());
    }

    private PartitionTopology collectPartitionTopology(Admin admin, Instant collectedAt,
        KafkaMetricCollection.Builder result) {
        try {
            Set<String> topicNames = admin.listTopics(new ListTopicsOptions().listInternal(true)).names()
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Map<String, TopicDescription> topics = admin.describeTopics(topicNames).allTopicNames()
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Map<TopicPartition, OffsetSpec> earliestRequest = new LinkedHashMap<>();
            Map<TopicPartition, OffsetSpec> latestRequest = new LinkedHashMap<>();
            Map<TopicPartition, KafkaClusterMetadata.PartitionMetadata> partitionMetadata = new LinkedHashMap<>();
            long noLeader = 0;
            for (TopicDescription topic : topics.values()) {
                for (TopicPartitionInfo partition : topic.partitions()) {
                    TopicPartition key = new TopicPartition(topic.name(), partition.partition());
                    Integer leader = partition.leader() == null ? null : partition.leader().id();
                    Set<Integer> replicas = partition.replicas().stream().map(Node::id).collect(Collectors.toSet());
                    Set<Integer> inSyncReplicas = partition.isr().stream().map(Node::id).collect(Collectors.toSet());
                    partitionMetadata.put(key,
                        new KafkaClusterMetadata.PartitionMetadata(leader, replicas, inSyncReplicas));
                    if (partition.leader() == null) {
                        noLeader++;
                        continue;
                    }
                    earliestRequest.put(key, OffsetSpec.earliest());
                    latestRequest.put(key, OffsetSpec.latest());
                }
            }
            result.add(ClusterMetric.PARTITION_NO_LEADER, number(noLeader), collectedAt);
            return new PartitionTopology(earliestRequest, latestRequest, partitionMetadata, true);
        } catch (Exception ex) {
            restoreInterrupt(ex);
            result.failure(failure("PartitionTopology", "Kafka AdminClient", ex));
            return new PartitionTopology(Map.of(), Map.of(), Map.of(), false);
        }
    }

    private Map<TopicPartition, Long> collectPartitionOffsets(Admin admin, PartitionTopology topology,
        Instant collectedAt, KafkaMetricCollection.Builder result) {
        try {
            if (topology.latestRequest().isEmpty()) {
                result.add(ClusterMetric.LEADER_MESSAGES, BigDecimal.ZERO, collectedAt);
                return Map.of();
            }
            Map<TopicPartition, ListOffsetsResultInfo> earliest = admin.listOffsets(topology.earliestRequest()).all()
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Map<TopicPartition, ListOffsetsResultInfo> latest = admin.listOffsets(topology.latestRequest()).all()
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Map<TopicPartition, Long> latestValues = new HashMap<>();
            Map<String, Long> topicMessages = new HashMap<>();
            long clusterMessages = 0;
            for (Map.Entry<TopicPartition, ListOffsetsResultInfo> entry : latest.entrySet()) {
                TopicPartition partition = entry.getKey();
                long start = earliest.get(partition).offset();
                long end = entry.getValue().offset();
                long messages = Math.max(0L, end - start);
                latestValues.put(partition, end);
                topicMessages.merge(partition.topic(), messages, Long::sum);
                clusterMessages += messages;
                result.add(PartitionMetric.LOG_START_OFFSET, partition, number(start), collectedAt);
                result.add(PartitionMetric.LOG_END_OFFSET, partition, number(end), collectedAt);
                result.add(PartitionMetric.MESSAGES, partition, number(messages), collectedAt);
            }
            for (Map.Entry<String, Long> entry : topicMessages.entrySet()) {
                result.add(TopicMetric.MESSAGES, entry.getKey(), null, number(entry.getValue()), collectedAt);
            }
            result.add(ClusterMetric.LEADER_MESSAGES, number(clusterMessages), collectedAt);
            return latestValues;
        } catch (Exception ex) {
            restoreInterrupt(ex);
            result.failure(failure("PartitionOffsets", "Kafka AdminClient", ex));
            return Map.of();
        }
    }

    private void collectGroups(Admin admin, Map<TopicPartition, Long> latestOffsets, Instant collectedAt,
        KafkaMetricCollection.Builder result) {
        Collection<ConsumerGroupListing> groups;
        try {
            groups = admin.listConsumerGroups().all().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            restoreInterrupt(ex);
            result.failure(failure("GroupMetrics", "Kafka AdminClient", ex));
            return;
        }
        Map<String, Long> stateCounts = new HashMap<>();
        for (ConsumerGroupListing group : groups) {
            String state = group.state().map(Enum::name).orElse("UNKNOWN");
            stateCounts.merge(state, 1L, Long::sum);
            result.add(GroupMetric.STATE, group.groupId(), null, number(groupStateCode(state)), collectedAt);
            collectGroupOffsets(admin, group.groupId(), latestOffsets, collectedAt, result);
        }
        result.add(ClusterMetric.GROUP_ACTIVES, number(stateCounts.getOrDefault("STABLE", 0L)), collectedAt);
        result.add(ClusterMetric.GROUP_EMPTYS, number(stateCounts.getOrDefault("EMPTY", 0L)), collectedAt);
        result.add(ClusterMetric.GROUP_REBALANCES,
            number(stateCounts.getOrDefault("PREPARING_REBALANCE", 0L)), collectedAt);
        result.add(ClusterMetric.GROUP_DEADS, number(stateCounts.getOrDefault("DEAD", 0L)), collectedAt);
    }

    private void collectGroupOffsets(Admin admin, String groupId, Map<TopicPartition, Long> knownLatest, Instant collectedAt,
        KafkaMetricCollection.Builder result) {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Map<TopicPartition, Long> latest = new HashMap<>(knownLatest);
            Map<TopicPartition, OffsetSpec> missing = new HashMap<>();
            committed.keySet().stream().filter(partition -> !latest.containsKey(partition))
                .forEach(partition -> missing.put(partition, OffsetSpec.latest()));
            if (!missing.isEmpty()) {
                admin.listOffsets(missing).all().get(timeout.toMillis(), TimeUnit.MILLISECONDS)
                    .forEach((partition, info) -> latest.put(partition, info.offset()));
            }
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                Long end = latest.get(entry.getKey());
                if (end == null) {
                    continue;
                }
                long consumed = entry.getValue().offset();
                final long lag = Math.max(0L, end - consumed);
                result.add(GroupMetric.OFFSET_CONSUMED, groupId, entry.getKey(), number(consumed), collectedAt);
                result.add(GroupMetric.LOG_END_OFFSET, groupId, entry.getKey(), number(end), collectedAt);
                result.add(GroupMetric.LAG, groupId, entry.getKey(), number(lag), collectedAt);
            }
        } catch (Exception ex) {
            restoreInterrupt(ex);
            result.failure(failure("GroupOffsets", groupId, ex));
        }
    }

    private BigDecimal number(long value) {
        return BigDecimal.valueOf(value);
    }

    private long groupStateCode(String state) {
        switch (state) {
            case "PREPARING_REBALANCE":
                return 1L;
            case "COMPLETING_REBALANCE":
                return 2L;
            case "STABLE":
                return 3L;
            case "DEAD":
                return 4L;
            case "EMPTY":
                return 5L;
            default:
                return 0L;
        }
    }

    private KafkaMetricFailure failure(String metricName, String objectName, Exception exception) {
        String message = exception.getMessage();
        return new KafkaMetricFailure(metricName, objectName,
            message == null || message.trim().isEmpty() ? exception.getClass().getSimpleName() : message);
    }

    private void restoreInterrupt(Exception exception) {
        if (exception instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    @Value
    @Accessors(fluent = true)
    private static final class ClusterIdentity {

        Set<Integer> brokerIds;

        Integer controllerId;

        List<KafkaMetricFailure> failures;

        private ClusterIdentity(Set<Integer> brokerIds, Integer controllerId,
            List<KafkaMetricFailure> failures) {
            this.brokerIds = Set.copyOf(brokerIds);
            this.controllerId = controllerId;
            this.failures = List.copyOf(failures);
        }
    }

    @Value
    @Accessors(fluent = true)
    private static final class PartitionSnapshot {

        Map<TopicPartition, Long> latestOffsets;

        Map<TopicPartition, KafkaClusterMetadata.PartitionMetadata> metadata;

        private PartitionSnapshot(Map<TopicPartition, Long> latestOffsets,
            Map<TopicPartition, KafkaClusterMetadata.PartitionMetadata> metadata) {
            this.latestOffsets = Map.copyOf(latestOffsets);
            this.metadata = Map.copyOf(metadata);
        }
    }

    @Value
    @Accessors(fluent = true)
    private static final class PartitionTopology {

        Map<TopicPartition, OffsetSpec> earliestRequest;

        Map<TopicPartition, OffsetSpec> latestRequest;

        Map<TopicPartition, KafkaClusterMetadata.PartitionMetadata> metadata;

        boolean available;

        private PartitionTopology(Map<TopicPartition, OffsetSpec> earliestRequest,
            Map<TopicPartition, OffsetSpec> latestRequest,
            Map<TopicPartition, KafkaClusterMetadata.PartitionMetadata> metadata, boolean available) {
            this.earliestRequest = Map.copyOf(earliestRequest);
            this.latestRequest = Map.copyOf(latestRequest);
            this.metadata = Map.copyOf(metadata);
            this.available = available;
        }
    }
}
