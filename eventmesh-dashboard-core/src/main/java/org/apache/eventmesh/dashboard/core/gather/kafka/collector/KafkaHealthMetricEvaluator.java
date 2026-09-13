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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Applies the built-in Kafka availability and replication health checks.
 */
final class KafkaHealthMetricEvaluator {

    private static final int GOOD = 0;

    private static final int MEDIUM = 1;

    private static final int POOR = 2;

    private static final int DEAD = 3;

    KafkaMetricCollection evaluate(KafkaMetricCollection base,
        KafkaClusterMetadata metadata, Instant collectedAt) {
        KafkaMetricCollection.Builder result = KafkaMetricCollection.builder().addAll(base);
        Map<String, Map<BrokerMetric, BigDecimal>> brokers = brokerValues(base);
        Map<String, Map<TopicMetric, BigDecimal>> topics = topicValues(base);
        Map<String, Map<GroupMetric, BigDecimal>> groups = groupValues(base);
        metadata.partitions().keySet().stream().map(TopicPartition::topic)
            .forEach(topic -> topics.computeIfAbsent(topic, ignored -> new HashMap<>()));

        HealthSummary brokerHealth = addBrokerHealth(result, brokers, collectedAt);
        HealthSummary topicHealth = addTopicHealth(result, topics, metadata, collectedAt);
        HealthSummary groupHealth = addGroupHealth(result, groups, collectedAt);
        HealthSummary clusterHealth = clusterHealth(base, metadata);
        addClusterHealth(result, brokerHealth, topicHealth, groupHealth, clusterHealth, metadata, collectedAt);
        return result.build();
    }

    private HealthSummary addBrokerHealth(KafkaMetricCollection.Builder result,
        Map<String, Map<BrokerMetric, BigDecimal>> brokers, Instant collectedAt) {
        HealthSummary summary = new HealthSummary();
        brokers.forEach((broker, values) -> {
            boolean alive = positive(values.get(BrokerMetric.ALIVE));
            boolean noUnderReplicated = values.containsKey(BrokerMetric.PARTITION_URP)
                && zero(values.get(BrokerMetric.PARTITION_URP));
            boolean noUnderMinIsr = values.containsKey(BrokerMetric.PARTITION_MIN_ISR_S)
                && zero(values.get(BrokerMetric.PARTITION_MIN_ISR_S));
            int passed = count(alive, noUnderReplicated, noUnderMinIsr);
            int state = !alive ? DEAD : passed == 3 ? GOOD : MEDIUM;
            result.add(BrokerMetric.HEALTH_STATE, broker, number(state), collectedAt);
            result.add(BrokerMetric.HEALTH_CHECK_PASSED, broker, number(passed), collectedAt);
            result.add(BrokerMetric.HEALTH_CHECK_TOTAL, broker, number(3), collectedAt);
            summary.add(state, passed, 3);
        });
        return summary;
    }

    private HealthSummary addTopicHealth(KafkaMetricCollection.Builder result,
        Map<String, Map<TopicMetric, BigDecimal>> topics, KafkaClusterMetadata metadata,
        Instant collectedAt) {
        Map<String, Long> noLeader = new HashMap<>();
        metadata.partitions().forEach((partition, item) -> {
            if (item.leader() == null) {
                noLeader.merge(partition.topic(), 1L, Long::sum);
            }
        });
        HealthSummary summary = new HealthSummary();
        topics.forEach((topic, values) -> {
            boolean hasLeader = noLeader.getOrDefault(topic, 0L) == 0L;
            boolean noUnderReplicated = values.containsKey(TopicMetric.PARTITION_URP)
                && zero(values.get(TopicMetric.PARTITION_URP));
            int passed = count(hasLeader, noUnderReplicated);
            int state = !hasLeader ? POOR : passed == 2 ? GOOD : MEDIUM;
            result.add(TopicMetric.HEALTH_STATE, topic, null, number(state), collectedAt);
            result.add(TopicMetric.HEALTH_CHECK_PASSED, topic, null, number(passed), collectedAt);
            result.add(TopicMetric.HEALTH_CHECK_TOTAL, topic, null, number(2), collectedAt);
            summary.add(state, passed, 2);
        });
        return summary;
    }

    private HealthSummary addGroupHealth(KafkaMetricCollection.Builder result,
        Map<String, Map<GroupMetric, BigDecimal>> groups, Instant collectedAt) {
        HealthSummary summary = new HealthSummary();
        groups.forEach((group, values) -> {
            BigDecimal groupState = values.get(GroupMetric.STATE);
            boolean notDead = groupState == null || groupState.intValue() != 4;
            int passed = notDead ? 1 : 0;
            int state = notDead ? GOOD : DEAD;
            result.add(GroupMetric.HEALTH_STATE, group, null, number(state), collectedAt);
            result.add(GroupMetric.HEALTH_CHECK_PASSED, group, null, number(passed), collectedAt);
            result.add(GroupMetric.HEALTH_CHECK_TOTAL, group, null, BigDecimal.ONE, collectedAt);
            summary.add(state, passed, 1);
        });
        return summary;
    }

    private HealthSummary clusterHealth(KafkaMetricCollection base,
        KafkaClusterMetadata metadata) {
        Map<ClusterMetric, BigDecimal> values = new HashMap<>();
        base.clusters().forEach(sample -> values.put(sample.metric(), sample.value()));
        boolean alive = metadata.controllerId() != null && !metadata.brokerIds().isEmpty();
        boolean noLeader = values.containsKey(ClusterMetric.PARTITION_NO_LEADER)
            && zero(values.get(ClusterMetric.PARTITION_NO_LEADER));
        boolean noUnderReplicated = values.containsKey(ClusterMetric.PARTITION_URP)
            && zero(values.get(ClusterMetric.PARTITION_URP));
        boolean noUnderMinIsr = values.containsKey(ClusterMetric.PARTITION_MIN_ISR_S)
            && zero(values.get(ClusterMetric.PARTITION_MIN_ISR_S));
        int passed = count(alive, noLeader, noUnderReplicated, noUnderMinIsr);
        int state = !alive ? DEAD : !noLeader ? POOR : passed == 4 ? GOOD : MEDIUM;
        return new HealthSummary(state, passed, 4);
    }

    private void addClusterHealth(KafkaMetricCollection.Builder result, HealthSummary brokerHealth,
        HealthSummary topicHealth, HealthSummary groupHealth, HealthSummary clusterHealth,
        KafkaClusterMetadata metadata, Instant collectedAt) {
        result.add(ClusterMetric.ALIVE,
            metadata.controllerId() != null && !metadata.brokerIds().isEmpty() ? BigDecimal.ONE : BigDecimal.ZERO,
            collectedAt);
        addSummary(result, ClusterMetric.HEALTH_STATE_BROKERS, ClusterMetric.HEALTH_CHECK_PASSED_BROKERS,
            ClusterMetric.HEALTH_CHECK_TOTAL_BROKERS, brokerHealth, collectedAt);
        addSummary(result, ClusterMetric.HEALTH_STATE_TOPICS, ClusterMetric.HEALTH_CHECK_PASSED_TOPICS,
            ClusterMetric.HEALTH_CHECK_TOTAL_TOPICS, topicHealth, collectedAt);
        addSummary(result, ClusterMetric.HEALTH_STATE_GROUPS, ClusterMetric.HEALTH_CHECK_PASSED_GROUPS,
            ClusterMetric.HEALTH_CHECK_TOTAL_GROUPS, groupHealth, collectedAt);
        addSummary(result, ClusterMetric.HEALTH_STATE_CLUSTER, ClusterMetric.HEALTH_CHECK_PASSED_CLUSTER,
            ClusterMetric.HEALTH_CHECK_TOTAL_CLUSTER, clusterHealth, collectedAt);

        HealthSummary overall = new HealthSummary();
        overall.merge(brokerHealth);
        overall.merge(topicHealth);
        overall.merge(groupHealth);
        overall.merge(clusterHealth);
        addSummary(result, ClusterMetric.HEALTH_STATE, ClusterMetric.HEALTH_CHECK_PASSED,
            ClusterMetric.HEALTH_CHECK_TOTAL, overall, collectedAt);
    }

    private void addSummary(KafkaMetricCollection.Builder result, ClusterMetric stateMetric,
        ClusterMetric passedMetric, ClusterMetric totalMetric, HealthSummary summary, Instant collectedAt) {
        result.add(stateMetric, number(summary.state), collectedAt);
        result.add(passedMetric, number(summary.passed), collectedAt);
        result.add(totalMetric, number(summary.total), collectedAt);
    }

    private Map<String, Map<BrokerMetric, BigDecimal>> brokerValues(KafkaMetricCollection collection) {
        Map<String, Map<BrokerMetric, BigDecimal>> values = new LinkedHashMap<>();
        collection.brokers().forEach(sample -> values.computeIfAbsent(sample.broker(), ignored -> new HashMap<>())
            .put(sample.metric(), sample.value()));
        return values;
    }

    private Map<String, Map<TopicMetric, BigDecimal>> topicValues(KafkaMetricCollection collection) {
        Map<String, Map<TopicMetric, BigDecimal>> values = new LinkedHashMap<>();
        collection.topics().forEach(sample -> values.computeIfAbsent(sample.topic(), ignored -> new HashMap<>())
            .put(sample.metric(), sample.value()));
        return values;
    }

    private Map<String, Map<GroupMetric, BigDecimal>> groupValues(KafkaMetricCollection collection) {
        Map<String, Map<GroupMetric, BigDecimal>> values = new LinkedHashMap<>();
        collection.groups().forEach(sample -> values.computeIfAbsent(sample.group(), ignored -> new HashMap<>())
            .put(sample.metric(), sample.value()));
        return values;
    }

    private boolean positive(BigDecimal value) {
        return value != null && value.signum() > 0;
    }

    private boolean zero(BigDecimal value) {
        return value == null || value.signum() == 0;
    }

    private int count(boolean... checks) {
        int passed = 0;
        for (boolean check : checks) {
            if (check) {
                passed++;
            }
        }
        return passed;
    }

    private BigDecimal number(long value) {
        return BigDecimal.valueOf(value);
    }

    private static final class HealthSummary {

        private int state = GOOD;

        private int passed;

        private int total;

        private HealthSummary() {
        }

        private HealthSummary(int state, int passed, int total) {
            this.state = state;
            this.passed = passed;
            this.total = total;
        }

        private void add(int itemState, int itemPassed, int itemTotal) {
            state = Math.max(state, itemState);
            passed += itemPassed;
            total += itemTotal;
        }

        private void merge(HealthSummary summary) {
            add(summary.state, summary.passed, summary.total);
        }
    }
}
