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

import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricDimension;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.PartitionMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Produces all Partition-dimension samples from offsets, leader traffic and leader replica size. */
final class KafkaPartitionMetricCollector implements KafkaDimensionMetricCollector {

    private static final MathContext DIVISION_CONTEXT = MathContext.DECIMAL64;

    @Override
    public KafkaMetricDimension dimension() {
        return KafkaMetricDimension.PARTITION;
    }

    @Override
    public void collect(KafkaMetricCollectionContext context, KafkaMetricCollection.Builder result) {
        context.adminMetrics().partitions().forEach(sample -> result.add(sample.metric(), sample.topicPartition(),
            sample.value(), sample.collectedAt()));
        Map<BrokerTopicKey, Long> leaderCounts = new HashMap<>();
        context.metadata().partitions().forEach((partition, item) -> {
            if (item.leader() != null) {
                leaderCounts.merge(new BrokerTopicKey(item.leader().toString(), partition.topic()), 1L, Long::sum);
            }
        });
        context.metadata().partitions().forEach((partition, item) -> {
            if (item.leader() == null) {
                return;
            }
            String broker = item.leader().toString();
            long leaderCount = leaderCounts.getOrDefault(new BrokerTopicKey(broker, partition.topic()), 0L);
            addRate(result, context, PartitionMetric.BYTES_IN, TopicMetric.BYTES_IN, partition, broker, leaderCount);
            addRate(result, context, PartitionMetric.BYTES_OUT, TopicMetric.BYTES_OUT, partition, broker, leaderCount);
            BigDecimal logSize = context.replicaValues().get(new KafkaMetricCollectionContext.ReplicaMetricKey(
                broker, partition, ReplicaMetric.LOG_SIZE));
            if (logSize != null) {
                result.add(PartitionMetric.LOG_SIZE, partition, logSize, context.collectedAt());
            }
        });
    }

    private void addRate(KafkaMetricCollection.Builder result, KafkaMetricCollectionContext context,
        PartitionMetric partitionMetric, TopicMetric topicMetric, TopicPartition partition, String broker,
        long leaderCount) {
        BigDecimal value = context.topicContributions().get(new KafkaMetricCollectionContext.TopicMetricKey(
            broker, partition.topic(), topicMetric));
        if (leaderCount > 0) {
            BigDecimal total = value == null ? BigDecimal.ZERO : value;
            result.add(partitionMetric, partition, total.divide(BigDecimal.valueOf(leaderCount), DIVISION_CONTEXT),
                context.collectedAt());
        }
    }

    private static final class BrokerTopicKey {
        private final String broker;
        private final String topic;

        private BrokerTopicKey(String broker, String topic) {
            this.broker = broker;
            this.topic = topic;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof BrokerTopicKey)) {
                return false;
            }
            BrokerTopicKey that = (BrokerTopicKey) object;
            return Objects.equals(broker, that.broker) && Objects.equals(topic, that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(broker, topic);
        }
    }
}
