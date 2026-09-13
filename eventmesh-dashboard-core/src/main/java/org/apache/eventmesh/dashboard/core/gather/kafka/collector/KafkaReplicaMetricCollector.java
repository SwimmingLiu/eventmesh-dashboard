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
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetric;

import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.util.Map;

/** Produces all Replica-dimension samples for each Broker/Topic/Partition identity. */
final class KafkaReplicaMetricCollector implements KafkaDimensionMetricCollector {

    @Override
    public KafkaMetricDimension dimension() {
        return KafkaMetricDimension.REPLICA;
    }

    @Override
    public void collect(KafkaMetricCollectionContext context, KafkaMetricCollection.Builder result) {
        context.replicaValues().forEach((key, value) -> {
            KafkaClusterMetadata.PartitionMetadata partition =
                context.metadata().partitions().get(key.topicPartition());
            if (partition != null && partition.replicas().contains(Integer.parseInt(key.broker()))) {
                result.add(key.metric(), key.broker(), key.topicPartition(), value, context.collectedAt());
            }
        });
        for (Map.Entry<TopicPartition, KafkaClusterMetadata.PartitionMetadata> entry
            : context.metadata().partitions().entrySet()) {
            for (Integer brokerId : entry.getValue().replicas()) {
                String broker = brokerId.toString();
                boolean inSync = entry.getValue().inSyncReplicas().contains(brokerId);
                result.add(ReplicaMetric.IN_SYNC, broker, entry.getKey(), state(inSync), context.collectedAt());
                BigDecimal start = context.replicaValues().get(new KafkaMetricCollectionContext.ReplicaMetricKey(
                    broker, entry.getKey(), ReplicaMetric.LOG_START_OFFSET));
                BigDecimal end = context.replicaValues().get(new KafkaMetricCollectionContext.ReplicaMetricKey(
                    broker, entry.getKey(), ReplicaMetric.LOG_END_OFFSET));
                if (start != null && end != null) {
                    result.add(ReplicaMetric.MESSAGES, broker, entry.getKey(),
                        end.subtract(start).max(BigDecimal.ZERO), context.collectedAt());
                }
            }
        }
    }

    private BigDecimal state(boolean value) {
        return value ? BigDecimal.ONE : BigDecimal.ZERO;
    }
}
