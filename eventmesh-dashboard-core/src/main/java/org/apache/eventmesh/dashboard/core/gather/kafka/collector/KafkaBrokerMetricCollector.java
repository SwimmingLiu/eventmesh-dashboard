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
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricDimension;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashMap;
import java.util.Map;

/** Produces all Broker-dimension samples from JMX values and AdminClient topology. */
final class KafkaBrokerMetricCollector implements KafkaDimensionMetricCollector {

    private static final MathContext DIVISION_CONTEXT = MathContext.DECIMAL64;

    @Override
    public KafkaMetricDimension dimension() {
        return KafkaMetricDimension.BROKER;
    }

    @Override
    public void collect(KafkaMetricCollectionContext context, KafkaMetricCollection.Builder result) {
        Map<KafkaMetricCollectionContext.BrokerMetricKey, BigDecimal> values =
            new HashMap<>(context.brokerValues());
        Map<String, Long> partitionCounts = new HashMap<>();
        Map<String, Long> leaderCounts = new HashMap<>();
        context.metadata().partitions().forEach((ignored, partition) -> {
            partition.replicas().forEach(broker -> partitionCounts.merge(broker.toString(), 1L, Long::sum));
            if (partition.leader() != null) {
                leaderCounts.merge(partition.leader().toString(), 1L, Long::sum);
            }
        });
        context.brokers().forEach(broker -> {
            values.put(new KafkaMetricCollectionContext.BrokerMetricKey(broker, BrokerMetric.PARTITIONS),
                BigDecimal.valueOf(partitionCounts.getOrDefault(broker, 0L)));
            values.put(new KafkaMetricCollectionContext.BrokerMetricKey(broker, BrokerMetric.LEADERS),
                BigDecimal.valueOf(leaderCounts.getOrDefault(broker, 0L)));
            values.putIfAbsent(
                new KafkaMetricCollectionContext.BrokerMetricKey(broker, BrokerMetric.ACTIVE_CONTROLLER_COUNT),
                state(context.metadata().controllerId() != null
                    && context.metadata().controllerId().toString().equals(broker)));
        });
        values.forEach((key, value) -> result.add(key.metric(), key.broker(), value, context.collectedAt()));

        int brokerCount = context.brokers().size();
        long replicaCount = partitionCounts.values().stream().mapToLong(Long::longValue).sum();
        long leaderCount = leaderCounts.values().stream().mapToLong(Long::longValue).sum();
        BigDecimal averageReplicas = average(replicaCount, brokerCount);
        BigDecimal averageLeaders = average(leaderCount, brokerCount);
        context.brokers().forEach(broker -> {
            boolean alive = context.metadata().brokerIds().contains(Integer.parseInt(broker));
            result.add(BrokerMetric.ALIVE, broker, state(alive), context.collectedAt());
            KafkaMetricCollectionContext.BrokerMetricKey eventQueueKey =
                new KafkaMetricCollectionContext.BrokerMetricKey(broker, BrokerMetric.EVENT_QUEUE_SIZE);
            if (!values.containsKey(eventQueueKey)) {
                result.add(BrokerMetric.EVENT_QUEUE_SIZE, broker, BigDecimal.ZERO, context.collectedAt());
            }
            result.add(BrokerMetric.PARTITIONS_SKEW, broker,
                skew(values.get(new KafkaMetricCollectionContext.BrokerMetricKey(broker, BrokerMetric.PARTITIONS)),
                    averageReplicas), context.collectedAt());
            result.add(BrokerMetric.LEADERS_SKEW, broker,
                skew(values.get(new KafkaMetricCollectionContext.BrokerMetricKey(broker, BrokerMetric.LEADERS)),
                    averageLeaders), context.collectedAt());
        });
    }

    private BigDecimal average(long total, int count) {
        return count == 0 ? BigDecimal.ZERO : BigDecimal.valueOf(total).divide(BigDecimal.valueOf(count), DIVISION_CONTEXT);
    }

    private BigDecimal skew(BigDecimal value, BigDecimal average) {
        if (value == null || average.signum() == 0) {
            return BigDecimal.ZERO;
        }
        return value.subtract(average).divide(average, DIVISION_CONTEXT);
    }

    private BigDecimal state(boolean value) {
        return value ? BigDecimal.ONE : BigDecimal.ZERO;
    }
}
