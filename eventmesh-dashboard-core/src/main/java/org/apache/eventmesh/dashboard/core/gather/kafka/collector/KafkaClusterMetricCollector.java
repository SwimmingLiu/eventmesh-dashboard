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
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricDimension;

import java.math.BigDecimal;
import java.util.EnumMap;
import java.util.Map;

/** Produces all Cluster-dimension samples by aggregating the lower dimensions. */
final class KafkaClusterMetricCollector implements KafkaDimensionMetricCollector {

    @Override
    public KafkaMetricDimension dimension() {
        return KafkaMetricDimension.CLUSTER;
    }

    @Override
    public void collect(KafkaMetricCollectionContext context, KafkaMetricCollection.Builder result) {
        context.adminMetrics().clusters().forEach(sample -> result.add(sample.metric(), sample.value(),
            sample.collectedAt()));
        Map<BrokerMetric, ClusterMetric> aggregations = new EnumMap<>(BrokerMetric.class);
        aggregations.put(BrokerMetric.TOTAL_REQUEST_QUEUE_SIZE, ClusterMetric.TOTAL_REQUEST_QUEUE_SIZE);
        aggregations.put(BrokerMetric.TOTAL_RESPONSE_QUEUE_SIZE, ClusterMetric.TOTAL_RESPONSE_QUEUE_SIZE);
        aggregations.put(BrokerMetric.TOTAL_PRODUCE_REQUESTS, ClusterMetric.TOTAL_PRODUCE_REQUESTS);
        aggregations.put(BrokerMetric.CONNECTIONS_COUNT, ClusterMetric.CONNECTIONS_COUNT);
        aggregations.put(BrokerMetric.PARTITION_MIN_ISR_S, ClusterMetric.PARTITION_MIN_ISR_S);
        aggregations.put(BrokerMetric.PARTITION_MIN_ISR_E, ClusterMetric.PARTITION_MIN_ISR_E);
        aggregations.put(BrokerMetric.PARTITION_URP, ClusterMetric.PARTITION_URP);
        aggregations.put(BrokerMetric.MESSAGES_IN, ClusterMetric.MESSAGES_IN);
        aggregations.put(BrokerMetric.LOG_SIZE, ClusterMetric.TOTAL_LOG_SIZE);
        aggregations.put(BrokerMetric.BYTES_IN, ClusterMetric.BYTES_IN);
        aggregations.put(BrokerMetric.BYTES_IN_MIN_5, ClusterMetric.BYTES_IN_MIN_5);
        aggregations.put(BrokerMetric.BYTES_IN_MIN_15, ClusterMetric.BYTES_IN_MIN_15);
        aggregations.put(BrokerMetric.BYTES_OUT, ClusterMetric.BYTES_OUT);
        aggregations.put(BrokerMetric.BYTES_OUT_MIN_5, ClusterMetric.BYTES_OUT_MIN_5);
        aggregations.put(BrokerMetric.BYTES_OUT_MIN_15, ClusterMetric.BYTES_OUT_MIN_15);
        aggregations.forEach((brokerMetric, clusterMetric) -> {
            BigDecimal total = context.brokerValues().entrySet().stream()
                .filter(entry -> entry.getKey().metric() == brokerMetric)
                .map(Map.Entry::getValue).reduce(BigDecimal.ZERO, BigDecimal::add);
            if (context.brokerValues().keySet().stream().anyMatch(key -> key.metric() == brokerMetric)) {
                result.add(clusterMetric, total, context.collectedAt());
            }
        });
        BigDecimal eventQueueSize = context.brokerValues().entrySet().stream()
            .filter(entry -> entry.getKey().metric() == BrokerMetric.EVENT_QUEUE_SIZE)
            .map(Map.Entry::getValue).reduce(BigDecimal.ZERO, BigDecimal::add);
        result.add(ClusterMetric.EVENT_QUEUE_SIZE, eventQueueSize, context.collectedAt());
        result.add(ClusterMetric.ACTIVE_CONTROLLER_COUNT,
            context.metadata().controllerId() == null ? BigDecimal.ZERO : BigDecimal.ONE, context.collectedAt());
        result.add(ClusterMetric.LOAD_REBALANCE_ENABLE, BigDecimal.ZERO, context.collectedAt());
        result.add(ClusterMetric.LOAD_REBALANCE_NW_IN, BigDecimal.ZERO, context.collectedAt());
        result.add(ClusterMetric.LOAD_REBALANCE_NW_OUT, BigDecimal.ZERO, context.collectedAt());
        result.add(ClusterMetric.LOAD_REBALANCE_DISK, BigDecimal.ZERO, context.collectedAt());
    }
}
