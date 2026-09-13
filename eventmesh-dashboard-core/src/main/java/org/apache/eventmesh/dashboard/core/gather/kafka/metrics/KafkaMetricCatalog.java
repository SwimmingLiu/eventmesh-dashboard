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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class KafkaMetricCatalog {

    private static final List<KafkaMetricDefinition> ALL = createAll();

    private KafkaMetricCatalog() {
    }

    public static List<KafkaMetricDefinition> all() {
        return ALL;
    }

    public static List<KafkaMetricDefinition> jmxMetrics() {
        return ALL.stream().filter(metric -> metric.jmx().isPresent()).collect(Collectors.toList());
    }

    public static List<KafkaMetricDefinition> byDimension(KafkaMetricDimension dimension) {
        return ALL.stream().filter(metric -> metric.dimension() == dimension).collect(Collectors.toList());
    }

    public static KafkaMetricSupport support(KafkaMetricDefinition metric) {
        if (metric == BrokerMetric.EVENT_QUEUE_SIZE) {
            return KafkaMetricSupport.LEGACY_JMX_COMPATIBILITY;
        }
        if (metric == TopicMetric.MIRROR_FETCH_LAG) {
            return KafkaMetricSupport.CUSTOM_KAFKA_COMPATIBILITY;
        }
        if (metric == ClusterMetric.LOAD_REBALANCE_ENABLE || metric == ClusterMetric.LOAD_REBALANCE_NW_IN
            || metric == ClusterMetric.LOAD_REBALANCE_NW_OUT || metric == ClusterMetric.LOAD_REBALANCE_DISK) {
            return KafkaMetricSupport.ENTERPRISE_COMPATIBILITY;
        }
        return KafkaMetricSupport.STANDARD;
    }

    private static List<KafkaMetricDefinition> createAll() {
        List<KafkaMetricDefinition> metrics = new ArrayList<>();
        metrics.addAll(Arrays.asList(ClusterMetric.values()));
        metrics.addAll(Arrays.asList(BrokerMetric.values()));
        metrics.addAll(Arrays.asList(TopicMetric.values()));
        metrics.addAll(Arrays.asList(PartitionMetric.values()));
        metrics.addAll(Arrays.asList(GroupMetric.values()));
        metrics.addAll(Arrays.asList(ReplicaMetric.values()));
        return Collections.unmodifiableList(metrics);
    }
}
