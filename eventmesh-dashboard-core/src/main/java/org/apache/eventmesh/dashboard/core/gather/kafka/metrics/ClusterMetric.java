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

public enum ClusterMetric implements KafkaMetricDefinition {

    HEALTH_STATE("HealthState", KafkaMetricType.STATE, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_PASSED("HealthCheckPassed", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_TOTAL("HealthCheckTotal", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_STATE_TOPICS("HealthState_Topics", KafkaMetricType.STATE, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_PASSED_TOPICS("HealthCheckPassed_Topics", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_TOTAL_TOPICS("HealthCheckTotal_Topics", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_STATE_BROKERS("HealthState_Brokers", KafkaMetricType.STATE, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_PASSED_BROKERS("HealthCheckPassed_Brokers", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_TOTAL_BROKERS("HealthCheckTotal_Brokers", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_STATE_GROUPS("HealthState_Groups", KafkaMetricType.STATE, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_PASSED_GROUPS("HealthCheckPassed_Groups", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_TOTAL_GROUPS("HealthCheckTotal_Groups", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_STATE_CLUSTER("HealthState_Cluster", KafkaMetricType.STATE, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_PASSED_CLUSTER("HealthCheckPassed_Cluster", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_TOTAL_CLUSTER("HealthCheckTotal_Cluster", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    TOTAL_REQUEST_QUEUE_SIZE("TotalRequestQueueSize", KafkaMetricType.GAUGE, KafkaMetricSource.DERIVED),
    TOTAL_RESPONSE_QUEUE_SIZE("TotalResponseQueueSize", KafkaMetricType.GAUGE, KafkaMetricSource.DERIVED),
    EVENT_QUEUE_SIZE("EventQueueSize", KafkaMetricType.GAUGE, KafkaMetricSource.DERIVED),
    ACTIVE_CONTROLLER_COUNT("ActiveControllerCount", KafkaMetricType.GAUGE, KafkaMetricSource.DERIVED),
    TOTAL_PRODUCE_REQUESTS("TotalProduceRequests", KafkaMetricType.RATE, KafkaMetricSource.DERIVED),
    CONNECTIONS_COUNT("ConnectionsCount", KafkaMetricType.GAUGE, KafkaMetricSource.DERIVED),
    PARTITION_NO_LEADER("PartitionNoLeader", KafkaMetricType.COUNT, KafkaMetricSource.ADMIN_CLIENT),
    PARTITION_MIN_ISR_S("PartitionMinISR_S", KafkaMetricType.COUNT, KafkaMetricSource.DERIVED),
    PARTITION_MIN_ISR_E("PartitionMinISR_E", KafkaMetricType.COUNT, KafkaMetricSource.DERIVED),
    PARTITION_URP("PartitionURP", KafkaMetricType.COUNT, KafkaMetricSource.DERIVED),
    MESSAGES_IN("MessagesIn", KafkaMetricType.RATE, KafkaMetricSource.DERIVED),
    LEADER_MESSAGES("LeaderMessages", KafkaMetricType.COUNT, KafkaMetricSource.ADMIN_CLIENT),
    TOTAL_LOG_SIZE("TotalLogSize", KafkaMetricType.GAUGE, KafkaMetricSource.DERIVED),
    BYTES_IN("BytesIn", KafkaMetricType.RATE, KafkaMetricSource.DERIVED),
    BYTES_IN_MIN_5("BytesIn_min_5", KafkaMetricType.RATE, KafkaMetricSource.DERIVED),
    BYTES_IN_MIN_15("BytesIn_min_15", KafkaMetricType.RATE, KafkaMetricSource.DERIVED),
    BYTES_OUT("BytesOut", KafkaMetricType.RATE, KafkaMetricSource.DERIVED),
    BYTES_OUT_MIN_5("BytesOut_min_5", KafkaMetricType.RATE, KafkaMetricSource.DERIVED),
    BYTES_OUT_MIN_15("BytesOut_min_15", KafkaMetricType.RATE, KafkaMetricSource.DERIVED),
    GROUP_ACTIVES("GroupActives", KafkaMetricType.COUNT, KafkaMetricSource.ADMIN_CLIENT),
    GROUP_EMPTYS("GroupEmptys", KafkaMetricType.COUNT, KafkaMetricSource.ADMIN_CLIENT),
    GROUP_REBALANCES("GroupRebalances", KafkaMetricType.COUNT, KafkaMetricSource.ADMIN_CLIENT),
    GROUP_DEADS("GroupDeads", KafkaMetricType.COUNT, KafkaMetricSource.ADMIN_CLIENT),
    ALIVE("Alive", KafkaMetricType.STATE, KafkaMetricSource.ADMIN_CLIENT),
    LOAD_REBALANCE_ENABLE("LoadReBalanceEnable", KafkaMetricType.STATE, KafkaMetricSource.DERIVED),
    LOAD_REBALANCE_NW_IN("LoadReBalanceNwIn", KafkaMetricType.STATE, KafkaMetricSource.DERIVED),
    LOAD_REBALANCE_NW_OUT("LoadReBalanceNwOut", KafkaMetricType.STATE, KafkaMetricSource.DERIVED),
    LOAD_REBALANCE_DISK("LoadReBalanceDisk", KafkaMetricType.STATE, KafkaMetricSource.DERIVED);

    private final String metricName;

    private final KafkaMetricType type;

    private final KafkaMetricSource source;

    ClusterMetric(String metricName, KafkaMetricType type, KafkaMetricSource source) {
        this.metricName = metricName;
        this.type = type;
        this.source = source;
    }

    @Override
    public String metricName() {
        return metricName;
    }

    @Override
    public KafkaMetricType type() {
        return type;
    }

    @Override
    public KafkaMetricDimension dimension() {
        return KafkaMetricDimension.CLUSTER;
    }

    @Override
    public KafkaMetricSource source() {
        return source;
    }
}
