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

import java.util.Optional;



public enum BrokerMetric implements KafkaMetricDefinition {

    HEALTH_STATE("HealthState", KafkaMetricType.STATE, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_PASSED("HealthCheckPassed", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_TOTAL("HealthCheckTotal", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    TOTAL_REQUEST_QUEUE_SIZE("TotalRequestQueueSize", KafkaMetricType.GAUGE,
        "kafka.network:type=RequestChannel,name=RequestQueueSize", "Value"),
    TOTAL_RESPONSE_QUEUE_SIZE("TotalResponseQueueSize", KafkaMetricType.GAUGE,
        "kafka.network:type=RequestChannel,name=ResponseQueueSize", "Value"),
    MESSAGES_IN("MessagesIn", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec", "OneMinuteRate"),
    TOTAL_PRODUCE_REQUESTS("TotalProduceRequests", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=TotalProduceRequestsPerSec", "OneMinuteRate"),
    NETWORK_PROCESSOR_AVG_IDLE("NetworkProcessorAvgIdle", KafkaMetricType.RATIO,
        "kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent", "Value"),
    REQUEST_HANDLER_AVG_IDLE("RequestHandlerAvgIdle", KafkaMetricType.RATIO,
        "kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent", "OneMinuteRate"),
    CONNECTIONS_COUNT("ConnectionsCount", KafkaMetricType.GAUGE,
        "kafka.server:type=socket-server-metrics,listener=*,networkProcessor=*", "connection-count"),
    PARTITION_URP("PartitionURP", KafkaMetricType.COUNT,
        "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions", "Value"),
    PARTITION_MIN_ISR_S("PartitionMinISR_S", KafkaMetricType.COUNT,
        "kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount", "Value"),
    PARTITION_MIN_ISR_E("PartitionMinISR_E", KafkaMetricType.COUNT,
        "kafka.server:type=ReplicaManager,name=AtMinIsrPartitionCount", "Value"),
    PARTITIONS("Partitions", KafkaMetricType.COUNT,
        "kafka.server:type=ReplicaManager,name=PartitionCount", "Value"),
    PARTITIONS_SKEW("PartitionsSkew", KafkaMetricType.RATIO, KafkaMetricSource.DERIVED),
    LEADERS("Leaders", KafkaMetricType.COUNT,
        "kafka.server:type=ReplicaManager,name=LeaderCount", "Value"),
    LEADERS_SKEW("LeadersSkew", KafkaMetricType.RATIO, KafkaMetricSource.DERIVED),
    ACTIVE_CONTROLLER_COUNT("ActiveControllerCount", KafkaMetricType.GAUGE,
        "kafka.controller:type=KafkaController,name=ActiveControllerCount", "Value"),
    EVENT_QUEUE_SIZE("EventQueueSize", KafkaMetricType.GAUGE,
        "kafka.controller:type=ControllerEventManager,name=EventQueueSize", "Value"),
    BYTES_IN("BytesIn", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", "OneMinuteRate"),
    BYTES_IN_MIN_5("BytesIn_min_5", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", "FiveMinuteRate"),
    BYTES_IN_MIN_15("BytesIn_min_15", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", "FifteenMinuteRate"),
    BYTES_OUT("BytesOut", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec", "OneMinuteRate"),
    BYTES_OUT_MIN_5("BytesOut_min_5", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec", "FiveMinuteRate"),
    BYTES_OUT_MIN_15("BytesOut_min_15", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec", "FifteenMinuteRate"),
    REPLICATION_BYTES_IN("ReplicationBytesIn", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec", "OneMinuteRate"),
    REPLICATION_BYTES_OUT("ReplicationBytesOut", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesOutPerSec", "OneMinuteRate"),
    REASSIGNMENT_BYTES_IN("ReassignmentBytesIn", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=ReassignmentBytesInPerSec", "OneMinuteRate"),
    REASSIGNMENT_BYTES_OUT("ReassignmentBytesOut", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=ReassignmentBytesOutPerSec", "OneMinuteRate"),
    LOG_SIZE("LogSize", KafkaMetricType.GAUGE,
        "kafka.log:type=Log,name=Size,topic=*,partition=*", "Value"),
    ALIVE("Alive", KafkaMetricType.STATE, KafkaMetricSource.ADMIN_CLIENT);

    private final String metricName;

    private final KafkaMetricType type;

    private final KafkaMetricSource source;

    private final KafkaJmxDescriptor jmx;

    BrokerMetric(String metricName, KafkaMetricType type, KafkaMetricSource source) {
        this.metricName = metricName;
        this.type = type;
        this.source = source;
        this.jmx = null;
    }

    BrokerMetric(String metricName, KafkaMetricType type, String objectNamePattern, String attribute) {
        this.metricName = metricName;
        this.type = type;
        this.source = KafkaMetricSource.JMX;
        this.jmx = new KafkaJmxDescriptor(objectNamePattern, attribute);
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
        return KafkaMetricDimension.BROKER;
    }

    @Override
    public KafkaMetricSource source() {
        return source;
    }

    @Override
    public Optional<KafkaJmxDescriptor> jmx() {
        return Optional.ofNullable(jmx);
    }
}
