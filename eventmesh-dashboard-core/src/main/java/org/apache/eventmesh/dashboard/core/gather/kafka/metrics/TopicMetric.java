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

public enum TopicMetric implements KafkaMetricDefinition {

    HEALTH_STATE("HealthState", KafkaMetricType.STATE, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_PASSED("HealthCheckPassed", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_TOTAL("HealthCheckTotal", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    TOTAL_PRODUCE_REQUESTS("TotalProduceRequests", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=TotalProduceRequestsPerSec,topic=*", "OneMinuteRate"),
    BYTES_REJECTED("BytesRejected", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec,topic=*", "OneMinuteRate"),
    FAILED_FETCH_REQUESTS("FailedFetchRequests", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec,topic=*", "OneMinuteRate"),
    FAILED_PRODUCE_REQUESTS("FailedProduceRequests", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec,topic=*", "OneMinuteRate"),
    MESSAGES("Messages", KafkaMetricType.COUNT, KafkaMetricSource.ADMIN_CLIENT),
    MESSAGES_IN("MessagesIn", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*", "OneMinuteRate"),
    BYTES_IN("BytesIn", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=*", "OneMinuteRate"),
    BYTES_IN_MIN_5("BytesIn_min_5", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=*", "FiveMinuteRate"),
    BYTES_IN_MIN_15("BytesIn_min_15", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=*", "FifteenMinuteRate"),
    BYTES_OUT("BytesOut", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=*", "OneMinuteRate"),
    BYTES_OUT_MIN_5("BytesOut_min_5", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=*", "FiveMinuteRate"),
    BYTES_OUT_MIN_15("BytesOut_min_15", KafkaMetricType.RATE,
        "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=*", "FifteenMinuteRate"),
    LOG_SIZE("LogSize", KafkaMetricType.GAUGE,
        "kafka.log:type=Log,name=Size,topic=*,partition=*", "Value"),
    PARTITION_URP("PartitionURP", KafkaMetricType.COUNT,
        "kafka.cluster:type=Partition,name=UnderReplicated,topic=*,partition=*", "Value"),
    MIRROR_FETCH_LAG("MirrorFetchLag", KafkaMetricType.COUNT,
        "kafka.server:type=FetcherLagMetrics,name=ConsumerLag,clientId=*,topic=*,partition=*", "Value");

    private final String metricName;

    private final KafkaMetricType type;

    private final KafkaMetricSource source;

    private final KafkaJmxDescriptor jmx;

    TopicMetric(String metricName, KafkaMetricType type, KafkaMetricSource source) {
        this.metricName = metricName;
        this.type = type;
        this.source = source;
        this.jmx = null;
    }

    TopicMetric(String metricName, KafkaMetricType type, String objectNamePattern, String attribute) {
        this.metricName = metricName;
        this.type = type;
        this.source = KafkaMetricSource.JMX;
        this.jmx = new KafkaJmxDescriptor(objectNamePattern, attribute, "topic");
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
        return KafkaMetricDimension.TOPIC;
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
