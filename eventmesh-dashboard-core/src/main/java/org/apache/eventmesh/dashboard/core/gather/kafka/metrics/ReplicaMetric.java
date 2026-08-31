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

public enum ReplicaMetric implements KafkaMetricDefinition {

    LOG_START_OFFSET("LogStartOffset", KafkaMetricType.OFFSET,
        "kafka.log:type=Log,name=LogStartOffset,topic=*,partition=*", "Value"),
    LOG_END_OFFSET("LogEndOffset", KafkaMetricType.OFFSET,
        "kafka.log:type=Log,name=LogEndOffset,topic=*,partition=*", "Value"),
    MESSAGES("Messages", KafkaMetricType.COUNT, KafkaMetricSource.DERIVED),
    LOG_SIZE("LogSize", KafkaMetricType.GAUGE,
        "kafka.log:type=Log,name=Size,topic=*,partition=*", "Value"),
    IN_SYNC("InSync", KafkaMetricType.STATE, KafkaMetricSource.ADMIN_CLIENT);

    private final String metricName;

    private final KafkaMetricType type;

    private final KafkaMetricSource source;

    private final KafkaJmxDescriptor jmx;

    ReplicaMetric(String metricName, KafkaMetricType type, KafkaMetricSource source) {
        this.metricName = metricName;
        this.type = type;
        this.source = source;
        this.jmx = null;
    }

    ReplicaMetric(String metricName, KafkaMetricType type, String objectNamePattern, String attribute) {
        this.metricName = metricName;
        this.type = type;
        this.source = KafkaMetricSource.JMX;
        this.jmx = new KafkaJmxDescriptor(objectNamePattern, attribute, "topic", "partition");
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
        return KafkaMetricDimension.REPLICA;
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
