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

public enum GroupMetric implements KafkaMetricDefinition {

    HEALTH_STATE("HealthState", KafkaMetricType.STATE, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_PASSED("HealthCheckPassed", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    HEALTH_CHECK_TOTAL("HealthCheckTotal", KafkaMetricType.COUNT, KafkaMetricSource.HEALTH_CHECK),
    OFFSET_CONSUMED("OffsetConsumed", KafkaMetricType.OFFSET, KafkaMetricSource.ADMIN_CLIENT),
    LOG_END_OFFSET("LogEndOffset", KafkaMetricType.OFFSET, KafkaMetricSource.ADMIN_CLIENT),
    LAG("Lag", KafkaMetricType.COUNT, KafkaMetricSource.DERIVED),
    STATE("State", KafkaMetricType.STATE, KafkaMetricSource.ADMIN_CLIENT);

    private final String metricName;

    private final KafkaMetricType type;

    private final KafkaMetricSource source;

    GroupMetric(String metricName, KafkaMetricType type, KafkaMetricSource source) {
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
        return KafkaMetricDimension.GROUP;
    }

    @Override
    public KafkaMetricSource source() {
        return source;
    }
}
