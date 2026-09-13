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

package org.apache.eventmesh.dashboard.console.function.report.collect.kafka;

import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;

import java.time.Instant;
import java.util.Objects;

import lombok.Value;
import lombok.experimental.Accessors;

/**
 * Immutable Kafka metric data collected for one cluster and one cycle.
 */
@Value
@Accessors(fluent = true)
public class KafkaMetricWriteBatch {

    String organizationId;

    String clusterId;

    String clusterName;

    Instant collectedAt;

    long durationMillis;

    KafkaMetricCollection metrics;

    public KafkaMetricWriteBatch(String organizationId, String clusterId, String clusterName,
        Instant collectedAt, long durationMillis, KafkaMetricCollection metrics) {
        this.organizationId = requireText(organizationId, "organizationId");
        this.clusterId = requireText(clusterId, "clusterId");
        this.clusterName = requireText(clusterName, "clusterName");
        this.collectedAt = Objects.requireNonNull(collectedAt, "collectedAt");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        if (durationMillis < 0) {
            throw new IllegalArgumentException("durationMillis must not be negative");
        }
        this.durationMillis = durationMillis;
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
