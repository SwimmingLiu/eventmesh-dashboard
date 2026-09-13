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

import org.apache.eventmesh.dashboard.core.gather.kafka.collector.KafkaBrokerJmxEndpoint;
import org.apache.eventmesh.dashboard.core.gather.kafka.collector.KafkaMetricsCollector;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;

import org.apache.kafka.clients.admin.Admin;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

final class KafkaMetricPersistenceCoordinator {

    private final KafkaMetricsCollector collector;
    private final KafkaMetricStore store;
    private final Clock clock;

    KafkaMetricPersistenceCoordinator(KafkaMetricStore store) {
        this(new KafkaMetricsCollector(), store, Clock.systemUTC());
    }

    KafkaMetricPersistenceCoordinator(KafkaMetricsCollector collector, KafkaMetricStore store, Clock clock) {
        this.collector = Objects.requireNonNull(collector, "collector");
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public KafkaMetricCollection collectAndStore(String organizationId, String clusterId, String clusterName,
        Admin admin, Collection<KafkaBrokerJmxEndpoint> endpoints) {
        KafkaMetricWriteBatch batch = collect(organizationId, clusterId, clusterName, admin, endpoints);
        store.write(batch);
        return batch.metrics();
    }

    KafkaMetricWriteBatch collect(String organizationId, String clusterId, String clusterName,
        Admin admin, Collection<KafkaBrokerJmxEndpoint> endpoints) {
        Instant startedAt = clock.instant();
        long startedNanos = System.nanoTime();
        KafkaMetricCollection metrics = collector.collect(admin, endpoints);
        long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
        return new KafkaMetricWriteBatch(organizationId, clusterId, clusterName, startedAt, durationMillis, metrics);
    }
}
