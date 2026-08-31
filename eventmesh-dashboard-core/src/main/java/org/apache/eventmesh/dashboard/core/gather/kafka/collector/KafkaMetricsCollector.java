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

import org.apache.eventmesh.dashboard.core.gather.jmx.JmxConnector;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCatalog;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricFailure;

import org.apache.kafka.clients.admin.Admin;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Orchestrates one Kafka collection cycle; metric semantics live in the six dimension collectors.
 */
public final class KafkaMetricsCollector {

    private final KafkaAdminSource adminSource;

    private final KafkaJmxMetricCollector jmxSource;

    private final KafkaHealthMetricEvaluator healthEvaluator;

    private final List<KafkaDimensionMetricCollector> dimensionCollectors;

    private final Clock clock;

    public KafkaMetricsCollector() {
        this(new KafkaAdminSource(), new KafkaJmxMetricCollector(), new KafkaHealthMetricEvaluator(),
            Arrays.<KafkaDimensionMetricCollector>asList(new KafkaClusterMetricCollector(), new KafkaBrokerMetricCollector(),
                new KafkaTopicMetricCollector(), new KafkaPartitionMetricCollector(),
                new KafkaGroupMetricCollector(), new KafkaReplicaMetricCollector()),
            Clock.systemUTC());
    }

    KafkaMetricsCollector(KafkaAdminSource adminSource, KafkaJmxMetricCollector jmxSource,
        KafkaHealthMetricEvaluator healthEvaluator, List<KafkaDimensionMetricCollector> dimensionCollectors,
        Clock clock) {
        this.adminSource = Objects.requireNonNull(adminSource, "adminSource");
        this.jmxSource = Objects.requireNonNull(jmxSource, "jmxSource");
        this.healthEvaluator = Objects.requireNonNull(healthEvaluator, "healthEvaluator");
        this.dimensionCollectors = Collections.unmodifiableList(new ArrayList<>(dimensionCollectors));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public KafkaMetricCollection collect(Admin admin, Collection<KafkaBrokerJmxEndpoint> endpoints) {
        Objects.requireNonNull(admin, "admin");
        Objects.requireNonNull(endpoints, "endpoints");
        Instant collectedAt = clock.instant();
        KafkaAdminSnapshot adminSnapshot = adminSource.collect(admin, collectedAt);
        KafkaMetricCollection adminMetrics = adminSnapshot.metrics();
        KafkaClusterMetadata metadata = adminSnapshot.metadata();
        KafkaMetricCollection jmxMetrics = collectJmx(endpoints);
        final KafkaMetricCollectionContext context = KafkaMetricCollectionContext.create(adminMetrics, jmxMetrics,
            metadata, endpoints, collectedAt);

        KafkaMetricCollection.Builder result = KafkaMetricCollection.builder();
        adminMetrics.failures().forEach(result::failure);
        jmxMetrics.failures().forEach(result::failure);
        metadata.failures().forEach(result::failure);
        dimensionCollectors.forEach(collector -> collector.collect(context, result));
        return healthEvaluator.evaluate(result.build(), metadata, collectedAt);
    }

    private KafkaMetricCollection collectJmx(Collection<KafkaBrokerJmxEndpoint> endpoints) {
        KafkaMetricCollection.Builder result = KafkaMetricCollection.builder();
        for (KafkaBrokerJmxEndpoint endpoint : endpoints) {
            try (JmxConnector connector = JmxConnector.connect(endpoint.connectionConfig())) {
                result.addAll(jmxSource.collect(connector, endpoint.broker(), KafkaMetricCatalog.jmxMetrics()));
            } catch (IOException ex) {
                result.failure(failure("JmxConnection", endpoint.broker(), ex));
            }
        }
        return result.build();
    }

    private KafkaMetricFailure failure(String metricName, String objectName, Exception exception) {
        String message = exception.getMessage();
        return new KafkaMetricFailure(metricName, objectName,
            message == null || message.trim().isEmpty() ? exception.getClass().getSimpleName() : message);
    }
}
