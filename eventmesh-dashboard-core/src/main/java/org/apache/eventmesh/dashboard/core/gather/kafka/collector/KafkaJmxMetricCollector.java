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

import org.apache.eventmesh.dashboard.core.gather.jmx.JmxConnectionConfig;
import org.apache.eventmesh.dashboard.core.gather.jmx.JmxConnector;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.BrokerMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaJmxDescriptor;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCatalog;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricDefinition;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricFailure;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.ReplicaMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.management.ObjectName;

final class KafkaJmxMetricCollector {

    private final Clock clock;

    KafkaJmxMetricCollector() {
        this(Clock.systemUTC());
    }

    KafkaJmxMetricCollector(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    KafkaMetricCollection collect(JmxConnectionConfig config) throws IOException {
        return collect(config, KafkaMetricCatalog.jmxMetrics());
    }

    KafkaMetricCollection collect(JmxConnectionConfig config,
        Collection<? extends KafkaMetricDefinition> metrics) throws IOException {
        try (JmxConnector connector = JmxConnector.connect(config)) {
            return collect(connector, config.instance(), metrics);
        }
    }

    KafkaMetricCollection collect(JmxConnector connector, String brokerInstance,
        Collection<? extends KafkaMetricDefinition> metrics) {
        Objects.requireNonNull(connector, "connector");
        Objects.requireNonNull(brokerInstance, "brokerInstance");
        Objects.requireNonNull(metrics, "metrics");
        Instant collectedAt = clock.instant();
        KafkaMetricCollection.Builder result = KafkaMetricCollection.builder();
        for (KafkaMetricDefinition metric : metrics) {
            collectMetric(connector, brokerInstance, metric, collectedAt, result);
        }
        return result.build();
    }

    private void collectMetric(JmxConnector connector, String brokerInstance, KafkaMetricDefinition metric, Instant collectedAt,
        KafkaMetricCollection.Builder result) {
        KafkaJmxDescriptor descriptor = metric.jmx().orElse(null);
        if (descriptor == null) {
            return;
        }
        Set<ObjectName> names;
        try {
            names = connector.queryNames(new ObjectName(descriptor.objectNamePattern()));
        } catch (Exception ex) {
            result.failure(failure(metric, descriptor.objectNamePattern(), ex));
            return;
        }
        Map<Map<String, String>, BigDecimal> values = new LinkedHashMap<>();
        for (ObjectName name : names) {
            try {
                Object rawValue = connector.getAttribute(name, descriptor.attribute());
                BigDecimal value = toBigDecimal(rawValue);
                Map<String, String> dimensions = dimensions(brokerInstance, descriptor, name);
                values.merge(dimensions, value, BigDecimal::add);
            } catch (Exception ex) {
                result.failure(failure(metric, name.toString(), ex));
            }
        }
        values.forEach((dimensions, value) -> addSample(result, metric, dimensions, value, collectedAt));
    }

    private void addSample(KafkaMetricCollection.Builder result, KafkaMetricDefinition metric,
        Map<String, String> dimensions, BigDecimal value, Instant collectedAt) {
        String broker = dimensions.get("broker");
        if (metric instanceof BrokerMetric) {
            result.add((BrokerMetric) metric, broker, value, collectedAt);
        } else if (metric instanceof TopicMetric) {
            result.add((TopicMetric) metric, dimensions.get("topic"), broker, value, collectedAt);
        } else if (metric instanceof ReplicaMetric) {
            TopicPartition topicPartition = new TopicPartition(dimensions.get("topic"),
                Integer.parseInt(dimensions.get("partition")));
            result.add((ReplicaMetric) metric, broker, topicPartition, value, collectedAt);
        } else {
            throw new IllegalArgumentException("JMX metric has unsupported dimension: " + metric.dimension());
        }
    }

    private Map<String, String> dimensions(String brokerInstance, KafkaJmxDescriptor descriptor, ObjectName name) {
        Map<String, String> dimensions = new LinkedHashMap<>();
        dimensions.put("broker", brokerInstance);
        for (String key : descriptor.dimensionKeys()) {
            String value = name.getKeyProperty(key);
            if (value != null) {
                dimensions.put(key, unquote(value));
            }
        }
        return Collections.unmodifiableMap(dimensions);
    }

    private String unquote(String value) {
        if (value.length() >= 2 && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
            return ObjectName.unquote(value);
        }
        return value;
    }

    private BigDecimal toBigDecimal(Object rawValue) {
        if (!(rawValue instanceof Number)) {
            throw new IllegalArgumentException("JMX attribute is not numeric: " + rawValue);
        }
        return new BigDecimal(rawValue.toString());
    }

    private KafkaMetricFailure failure(KafkaMetricDefinition metric, String objectName, Exception exception) {
        String message = exception.getMessage();
        if (message == null || message.trim().isEmpty()) {
            message = exception.getClass().getSimpleName();
        }
        return new KafkaMetricFailure(metric.metricName(), objectName, message);
    }
}
