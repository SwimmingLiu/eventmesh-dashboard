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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * A Topic metric contribution read from one Broker. Contributions are summed by Topic by the cluster collector.
 */
public class TopicMetricSample {

    private TopicMetric metric;
    private String topic;
    private Optional<String> sourceBroker;
    private BigDecimal value;
    private Instant collectedAt;

    public TopicMetricSample() {
        this.sourceBroker = Optional.empty();
    }

    public TopicMetricSample(TopicMetric metric, String topic, Optional<String> sourceBroker, BigDecimal value,
        Instant collectedAt) {
        this.metric = Objects.requireNonNull(metric, "metric");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.sourceBroker = Objects.requireNonNull(sourceBroker, "sourceBroker");
        this.value = Objects.requireNonNull(value, "value");
        this.collectedAt = Objects.requireNonNull(collectedAt, "collectedAt");
    }

    public TopicMetric metric() {
        return metric;
    }

    public String topic() {
        return topic;
    }

    public Optional<String> sourceBroker() {
        return sourceBroker;
    }

    public BigDecimal value() {
        return value;
    }

    public Instant collectedAt() {
        return collectedAt;
    }

    public TopicMetric getMetric() {
        return metric;
    }

    public void setMetric(TopicMetric metric) {
        this.metric = metric;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Optional<String> getSourceBroker() {
        return sourceBroker;
    }

    public void setSourceBroker(Optional<String> sourceBroker) {
        this.sourceBroker = sourceBroker;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    public Instant getCollectedAt() {
        return collectedAt;
    }

    public void setCollectedAt(Instant collectedAt) {
        this.collectedAt = collectedAt;
    }
}
