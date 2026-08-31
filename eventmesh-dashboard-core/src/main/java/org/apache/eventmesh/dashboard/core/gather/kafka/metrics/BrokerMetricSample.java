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

public class BrokerMetricSample {

    private BrokerMetric metric;
    private String broker;
    private BigDecimal value;
    private Instant collectedAt;

    public BrokerMetricSample() {
    }

    public BrokerMetricSample(BrokerMetric metric, String broker, BigDecimal value, Instant collectedAt) {
        this.metric = Objects.requireNonNull(metric, "metric");
        this.broker = Objects.requireNonNull(broker, "broker");
        this.value = Objects.requireNonNull(value, "value");
        this.collectedAt = Objects.requireNonNull(collectedAt, "collectedAt");
    }

    public BrokerMetric metric() {
        return metric;
    }

    public String broker() {
        return broker;
    }

    public BigDecimal value() {
        return value;
    }

    public Instant collectedAt() {
        return collectedAt;
    }

    public BrokerMetric getMetric() {
        return metric;
    }

    public void setMetric(BrokerMetric metric) {
        this.metric = metric;
    }

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
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
