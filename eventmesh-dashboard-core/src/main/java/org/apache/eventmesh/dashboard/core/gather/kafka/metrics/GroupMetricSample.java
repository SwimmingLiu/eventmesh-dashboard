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

import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public class GroupMetricSample {

    private GroupMetric metric;
    private String group;
    private Optional<TopicPartition> topicPartition;
    private BigDecimal value;
    private Instant collectedAt;

    public GroupMetricSample() {
        this.topicPartition = Optional.empty();
    }

    public GroupMetricSample(GroupMetric metric, String group, Optional<TopicPartition> topicPartition,
        BigDecimal value, Instant collectedAt) {
        this.metric = Objects.requireNonNull(metric, "metric");
        this.group = Objects.requireNonNull(group, "group");
        this.topicPartition = Objects.requireNonNull(topicPartition, "topicPartition");
        this.value = Objects.requireNonNull(value, "value");
        this.collectedAt = Objects.requireNonNull(collectedAt, "collectedAt");
        if (isPartitionMetric(metric) != topicPartition.isPresent()) {
            throw new IllegalArgumentException("Group offset and lag metrics require a TopicPartition");
        }
    }

    public GroupMetric metric() {
        return metric;
    }

    public String group() {
        return group;
    }

    public Optional<TopicPartition> topicPartition() {
        return topicPartition;
    }

    public BigDecimal value() {
        return value;
    }

    public Instant collectedAt() {
        return collectedAt;
    }

    public GroupMetric getMetric() {
        return metric;
    }

    public void setMetric(GroupMetric metric) {
        this.metric = metric;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Optional<TopicPartition> getTopicPartition() {
        return topicPartition;
    }

    public void setTopicPartition(Optional<TopicPartition> topicPartition) {
        this.topicPartition = topicPartition;
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

    private static boolean isPartitionMetric(GroupMetric metric) {
        return metric == GroupMetric.OFFSET_CONSUMED || metric == GroupMetric.LOG_END_OFFSET || metric == GroupMetric.LAG;
    }
}
