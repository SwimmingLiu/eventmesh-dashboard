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

import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricDimension;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Produces all Topic-dimension samples, including cross-Broker JMX aggregation. */
final class KafkaTopicMetricCollector implements KafkaDimensionMetricCollector {

    private static final Set<TopicMetric> ZERO_WHEN_ABSENT = Collections.unmodifiableSet(EnumSet.of(
        TopicMetric.TOTAL_PRODUCE_REQUESTS,
        TopicMetric.BYTES_REJECTED, TopicMetric.FAILED_FETCH_REQUESTS, TopicMetric.FAILED_PRODUCE_REQUESTS,
        TopicMetric.MESSAGES_IN, TopicMetric.BYTES_IN, TopicMetric.BYTES_IN_MIN_5, TopicMetric.BYTES_IN_MIN_15,
        TopicMetric.BYTES_OUT, TopicMetric.BYTES_OUT_MIN_5, TopicMetric.BYTES_OUT_MIN_15,
        TopicMetric.MIRROR_FETCH_LAG));

    @Override
    public KafkaMetricDimension dimension() {
        return KafkaMetricDimension.TOPIC;
    }

    @Override
    public void collect(KafkaMetricCollectionContext context, KafkaMetricCollection.Builder result) {
        context.adminMetrics().topics().forEach(sample -> result.add(sample.metric(), sample.topic(), null,
            sample.value(), sample.collectedAt()));
        Map<TopicTotalKey, BigDecimal> totals = new LinkedHashMap<>();
        context.topicContributions().forEach((key, value) -> {
            if (context.topics().contains(key.topic())) {
                totals.merge(new TopicTotalKey(key.topic(), key.metric()), value, BigDecimal::add);
            }
        });
        context.topics().forEach(topic -> ZERO_WHEN_ABSENT.forEach(metric ->
            totals.putIfAbsent(new TopicTotalKey(topic, metric), BigDecimal.ZERO)));
        totals.forEach((key, value) -> result.add(key.metric(), key.topic(), null, value, context.collectedAt()));
    }

    private static final class TopicTotalKey {
        private final String topic;
        private final TopicMetric metric;

        private TopicTotalKey(String topic, TopicMetric metric) {
            this.topic = topic;
            this.metric = metric;
        }

        private String topic() {
            return topic;
        }

        private TopicMetric metric() {
            return metric;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof TopicTotalKey)) {
                return false;
            }
            TopicTotalKey that = (TopicTotalKey) object;
            return Objects.equals(topic, that.topic) && metric == that.metric;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, metric);
        }
    }
}
