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

import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricFailure;

import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class KafkaClusterMetadata {

    private final Set<Integer> brokerIds;
    private final Integer controllerId;
    private final Map<TopicPartition, PartitionMetadata> partitions;
    private final List<KafkaMetricFailure> failures;
    private final Instant collectedAt;

    KafkaClusterMetadata(Set<Integer> brokerIds, Integer controllerId,
        Map<TopicPartition, PartitionMetadata> partitions, List<KafkaMetricFailure> failures, Instant collectedAt) {
        this.brokerIds = Collections.unmodifiableSet(new HashSet<>(brokerIds));
        this.controllerId = controllerId;
        this.partitions = Collections.unmodifiableMap(new HashMap<>(partitions));
        this.failures = Collections.unmodifiableList(new ArrayList<>(failures));
        this.collectedAt = collectedAt;
    }

    Set<Integer> brokerIds() {
        return brokerIds;
    }

    Integer controllerId() {
        return controllerId;
    }

    Map<TopicPartition, PartitionMetadata> partitions() {
        return partitions;
    }

    List<KafkaMetricFailure> failures() {
        return failures;
    }

    Instant collectedAt() {
        return collectedAt;
    }

    static final class PartitionMetadata {

        private final Integer leader;
        private final Set<Integer> replicas;
        private final Set<Integer> inSyncReplicas;

        PartitionMetadata(Integer leader, Set<Integer> replicas, Set<Integer> inSyncReplicas) {
            this.leader = leader;
            this.replicas = Collections.unmodifiableSet(new HashSet<>(replicas));
            this.inSyncReplicas = Collections.unmodifiableSet(new HashSet<>(inSyncReplicas));
        }

        Integer leader() {
            return leader;
        }

        Set<Integer> replicas() {
            return replicas;
        }

        Set<Integer> inSyncReplicas() {
            return inSyncReplicas;
        }
    }
}
