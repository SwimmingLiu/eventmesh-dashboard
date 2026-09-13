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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class KafkaAdminSourceTest {

    @Test
    public void testCollectsTopicTopologyOnlyOncePerSnapshot() {
        Admin admin = Mockito.mock(Admin.class);
        DescribeClusterResult cluster = Mockito.mock(DescribeClusterResult.class);
        Mockito.when(admin.describeCluster()).thenReturn(cluster);
        Mockito.when(cluster.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(new Node(1, "broker", 9092))));
        Mockito.when(cluster.controller()).thenReturn(KafkaFuture.completedFuture(new Node(1, "broker", 9092)));

        ListTopicsResult listedTopics = Mockito.mock(ListTopicsResult.class);
        Mockito.when(admin.listTopics(Mockito.any())).thenReturn(listedTopics);
        Mockito.when(listedTopics.names()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        DescribeTopicsResult describedTopics = Mockito.mock(DescribeTopicsResult.class);
        Mockito.when(admin.describeTopics(Set.of())).thenReturn(describedTopics);
        Mockito.when(describedTopics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of()));

        ListOffsetsResult offsets = Mockito.mock(ListOffsetsResult.class);
        Mockito.when(admin.listOffsets(Mockito.anyMap())).thenReturn(offsets);
        Mockito.when(offsets.all()).thenReturn(KafkaFuture.completedFuture(Map.<TopicPartition,
            ListOffsetsResult.ListOffsetsResultInfo>of()));
        ListConsumerGroupsResult groups = Mockito.mock(ListConsumerGroupsResult.class);
        Mockito.when(admin.listConsumerGroups()).thenReturn(groups);
        Mockito.when(groups.all()).thenReturn(KafkaFuture.completedFuture(List.of()));

        KafkaAdminSnapshot snapshot = new KafkaAdminSource().collect(admin, Instant.parse("2026-08-30T00:00:00Z"));

        Assertions.assertEquals(Set.of(1), snapshot.metadata().brokerIds());
        Assertions.assertEquals(Integer.valueOf(1), snapshot.metadata().controllerId());
        Assertions.assertTrue(snapshot.metrics().failures().isEmpty());
        Mockito.verify(admin, Mockito.times(1)).describeCluster();
        Mockito.verify(admin, Mockito.times(1)).listTopics(Mockito.any());
        Mockito.verify(admin, Mockito.times(1)).describeTopics(Set.of());
    }

    @Test
    public void testPreservesTopologyWhenOffsetsCannotBeRead() throws Exception {
        Admin admin = Mockito.mock(Admin.class);
        DescribeClusterResult cluster = Mockito.mock(DescribeClusterResult.class);
        Node broker = new Node(1, "broker", 9092);
        Mockito.when(admin.describeCluster()).thenReturn(cluster);
        Mockito.when(cluster.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(broker)));
        Mockito.when(cluster.controller()).thenReturn(KafkaFuture.completedFuture(broker));

        ListTopicsResult listedTopics = Mockito.mock(ListTopicsResult.class);
        Mockito.when(admin.listTopics(Mockito.any())).thenReturn(listedTopics);
        Mockito.when(listedTopics.names()).thenReturn(KafkaFuture.completedFuture(Set.of("orders")));
        TopicDescription topic = Mockito.mock(TopicDescription.class);
        TopicPartitionInfo partition = Mockito.mock(TopicPartitionInfo.class);
        Mockito.when(topic.name()).thenReturn("orders");
        Mockito.when(topic.partitions()).thenReturn(List.of(partition));
        Mockito.when(partition.partition()).thenReturn(0);
        Mockito.when(partition.leader()).thenReturn(broker);
        Mockito.when(partition.replicas()).thenReturn(List.of(broker));
        Mockito.when(partition.isr()).thenReturn(List.of(broker));
        DescribeTopicsResult describedTopics = Mockito.mock(DescribeTopicsResult.class);
        Mockito.when(admin.describeTopics(Set.of("orders"))).thenReturn(describedTopics);
        Mockito.when(describedTopics.allTopicNames()).thenReturn(
            KafkaFuture.completedFuture(Map.of("orders", topic)));

        ListOffsetsResult offsets = Mockito.mock(ListOffsetsResult.class);
        KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> failed = Mockito.mock(KafkaFuture.class);
        Mockito.when(admin.listOffsets(Mockito.anyMap())).thenReturn(offsets);
        Mockito.when(offsets.all()).thenReturn(failed);
        Mockito.when(failed.get(Mockito.anyLong(), Mockito.any(TimeUnit.class)))
            .thenThrow(new ExecutionException(new IllegalStateException("offsets unavailable")));
        ListConsumerGroupsResult groups = Mockito.mock(ListConsumerGroupsResult.class);
        Mockito.when(admin.listConsumerGroups()).thenReturn(groups);
        Mockito.when(groups.all()).thenReturn(KafkaFuture.completedFuture(List.of()));

        KafkaAdminSnapshot snapshot = new KafkaAdminSource().collect(admin, Instant.parse("2026-08-30T00:00:00Z"));

        TopicPartition topicPartition = new TopicPartition("orders", 0);
        Assertions.assertTrue(snapshot.metadata().partitions().containsKey(topicPartition));
        Assertions.assertEquals(Set.of(1), snapshot.metadata().partitions().get(topicPartition).replicas());
        Assertions.assertTrue(snapshot.metrics().failures().stream()
            .anyMatch(failure -> "PartitionOffsets".equals(failure.metricName())));
    }
}
