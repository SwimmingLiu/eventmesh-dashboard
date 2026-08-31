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
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
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

        Assert.assertEquals(Set.of(1), snapshot.metadata().brokerIds());
        Assert.assertEquals(Integer.valueOf(1), snapshot.metadata().controllerId());
        Assert.assertTrue(snapshot.metrics().failures().isEmpty());
        Mockito.verify(admin, Mockito.times(1)).describeCluster();
        Mockito.verify(admin, Mockito.times(1)).listTopics(Mockito.any());
        Mockito.verify(admin, Mockito.times(1)).describeTopics(Set.of());
    }
}
