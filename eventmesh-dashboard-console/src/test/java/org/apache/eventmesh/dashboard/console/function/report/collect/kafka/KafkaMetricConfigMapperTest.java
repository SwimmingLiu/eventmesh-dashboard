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

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.RuntimeEntity;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaMetricConfigMapperTest {

    private final KafkaMetricConfigMapper mapper = new KafkaMetricConfigMapper(new ObjectMapper());

    @Test
    public void testReadsExistingClusterJsonAndRuntimeJmxOverride() {
        ClusterEntity cluster = cluster();
        RuntimeEntity broker = broker();
        KafkaCollectConfig config = mapper.map(cluster, List.of(broker));
        Assertions.assertEquals("127.0.0.1:9092,127.0.0.1:9094", config.getBootstrapServers());
        Assertions.assertEquals(1L, config.getOrganizationId());
        Assertions.assertEquals(1L, config.getClusterId());
        Assertions.assertEquals(9999, config.getBrokers().get(0).getPort());
        Assertions.assertFalse(config.getAdminProperties().containsKey("bootstrapServers"));
        broker.setJmxPort(9998);
        Assertions.assertEquals(9998, mapper.map(cluster, List.of(broker)).getBrokers().get(0).getPort());
    }

    @Test
    public void testFiltersDeletedAndOtherOrganizationRuntimesAndDerivesBootstrap() {
        ClusterEntity cluster = cluster();
        cluster.setConfig("{}");
        RuntimeEntity active = broker();
        RuntimeEntity deleted = broker();
        deleted.setId(2L);
        deleted.setIsDelete(1);
        RuntimeEntity other = broker();
        other.setId(3L);
        other.setOrganizationId(2L);
        KafkaCollectConfig config = mapper.map(cluster, List.of(active, deleted, other));
        Assertions.assertEquals("127.0.0.1:9092", config.getBootstrapServers());
        Assertions.assertEquals(1, config.getBrokers().size());
    }

    @Test
    public void testInvalidJsonIsRejected() {
        ClusterEntity cluster = cluster();
        cluster.setConfig("bootstrapServers=localhost:9092");
        Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.map(cluster, List.of()));
    }

    private ClusterEntity cluster() {
        ClusterEntity cluster = new ClusterEntity();
        cluster.setId(1L);
        cluster.setOrganizationId(1L);
        cluster.setName("eventmesh-local-kafka");
        cluster.setClusterType(ClusterType.STORAGE_KAFKA_CLUSTER);
        cluster.setStatus(1L);
        cluster.setIsDelete(0);
        cluster.setConfig("{\"bootstrapServers\":\"127.0.0.1:9092,127.0.0.1:9094\"}");
        cluster.setJmxProperties("{\"jmxPort\":9999}");
        return cluster;
    }

    private RuntimeEntity broker() {
        RuntimeEntity broker = new RuntimeEntity();
        broker.setId(1L);
        broker.setOrganizationId(1L);
        broker.setClusterId(1L);
        broker.setClusterType(ClusterType.STORAGE_KAFKA_BROKER);
        broker.setHost("127.0.0.1");
        broker.setPort(9092);
        broker.setRuntimeIndex(0);
        broker.setStatus(1L);
        return broker;
    }
}
