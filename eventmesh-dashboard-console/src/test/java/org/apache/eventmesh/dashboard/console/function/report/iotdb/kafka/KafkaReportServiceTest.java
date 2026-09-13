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

package org.apache.eventmesh.dashboard.console.function.report.iotdb.kafka;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.RuntimeEntity;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaCollectConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaMetricCollect;
import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;
import org.apache.eventmesh.dashboard.console.service.cluster.ClusterService;
import org.apache.eventmesh.dashboard.console.service.cluster.RuntimeService;
import org.apache.eventmesh.dashboard.console.spring.support.FunctionConfig;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

public class KafkaReportServiceTest {

    @Test
    public void testDatabaseAddUpdateDeleteAndUnchangedSnapshot() {
        Fixture fixture = new Fixture();
        ClusterEntity cluster = cluster();
        Mockito.when(fixture.clusters.selectAll()).thenReturn(List.of(cluster));
        KafkaMetricCollect first = Mockito.mock(KafkaMetricCollect.class);
        KafkaMetricCollect replacement = Mockito.mock(KafkaMetricCollect.class);
        Mockito.when(fixture.module.createCollect(Mockito.any())).thenReturn(first, replacement);
        try {
            fixture.service.sync();
            Runnable task = fixture.task();
            task.run();
            fixture.service.sync();
            task.run();
            Mockito.verify(fixture.module).createCollect(Mockito.any());
            Mockito.verify(first, Mockito.times(2)).request();
            cluster.setConfig("{\"bootstrapServers\":\"127.0.0.1:9094\"}");
            fixture.service.sync();
            task.run();
            Mockito.verify(first).close();
            ArgumentCaptor<KafkaCollectConfig> configs = ArgumentCaptor.forClass(KafkaCollectConfig.class);
            Mockito.verify(fixture.module, Mockito.times(2)).createCollect(configs.capture());
            Assertions.assertEquals("127.0.0.1:9094", configs.getValue().getBootstrapServers());
            Mockito.when(fixture.clusters.selectAll()).thenReturn(List.of());
            fixture.service.sync();
            task.run();
            Assertions.assertTrue(fixture.tasks().isEmpty());
            Mockito.verify(replacement).close();
        } finally {
            fixture.service.close();
        }
    }

    @Test
    public void testRuntimeChangesReplaceJmxEndpoints() {
        Fixture fixture = new Fixture();
        RuntimeEntity runtime = new RuntimeEntity();
        runtime.setId(2L);
        runtime.setClusterId(1L);
        runtime.setOrganizationId(1L);
        runtime.setClusterType(ClusterType.STORAGE_KAFKA_BROKER);
        runtime.setStatus(1L);
        runtime.setHost("127.0.0.1");
        runtime.setPort(9092);
        runtime.setJmxPort(9999);
        runtime.setRuntimeIndex(1);
        Mockito.when(fixture.clusters.selectAll()).thenReturn(List.of(cluster()));
        Mockito.when(fixture.runtimes.selectAll()).thenReturn(List.of(runtime));
        Mockito.when(fixture.module.createCollect(Mockito.any())).thenAnswer(call -> Mockito.mock(KafkaMetricCollect.class));
        try {
            fixture.service.sync();
            fixture.task().run();
            runtime.setJmxPort(9998);
            fixture.service.sync();
            fixture.task().run();
            ArgumentCaptor<KafkaCollectConfig> configs = ArgumentCaptor.forClass(KafkaCollectConfig.class);
            Mockito.verify(fixture.module, Mockito.times(2)).createCollect(configs.capture());
            Assertions.assertEquals(9999, configs.getAllValues().get(0).getBrokers().get(0).getPort());
            Assertions.assertEquals(9998, configs.getValue().getBrokers().get(0).getPort());
        } finally {
            fixture.service.close();
        }
    }

    @Test
    public void testDatabaseFailureKeepsExistingTasksAndInvalidJsonStopsStaleTask() {
        Fixture fixture = new Fixture();
        ClusterEntity cluster = cluster();
        Mockito.when(fixture.clusters.selectAll()).thenReturn(List.of(cluster));
        Mockito.when(fixture.module.createCollect(Mockito.any())).thenReturn(Mockito.mock(KafkaMetricCollect.class));
        try {
            fixture.service.sync();
            Runnable task = fixture.task();
            task.run();
            Mockito.when(fixture.clusters.selectAll()).thenThrow(new IllegalStateException("database unavailable"));
            fixture.service.sync();
            Assertions.assertSame(task, fixture.task());
            Mockito.doReturn(List.of(cluster)).when(fixture.clusters).selectAll();
            cluster.setConfig("invalid-json");
            fixture.service.sync();
            Assertions.assertTrue(fixture.tasks().isEmpty());
        } finally {
            fixture.service.close();
        }
    }

    @Test
    public void testQueryScopeUsesCurrentDatabaseOrganizationAndDeletionState() {
        Fixture fixture = new Fixture();
        SingleGeneralReportDO query = new SingleGeneralReportDO();
        query.setOrganizationId(1L);
        query.setClustersId(1L);
        ClusterEntity cluster = cluster();
        Mockito.when(fixture.clusters.queryClusterById(Mockito.any())).thenReturn(cluster);
        fixture.service.validate(query);
        cluster.setOrganizationId(2L);
        Assertions.assertThrows(SecurityException.class, () -> fixture.service.validate(query));
        cluster.setOrganizationId(1L);
        cluster.setIsDelete(1);
        Assertions.assertThrows(SecurityException.class, () -> fixture.service.validate(query));
        fixture.service.close();
    }

    @Test
    public void testDisabledReportingDoesNotReadDatabase() {
        Fixture fixture = new Fixture();
        fixture.config.setEnabledReport(false);
        fixture.service.sync();
        Mockito.verifyNoInteractions(fixture.clusters, fixture.runtimes);
        fixture.service.close();
    }

    @Test
    public void testExistingSyncCycleChecksKafkaEvenWithoutIncrementalChanges() {
        org.apache.eventmesh.dashboard.console.spring.support.FunctionManage manage =
            new org.apache.eventmesh.dashboard.console.spring.support.FunctionManage();
        FunctionConfig config = new FunctionConfig();
        KafkaReportService reporting = Mockito.mock(KafkaReportService.class);
        ReflectionTestUtils.setField(manage, "functionConfig", config);
        ReflectionTestUtils.setField(manage, "kafkaReportService", reporting);
        ReflectionTestUtils.setField(manage, "clusterService", Mockito.mock(ClusterService.class));
        ReflectionTestUtils.setField(manage, "runtimeService", Mockito.mock(RuntimeService.class));
        ReflectionTestUtils.setField(manage, "clusterRelationshipService", Mockito.mock(
            org.apache.eventmesh.dashboard.console.service.cluster.ClusterRelationshipService.class));
        manage.sync();
        Mockito.verify(reporting).sync();
    }

    private static ClusterEntity cluster() {
        ClusterEntity cluster = new ClusterEntity();
        cluster.setId(1L);
        cluster.setOrganizationId(1L);
        cluster.setName("eventmesh-local-kafka");
        cluster.setClusterType(ClusterType.STORAGE_KAFKA_CLUSTER);
        cluster.setStatus(1L);
        cluster.setConfig("{\"bootstrapServers\":\"127.0.0.1:9092,127.0.0.1:9094\"}");
        cluster.setJmxProperties("{\"jmxPort\":9999}");
        return cluster;
    }

    private static class Fixture {
        private final FunctionConfig config = new FunctionConfig();
        private final ClusterService clusters = Mockito.mock(ClusterService.class);
        private final RuntimeService runtimes = Mockito.mock(RuntimeService.class);
        private final IotDBKafkaMetricModule module = Mockito.mock(IotDBKafkaMetricModule.class);
        private final KafkaReportService service = new KafkaReportService(config);

        private Fixture() {
            Mockito.when(runtimes.selectAll()).thenReturn(List.of());
            ReflectionTestUtils.setField(service, "clusterService", clusters);
            ReflectionTestUtils.setField(service, "runtimeService", runtimes);
            ReflectionTestUtils.setField(service, "objectMapper", new ObjectMapper());
            ReflectionTestUtils.setField(service, "module", module);
        }

        private Map<?, ?> tasks() {
            return (Map<?, ?>) ReflectionTestUtils.getField(service, "tasks");
        }

        private Runnable task() {
            return (Runnable) tasks().values().iterator().next();
        }
    }
}
