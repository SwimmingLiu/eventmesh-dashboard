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
import org.apache.eventmesh.dashboard.common.model.base.BaseSyncBase;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaCollectConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.AbstractCollect;
import org.apache.eventmesh.dashboard.console.function.report.collect.DataSyncHandler;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKManage;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKTypeEnum;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateKakfaConfig;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateSDKConfig;
import org.apache.eventmesh.dashboard.core.gather.kafka.collector.KafkaMetricsCollector;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.time.Clock;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class KafkaMetricCollectTest {

    @Test
    public void testUsesSdkManageForKafkaAdminLifecycle() {
        KafkaCollectConfig config = new KafkaCollectConfig();
        config.setOrganizationId(7L);
        config.setClusterId(9L);
        config.setClusterName("orders-kafka");
        config.setBootstrapServers("kafka-1:9092,kafka-2:9092");
        config.setAdminProperties(Map.of("security.protocol", "PLAINTEXT"));
        config.setIntervalMillis(5_000L);

        KafkaMetricStore store = Mockito.mock(KafkaMetricStore.class);
        SDKManage sdkManage = Mockito.mock(SDKManage.class);
        Admin admin = Mockito.mock(Admin.class);
        Mockito.when(sdkManage.createClient(Mockito.eq(SDKTypeEnum.ADMIN), Mockito.any(), Mockito.any(),
            Mockito.eq(ClusterType.STORAGE_KAFKA_BROKER))).thenReturn(admin);

        KafkaMetricCollect collector = new KafkaMetricCollect(config, store, Clock.systemUTC(), sdkManage);

        ArgumentCaptor<BaseSyncBase> metadataCaptor = ArgumentCaptor.forClass(BaseSyncBase.class);
        ArgumentCaptor<CreateSDKConfig> configCaptor = ArgumentCaptor.forClass(CreateSDKConfig.class);
        Mockito.verify(sdkManage).createClient(Mockito.eq(SDKTypeEnum.ADMIN), metadataCaptor.capture(),
            configCaptor.capture(), Mockito.eq(ClusterType.STORAGE_KAFKA_BROKER));

        BaseSyncBase metadata = metadataCaptor.getValue();
        Assertions.assertEquals(Long.valueOf(7L), metadata.getOrganizationId());
        Assertions.assertEquals(Long.valueOf(9L), metadata.getClusterId());

        CreateKakfaConfig sdkConfig = (CreateKakfaConfig) configCaptor.getValue();
        Assertions.assertEquals("kafka-1:9092,kafka-2:9092",
            sdkConfig.getAdminProperties().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        Assertions.assertEquals("PLAINTEXT", sdkConfig.getAdminProperties().get("security.protocol"));

        collector.close();

        Mockito.verify(sdkManage).deleteClient(null, metadata.getUnique(), admin);
        Mockito.verify(admin, Mockito.never()).close();
    }

    @Test
    public void testSameClusterIdInDifferentOrganizationsHasDistinctIdentity() {
        KafkaMetricStore store = Mockito.mock(KafkaMetricStore.class);
        SDKManage sdkManage = Mockito.mock(SDKManage.class);
        Admin firstAdmin = Mockito.mock(Admin.class);
        Admin secondAdmin = Mockito.mock(Admin.class);
        Mockito.when(sdkManage.createClient(Mockito.eq(SDKTypeEnum.ADMIN), Mockito.any(), Mockito.any(),
            Mockito.eq(ClusterType.STORAGE_KAFKA_BROKER))).thenReturn(firstAdmin, secondAdmin);

        KafkaMetricCollect first = new KafkaMetricCollect(config(7L, 9L), store, Clock.systemUTC(), sdkManage);
        KafkaMetricCollect second = new KafkaMetricCollect(config(8L, 9L), store, Clock.systemUTC(), sdkManage);

        Assertions.assertEquals("kafka:7:9", first.key());
        Assertions.assertEquals("kafka:8:9", second.key());
        first.close();
        second.close();
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(sdkManage).deleteClient(Mockito.isNull(), keyCaptor.capture(), Mockito.eq(firstAdmin));
        Mockito.verify(sdkManage).deleteClient(Mockito.isNull(), keyCaptor.capture(), Mockito.eq(secondAdmin));
        Assertions.assertNotEquals(keyCaptor.getAllValues().get(0), keyCaptor.getAllValues().get(1));
        Assertions.assertTrue(keyCaptor.getAllValues().stream().anyMatch(key -> key.startsWith("KafkaMetricCollect-7-9-")));
        Assertions.assertTrue(keyCaptor.getAllValues().stream().anyMatch(key -> key.startsWith("KafkaMetricCollect-8-9-")));
    }

    @Test
    public void testTemplateRetainsFailedKafkaBatchForRetry() {
        KafkaMetricStore store = Mockito.mock(KafkaMetricStore.class);
        SDKManage sdkManage = Mockito.mock(SDKManage.class);
        Admin admin = Mockito.mock(Admin.class);
        Mockito.when(sdkManage.createClient(Mockito.eq(SDKTypeEnum.ADMIN), Mockito.any(), Mockito.any(),
            Mockito.eq(ClusterType.STORAGE_KAFKA_BROKER))).thenReturn(admin);
        KafkaMetricCollection metrics = KafkaMetricCollection.builder().build();
        try (MockedConstruction<KafkaMetricsCollector> collectors = Mockito.mockConstruction(KafkaMetricsCollector.class,
            (mock, context) -> Mockito.when(mock.collect(Mockito.eq(admin), Mockito.any())).thenReturn(metrics))) {
            KafkaMetricCollect collector = new KafkaMetricCollect(config(7L, 9L), store, Clock.systemUTC(), sdkManage);
            Assertions.assertInstanceOf(AbstractCollect.class, collector);
            Mockito.doThrow(new IllegalStateException("storage unavailable")).doNothing().when(store).write(Mockito.any());
            Assertions.assertThrows(IllegalStateException.class, collector::request);
            // Explicit template invocation avoids depending on wall-clock scheduling in this retry test.
            DataSyncHandler handler = new DataSyncHandler();
            handler.setBatchWriter(batches -> batches.values().forEach(samples -> samples.forEach(sample ->
                store.write(((KafkaMetricSample) sample).getBatch()))));
            collector.collect(0, handler.getDataSyncHandlerWrapper(1));
            ArgumentCaptor<KafkaMetricWriteBatch> batches = ArgumentCaptor.forClass(KafkaMetricWriteBatch.class);
            Mockito.verify(store, Mockito.times(2)).write(batches.capture());
            Assertions.assertSame(batches.getAllValues().get(0), batches.getAllValues().get(1));
            Mockito.verify(collectors.constructed().get(0)).collect(Mockito.eq(admin), Mockito.any());
            collector.close();
        }
    }

    private KafkaCollectConfig config(long organizationId, long clusterId) {
        KafkaCollectConfig config = new KafkaCollectConfig();
        config.setOrganizationId(organizationId);
        config.setClusterId(clusterId);
        config.setClusterName("kafka-" + organizationId + '-' + clusterId);
        config.setBootstrapServers("127.0.0.1:9092");
        config.setIntervalMillis(5_000L);
        return config;
    }
}
