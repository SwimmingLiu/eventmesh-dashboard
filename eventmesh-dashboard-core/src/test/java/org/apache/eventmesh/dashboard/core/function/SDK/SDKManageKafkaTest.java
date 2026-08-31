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

package org.apache.eventmesh.dashboard.core.function.SDK;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.model.metadata.ClusterMetadata;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateKakfaConfig;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SDKManageKafkaTest {

    @Test
    public void testAdminAndPingShareManagedClient() {
        SDKManage sdkManage = SDKManage.getInstance();
        int lockCount = sdkManage.managedClientLockCount();
        ClusterMetadata metadata = new ClusterMetadata();
        metadata.setId(987_654_321L);
        metadata.setClusterId(987_654_321L);
        metadata.setClusterType(ClusterType.STORAGE_KAFKA_BROKER);
        CreateKakfaConfig config = new CreateKakfaConfig();
        config.setAdminProperties(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"));

        AdminClient client = sdkManage.createClient(SDKTypeEnum.ADMIN, metadata, config, metadata.getClusterType());

        Assertions.assertSame(client, sdkManage.getClient(SDKTypeEnum.ADMIN, metadata.getUnique()));
        Assertions.assertSame(client, sdkManage.getClient(SDKTypeEnum.PING, metadata.getUnique()));

        sdkManage.deleteClient(null, metadata.getUnique());
        Assertions.assertNull(sdkManage.getClientWrapper(metadata.getUnique()));
        Assertions.assertEquals(lockCount, sdkManage.managedClientLockCount());
    }

    @Test
    public void testStaleClientCannotDeleteReplacement() {
        SDKManage sdkManage = SDKManage.getInstance();
        ClusterMetadata metadata = new ClusterMetadata();
        metadata.setId(987_654_322L);
        metadata.setClusterId(987_654_322L);
        metadata.setClusterType(ClusterType.STORAGE_KAFKA_BROKER);
        CreateKakfaConfig config = new CreateKakfaConfig();
        config.setAdminProperties(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"));

        AdminClient first = sdkManage.createClient(SDKTypeEnum.ADMIN, metadata, config, metadata.getClusterType());
        AdminClient replacement = sdkManage.createClient(
            SDKTypeEnum.ADMIN, metadata, config, metadata.getClusterType());

        sdkManage.deleteClient(null, metadata.getUnique(), first);
        Assertions.assertSame(replacement, sdkManage.getClient(SDKTypeEnum.ADMIN, metadata.getUnique()));

        sdkManage.deleteClient(null, metadata.getUnique(), replacement);
        Assertions.assertNull(sdkManage.getClientWrapper(metadata.getUnique()));
    }

    @Test
    public void testFailedRetirementDoesNotPublishReplacement() {
        SDKManage sdkManage = SDKManage.getInstance();
        ClusterMetadata metadata = metadata(987_654_323L);
        CreateKakfaConfig config = config();
        AdminClient original = sdkManage.createClient(
            SDKTypeEnum.ADMIN, metadata, config, metadata.getClusterType());
        original.close();
        AdminClient failing = Mockito.mock(AdminClient.class);
        Mockito.doThrow(new RuntimeException("close failed")).when(failing).close();
        ClientWrapper wrapper = sdkManage.getClientWrapper(metadata.getUnique());
        wrapper.getClientMap().put(SDKTypeEnum.ADMIN, failing);
        wrapper.getClientMap().put(SDKTypeEnum.PING, failing);

        Assertions.assertThrows(RuntimeException.class,
            () -> sdkManage.createClient(SDKTypeEnum.ADMIN, metadata, config, metadata.getClusterType()));

        Assertions.assertNull(sdkManage.getClientWrapper(metadata.getUnique()));
    }

    @Test
    public void testSlowCloseDoesNotBlockDifferentClientKey() throws Exception {
        SDKManage sdkManage = SDKManage.getInstance();
        ClusterMetadata firstMetadata = metadata(987_654_324L);
        ClusterMetadata secondMetadata = metadata(987_654_325L);
        CreateKakfaConfig config = config();
        AdminClient original = sdkManage.createClient(
            SDKTypeEnum.ADMIN, firstMetadata, config, firstMetadata.getClusterType());
        original.close();
        CountDownLatch closeEntered = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        AdminClient slow = Mockito.mock(AdminClient.class);
        Mockito.doAnswer(invocation -> {
            closeEntered.countDown();
            releaseClose.await(5L, TimeUnit.SECONDS);
            return null;
        }).when(slow).close();
        ClientWrapper wrapper = sdkManage.getClientWrapper(firstMetadata.getUnique());
        wrapper.getClientMap().put(SDKTypeEnum.ADMIN, slow);
        wrapper.getClientMap().put(SDKTypeEnum.PING, slow);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> closeFuture = executor.submit(() -> sdkManage.deleteClient(null, firstMetadata.getUnique()));
            Assertions.assertTrue(closeEntered.await(1L, TimeUnit.SECONDS));

            Future<AdminClient> createFuture = executor.submit(() -> sdkManage.createClient(
                SDKTypeEnum.ADMIN, secondMetadata, config, secondMetadata.getClusterType()));
            AdminClient second = createFuture.get(1L, TimeUnit.SECONDS);
            Assertions.assertNotNull(second);

            releaseClose.countDown();
            closeFuture.get(1L, TimeUnit.SECONDS);
            sdkManage.deleteClient(null, secondMetadata.getUnique(), second);
        } finally {
            releaseClose.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testPartialDeleteFailureStillRemovesWrapperAndLock() {
        SDKManage sdkManage = SDKManage.getInstance();
        int lockCount = sdkManage.managedClientLockCount();
        ClusterMetadata metadata = metadata(987_654_326L);
        AdminClient original = sdkManage.createClient(
            SDKTypeEnum.ADMIN, metadata, config(), metadata.getClusterType());
        original.close();
        AdminClient failing = Mockito.mock(AdminClient.class);
        Mockito.doThrow(new RuntimeException("close failed")).when(failing).close();
        ClientWrapper wrapper = sdkManage.getClientWrapper(metadata.getUnique());
        wrapper.getClientMap().put(SDKTypeEnum.ADMIN, failing);
        wrapper.getClientMap().put(SDKTypeEnum.PING, failing);

        Assertions.assertThrows(RuntimeException.class,
            () -> sdkManage.deleteClient(SDKTypeEnum.ADMIN, metadata.getUnique(), failing));

        Assertions.assertNull(sdkManage.getClientWrapper(metadata.getUnique()));
        Assertions.assertEquals(lockCount, sdkManage.managedClientLockCount());
    }

    private ClusterMetadata metadata(long id) {
        ClusterMetadata metadata = new ClusterMetadata();
        metadata.setId(id);
        metadata.setClusterId(id);
        metadata.setClusterType(ClusterType.STORAGE_KAFKA_BROKER);
        return metadata;
    }

    private CreateKakfaConfig config() {
        CreateKakfaConfig config = new CreateKakfaConfig();
        config.setAdminProperties(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"));
        return config;
    }
}
