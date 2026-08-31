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

package org.apache.eventmesh.dashboard.console.function.report.collect;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CollectManageTest {

    @Test
    public void testReplacesAndClosesManagedCollectByKey() {
        CollectManage manage = new CollectManage();
        ManagedCollect first = Mockito.mock(ManagedCollect.class);
        ManagedCollect second = Mockito.mock(ManagedCollect.class);
        Mockito.when(first.key()).thenReturn("kafka:9");
        Mockito.when(second.key()).thenReturn("kafka:9");

        manage.registerManagedCollect(first);
        manage.registerManagedCollect(second);
        manage.unregisterManagedCollect("kafka:9");

        Mockito.verify(first).close();
        Mockito.verify(second).close();
    }

    @Test
    public void testCloseReleasesAllCollectsAndRejectsNewRegistration() {
        CollectManage manage = new CollectManage();
        ManagedCollect first = Mockito.mock(ManagedCollect.class);
        ManagedCollect second = Mockito.mock(ManagedCollect.class);
        ManagedCollect rejected = Mockito.mock(ManagedCollect.class);
        Mockito.when(first.key()).thenReturn("kafka:7:9");
        Mockito.when(second.key()).thenReturn("kafka:8:9");
        Mockito.when(rejected.key()).thenReturn("kafka:9:9");
        manage.registerManagedCollect(first);
        manage.registerManagedCollect(second);

        manage.close();
        manage.close();

        Mockito.verify(first).close();
        Mockito.verify(second).close();
        Assertions.assertThrows(IllegalStateException.class,
            () -> manage.registerManagedCollect(rejected));
        Mockito.verify(rejected).close();
    }

    @Test
    public void testCloseWaitsForInFlightCollectionBeforeReleasingIt() throws Exception {
        CollectManage manage = new CollectManage();
        ManagedCollect collect = Mockito.mock(ManagedCollect.class);
        Mockito.when(collect.key()).thenReturn("kafka:7:9");
        CountDownLatch requestEntered = new CountDownLatch(1);
        CountDownLatch releaseRequest = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            requestEntered.countDown();
            releaseRequest.await(5L, TimeUnit.SECONDS);
            return null;
        }).when(collect).request();
        manage.registerManagedCollect(collect);
        manage.request();
        Assertions.assertTrue(requestEntered.await(1L, TimeUnit.SECONDS));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> closeFuture = executor.submit(manage::close);
            releaseRequest.countDown();
            closeFuture.get(1L, TimeUnit.SECONDS);
            Mockito.verify(collect).close();
        } finally {
            releaseRequest.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testReplacementWaitsForInFlightCollectionBeforeClosingOldCollect() throws Exception {
        CollectManage manage = new CollectManage();
        ManagedCollect first = Mockito.mock(ManagedCollect.class);
        ManagedCollect replacement = Mockito.mock(ManagedCollect.class);
        Mockito.when(first.key()).thenReturn("kafka:7:9");
        Mockito.when(replacement.key()).thenReturn("kafka:7:9");
        CountDownLatch requestEntered = new CountDownLatch(1);
        CountDownLatch releaseRequest = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            requestEntered.countDown();
            releaseRequest.await(5L, TimeUnit.SECONDS);
            return null;
        }).when(first).request();
        manage.registerManagedCollect(first);
        manage.request();
        Assertions.assertTrue(requestEntered.await(1L, TimeUnit.SECONDS));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> replaceFuture = executor.submit(() -> manage.registerManagedCollect(replacement));
            Thread.sleep(50L);
            Assertions.assertFalse(replaceFuture.isDone());
            Mockito.verify(first, Mockito.never()).close();
            manage.request();
            Mockito.verify(replacement, Mockito.after(100L).never()).request();

            releaseRequest.countDown();
            replaceFuture.get(1L, TimeUnit.SECONDS);
            Mockito.verify(first).close();
            manage.request();
            Mockito.verify(replacement, Mockito.timeout(1_000L)).request();
        } finally {
            releaseRequest.countDown();
            manage.close();
            executor.shutdownNow();
        }
    }
}
