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

import org.apache.eventmesh.dashboard.common.model.metadata.ClusterMetadata;
import org.apache.eventmesh.dashboard.console.function.report.model.base.ClusterId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class AbstractCollectKafkaIsolationTest {

    @Test
    public void testFailedWriteReplaysSameSampleWithoutCollectingAgain() {
        DataSyncHandler handler = new DataSyncHandler();
        AtomicInteger writes = new AtomicInteger();
        SampleCollect collector = new SampleCollect();
        handler.setBatchWriter(samples -> {
            Assertions.assertSame(collector.sample, samples.get(ClusterId.class).get(0));
            Assertions.assertEquals(7L, collector.sample.getOrganizationId());
            Assertions.assertEquals(9L, collector.sample.getClustersId());
            if (writes.getAndIncrement() == 0) {
                throw new IllegalStateException("storage unavailable");
            }
        });
        Assertions.assertThrows(IllegalStateException.class,
            () -> collector.collect(0, handler.getDataSyncHandlerWrapper(1)));
        collector.collect(0, handler.getDataSyncHandlerWrapper(1));
        Assertions.assertEquals(1, collector.acquisitions);
        Assertions.assertEquals(2, writes.get());
    }

    @Test
    public void testFailedCollectorDoesNotPreventOtherCollectorPersistence() {
        DataSyncHandler handler = new DataSyncHandler();
        java.util.function.Consumer<Map<Class<?>, List<Object>>> writer = Mockito.mock(java.util.function.Consumer.class);
        handler.setBatchWriter(writer);
        DataSyncHandler.DataSyncHandlerWrapper wrapper = handler.getDataSyncHandlerWrapper(2);
        SampleCollect failed = new SampleCollect();
        failed.fail = true;
        Assertions.assertThrows(IllegalStateException.class, () -> failed.collect(0, wrapper));
        SampleCollect healthy = new SampleCollect();
        healthy.collect(1, wrapper);
        Mockito.verify(writer).accept(Map.of(ClusterId.class, List.of(healthy.sample)));
        Mockito.verifyNoMoreInteractions(writer);
    }

    @Test
    public void testConcurrentCompletionAndRepeatedPaddingWriteEachBatchOnce() {
        DataSyncHandler handler = new DataSyncHandler();
        AtomicInteger writes = new AtomicInteger();
        handler.setBatchWriter(samples -> writes.incrementAndGet());
        DataSyncHandler.DataSyncHandlerWrapper wrapper = handler.getDataSyncHandlerWrapper(2);
        CompletableFuture<Void> second = CompletableFuture.runAsync(() -> new SampleCollect().collect(1, wrapper));
        CompletableFuture<Void> first = CompletableFuture.runAsync(() -> new SampleCollect().collect(0, wrapper));
        CompletableFuture.allOf(first, second).join();
        handler.padding(wrapper);
        Assertions.assertEquals(2, writes.get());
    }

    private static final class SampleCollect extends AbstractCollect {

        private final ClusterId sample = new ClusterId();
        private int acquisitions;
        private boolean fail;

        private SampleCollect() {
            ClusterMetadata metadata = new ClusterMetadata();
            metadata.setId(9L);
            metadata.setOrganizationId(7L);
            metadata.setClusterName("kafka");
            setClusterMetadata(metadata);
        }

        @Override
        protected void doCollect() {
            acquisitions++;
            setData(sample);
            if (fail) {
                throw new IllegalStateException("acquisition failed");
            }
        }
    }
}
