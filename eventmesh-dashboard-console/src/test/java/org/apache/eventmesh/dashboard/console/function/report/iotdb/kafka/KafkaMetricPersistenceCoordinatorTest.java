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

import org.apache.eventmesh.dashboard.core.gather.kafka.collector.KafkaMetricsCollector;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;

import org.apache.kafka.clients.admin.Admin;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class KafkaMetricPersistenceCoordinatorTest {

    @Test
    public void testCollectsAndStoresOneBatch() {
        KafkaMetricsCollector collector = Mockito.mock(KafkaMetricsCollector.class);
        KafkaMetricStore store = Mockito.mock(KafkaMetricStore.class);
        Admin admin = Mockito.mock(Admin.class);
        Instant collectedAt = Instant.parse("2026-08-30T00:00:00Z");
        KafkaMetricCollection metrics = KafkaMetricCollection.builder().build();
        Mockito.when(collector.collect(admin, List.of())).thenReturn(metrics);
        KafkaMetricPersistenceCoordinator coordinator = new KafkaMetricPersistenceCoordinator(collector, store,
            Clock.fixed(collectedAt, ZoneOffset.UTC));

        KafkaMetricCollection result = coordinator.collectAndStore("org-1", "cluster-1", "primary", admin,
            List.of());

        Assert.assertSame(metrics, result);
        ArgumentCaptor<KafkaMetricWriteBatch> batch = ArgumentCaptor.forClass(KafkaMetricWriteBatch.class);
        Mockito.verify(store).write(batch.capture());
        Assert.assertEquals("org-1", batch.getValue().organizationId());
        Assert.assertEquals("cluster-1", batch.getValue().clusterId());
        Assert.assertEquals("primary", batch.getValue().clusterName());
        Assert.assertEquals(collectedAt, batch.getValue().collectedAt());
        Assert.assertSame(metrics, batch.getValue().metrics());
        Assert.assertTrue(batch.getValue().durationMillis() >= 0);
    }
}
