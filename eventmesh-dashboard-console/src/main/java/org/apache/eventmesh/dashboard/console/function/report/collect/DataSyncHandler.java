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


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.dashboard.console.function.report.collect.padding.PaddingService;

/** Pads and persists completed collector batches independently, retaining failed batches for replay. */
@Slf4j
public class DataSyncHandler {


    private PaddingService paddingService;

    @Setter
    private Consumer<Map<Class<?>, List<Object>>> batchWriter;

    public DataSyncHandlerWrapper getDataSyncHandlerWrapper(int count) {
        return new DataSyncHandlerWrapper(count);
    }

    /** Preserves the existing entry point while each batch is claimed and persisted only once. */
    public void padding(DataSyncHandlerWrapper wrapper) {
        for (int index = 0; index < wrapper.batches.length(); index++) {
            wrapper.persist(index);
        }
    }

    private void persist(RestoreData batch) {
        Map<Class<?>, List<Object>> samples = new LinkedHashMap<>();
        batch.getDataMap().forEach((type, values) -> samples.put(type, new ArrayList<>(values)));
        try {
            if (!samples.isEmpty()) {
                Objects.requireNonNull(batchWriter, "batchWriter").accept(samples);
            }
            batch.complete();
        } catch (RuntimeException | Error exception) {
            batch.restore();
            throw exception;
        }
    }

    public class DataSyncHandlerWrapper {

        private final AtomicReferenceArray<RestoreData> batches;
        private final AtomicIntegerArray completed;
        private final AtomicInteger nextIndex = new AtomicInteger();
        private final AtomicInteger remaining;

        public DataSyncHandlerWrapper(int count) {
            if (count < 0) {
                throw new IllegalArgumentException("Collector count cannot be negative");
            }
            batches = new AtomicReferenceArray<>(count);
            completed = new AtomicIntegerArray(count);
            remaining = new AtomicInteger(count);
        }

        public void sync(RestoreData batch) {
            int index = batch.getIndex();
            if (!batches.compareAndSet(index, null, batch)) {
                throw new IllegalStateException("Collector batch index was submitted twice");
            }
            persist(index);
        }

        private void persist(int index) {
            RestoreData batch = batches.get(index);
            if (batch != null && completed.compareAndSet(index, 0, 1)) {
                try {
                    DataSyncHandler.this.persist(batch);
                } finally {
                    remaining.decrementAndGet();
                }
            }
        }

        /** Signals an acquisition failure without delaying any other collector's completed batch. */
        public void failed(int index) {
            if (completed.compareAndSet(index, 0, 1)) {
                remaining.decrementAndGet();
            }
        }

        public int getIndex() {
            int index = nextIndex.getAndIncrement();
            if (index >= batches.length()) {
                throw new IllegalStateException("More collectors than registered batch slots");
            }
            return index;
        }

        public void shutdown() {
            if (remaining.get() > 0) {
                log.warn("Collectors have not completed: {}", remaining.get());
            }
        }
    }
}
