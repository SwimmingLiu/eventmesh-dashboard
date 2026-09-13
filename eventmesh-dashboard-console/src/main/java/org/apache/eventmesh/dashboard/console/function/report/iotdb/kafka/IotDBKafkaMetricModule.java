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

import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaCollectConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaMetricCollect;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaMetricStore;
import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.sql.DataSource;

/**
 * Owns the IoTDB schema, writer, and query path for the Kafka metric domain.
 */
public final class IotDBKafkaMetricModule {

    private final KafkaMetricStore store;
    private final KafkaMetricQueryService queryService;

    public IotDBKafkaMetricModule(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.store = new IotDBKafkaMetricStore(dataSource);
        this.queryService = new KafkaMetricQueryService(dataSource);
    }

    public String name() {
        return "kafka";
    }

    public void initialize() {
        store.initialize();
    }

    public boolean supports(SingleGeneralReportDO report) {
        return queryService.supports(report);
    }

    public void validate(SingleGeneralReportDO report) {
        queryService.validate(report);
    }

    public List<Map<String, Object>> query(SingleGeneralReportDO report) {
        return queryService.query(report);
    }

    public KafkaMetricCollect createCollect(KafkaCollectConfig config) {
        return new KafkaMetricCollect(config, store);
    }
}
