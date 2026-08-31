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

import org.apache.eventmesh.dashboard.console.function.report.ReportConfig;
import org.apache.eventmesh.dashboard.console.function.report.ReportConfig.KafkaCollectConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.ManagedCollect;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaMetricCollect;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaMetricStore;
import org.apache.eventmesh.dashboard.console.function.report.iotdb.IotDBMetricModule;
import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.sql.DataSource;

/**
 * Owns the IoTDB schema, writer, and query path for the Kafka metric domain.
 */
public final class IotDBKafkaMetricModule implements IotDBMetricModule {

    private final KafkaMetricStore store;
    private final KafkaMetricQueryService queryService;

    public IotDBKafkaMetricModule(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.store = new IotDBKafkaMetricStore(dataSource);
        this.queryService = new KafkaMetricQueryService(dataSource);
    }

    @Override
    public String name() {
        return "kafka";
    }

    @Override
    public void initialize() {
        store.initialize();
    }

    @Override
    public boolean supports(SingleGeneralReportDO report) {
        return queryService.supports(report);
    }

    @Override
    public List<Map<String, Object>> query(SingleGeneralReportDO report) {
        return queryService.query(report);
    }

    @Override
    public List<ManagedCollect> createCollects(ReportConfig reportConfig) {
        if (reportConfig == null || reportConfig.getKafkaCollectConfigList() == null) {
            return List.of();
        }
        List<ManagedCollect> collects = new ArrayList<>();
        try {
            reportConfig.getKafkaCollectConfigList().stream()
                .filter(KafkaCollectConfig::isEnabled)
                .map(config -> (ManagedCollect) new KafkaMetricCollect(config, store))
                .forEach(collects::add);
            return List.copyOf(collects);
        } catch (RuntimeException e) {
            for (ManagedCollect collect : collects) {
                try {
                    collect.close();
                } catch (RuntimeException closeException) {
                    e.addSuppressed(closeException);
                }
            }
            throw e;
        }
    }
}
