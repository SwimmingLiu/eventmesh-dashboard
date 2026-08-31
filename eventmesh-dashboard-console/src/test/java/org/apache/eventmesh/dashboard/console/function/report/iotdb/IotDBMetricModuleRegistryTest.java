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

package org.apache.eventmesh.dashboard.console.function.report.iotdb;

import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;
import org.apache.eventmesh.dashboard.console.function.report.collect.ManagedCollect;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IotDBMetricModuleRegistryTest {

    @Test
    public void testInitializesAndResolvesRegisteredModule() {
        IotDBMetricModuleRegistry registry = new IotDBMetricModuleRegistry();
        AtomicBoolean initialized = new AtomicBoolean();
        SingleGeneralReportDO report = new SingleGeneralReportDO();
        IotDBMetricModule module = module("kafka", report, initialized);

        registry.register(module);
        registry.initialize();

        Assertions.assertTrue(initialized.get());
        Assertions.assertSame(module, registry.resolve(report).orElseThrow());
        Assertions.assertTrue(registry.createCollects(null).isEmpty());
    }

    @Test
    public void testRejectsDuplicateModuleName() {
        IotDBMetricModuleRegistry registry = new IotDBMetricModuleRegistry();
        registry.register(module("kafka", null, new AtomicBoolean()));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> registry.register(module("kafka", null, new AtomicBoolean())));
    }

    @Test
    public void testCreatesCollectsFromRegisteredModules() {
        IotDBMetricModuleRegistry registry = new IotDBMetricModuleRegistry();
        ManagedCollect collect = org.mockito.Mockito.mock(ManagedCollect.class);
        registry.register(new IotDBMetricModule() {
            @Override
            public String name() {
                return "kafka";
            }

            @Override
            public void initialize() {
            }

            @Override
            public boolean supports(SingleGeneralReportDO report) {
                return false;
            }

            @Override
            public List<Map<String, Object>> query(SingleGeneralReportDO report) {
                return List.of();
            }

            @Override
            public List<ManagedCollect> createCollects(
                org.apache.eventmesh.dashboard.console.function.report.ReportConfig reportConfig) {
                return List.of(collect);
            }
        });

        Assertions.assertEquals(List.of(collect), registry.createCollects(null));
    }

    private IotDBMetricModule module(String name, SingleGeneralReportDO supportedReport,
        AtomicBoolean initialized) {
        return new IotDBMetricModule() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public void initialize() {
                initialized.set(true);
            }

            @Override
            public boolean supports(SingleGeneralReportDO report) {
                return report == supportedReport;
            }

            @Override
            public List<Map<String, Object>> query(SingleGeneralReportDO report) {
                return List.of();
            }
        };
    }
}
