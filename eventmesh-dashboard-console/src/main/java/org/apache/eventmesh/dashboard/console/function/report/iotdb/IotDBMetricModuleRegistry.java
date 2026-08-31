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

import org.apache.eventmesh.dashboard.console.function.report.ReportConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.ManagedCollect;
import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class IotDBMetricModuleRegistry {

    private final Map<String, IotDBMetricModule> modules = new LinkedHashMap<>();

    void register(IotDBMetricModule module) {
        Objects.requireNonNull(module, "module");
        String name = module.name();
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("IoTDB metric module name must not be blank");
        }
        if (modules.putIfAbsent(name, module) != null) {
            throw new IllegalArgumentException("Duplicate IoTDB metric module: " + name);
        }
    }

    void initialize() {
        modules.values().forEach(IotDBMetricModule::initialize);
    }

    Optional<IotDBMetricModule> resolve(SingleGeneralReportDO report) {
        return modules.values().stream().filter(module -> module.supports(report)).findFirst();
    }

    List<ManagedCollect> createCollects(ReportConfig reportConfig) {
        return modules.values().stream().flatMap(module -> module.createCollects(reportConfig).stream()).toList();
    }
}
