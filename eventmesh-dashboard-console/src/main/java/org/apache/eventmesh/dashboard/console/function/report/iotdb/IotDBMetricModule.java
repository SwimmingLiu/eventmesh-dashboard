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

import java.util.List;
import java.util.Map;

/**
 * A self-contained metric domain backed by the IoTDB report engine.
 */
public interface IotDBMetricModule {

    String name();

    void initialize();

    boolean supports(SingleGeneralReportDO report);

    List<Map<String, Object>> query(SingleGeneralReportDO report);

    default List<ManagedCollect> createCollects(ReportConfig reportConfig) {
        return List.of();
    }
}
