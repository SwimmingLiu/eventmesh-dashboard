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

package org.apache.eventmesh.dashboard.console.function.report;

import org.apache.eventmesh.dashboard.console.function.report.collect.ManagedCollect;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ReportHandlerManageCloseTest {

    @Test
    public void testCloseReleasesConfiguredReportEngineOnce() {
        ReportEngine reportEngine = Mockito.mock(ReportEngine.class);
        ReportHandlerManage manage = new ReportHandlerManage();
        manage.setReportEngine(reportEngine);

        manage.close();
        manage.close();

        Mockito.verify(reportEngine).close();
    }

    @Test
    public void testInitializationFailureReleasesCreatedCollectsAndEngine() {
        ReportEngine reportEngine = Mockito.mock(ReportEngine.class);
        ManagedCollect collect = Mockito.mock(ManagedCollect.class);
        Mockito.when(collect.key()).thenReturn("kafka:7:9");
        Mockito.when(reportEngine.createCollects(Mockito.any())).thenReturn(List.of(collect));
        ReportHandlerManage manage = new ReportHandlerManage();
        manage.setReportEngine(reportEngine);
        manage.setReportConfig(new ReportConfig());

        Assertions.assertThrows(RuntimeException.class, manage::init);

        Mockito.verify(collect).close();
        Mockito.verify(reportEngine).close();
    }
}
