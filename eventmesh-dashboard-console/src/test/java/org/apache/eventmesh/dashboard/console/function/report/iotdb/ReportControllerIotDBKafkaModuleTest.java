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

import org.apache.eventmesh.dashboard.console.controller.ReportController;
import org.apache.eventmesh.dashboard.console.function.report.ReportConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaCollectConfig;
import org.apache.eventmesh.dashboard.console.function.report.ReportHandlerManage;
import org.apache.eventmesh.dashboard.console.function.report.iotdb.kafka.KafkaReportService;
import org.apache.eventmesh.dashboard.console.spring.support.FunctionConfig;
import org.apache.eventmesh.dashboard.console.function.report.iotdb.kafka.IotDBKafkaMetricModule;
import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class ReportControllerIotDBKafkaModuleTest {

    @Test
    public void testQueriesKafkaMetricThroughHttpAndRealModuleRouting() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.getMetaData()).thenReturn(metaData);
        Mockito.when(metaData.getColumnCount()).thenReturn(2);
        Mockito.when(metaData.getColumnLabel(1)).thenReturn("broker_id");
        Mockito.when(metaData.getColumnLabel(2)).thenReturn("value");
        Mockito.when(resultSet.next()).thenReturn(true, false);
        Mockito.when(resultSet.getObject(1)).thenReturn("2");
        Mockito.when(resultSet.getObject(2)).thenReturn(42.5D);

        IotDBKafkaMetricModule module = new IotDBKafkaMetricModule(dataSource);
        ReportHandlerManage reportManage = new ReportHandlerManage();
        ReportConfig reportConfig = new ReportConfig();
        KafkaCollectConfig allowedScope = new KafkaCollectConfig();
        allowedScope.setOrganizationId(7L);
        allowedScope.setClusterId(9L);

        reportManage.setReportConfig(reportConfig);
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setReportConfig(reportConfig);
        KafkaReportService kafkaService = new KafkaReportService(functionConfig);
        ReflectionTestUtils.setField(kafkaService, "module", module);
        org.apache.eventmesh.dashboard.console.service.cluster.ClusterService clusters =
            Mockito.mock(org.apache.eventmesh.dashboard.console.service.cluster.ClusterService.class);
        org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity cluster =
            new org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity();
        cluster.setId(9L);
        cluster.setOrganizationId(7L);
        cluster.setStatus(1L);
        cluster.setClusterType(org.apache.eventmesh.dashboard.common.enums.ClusterType.STORAGE_KAFKA_CLUSTER);
        Mockito.when(clusters.queryClusterById(Mockito.any())).thenReturn(cluster);
        ReflectionTestUtils.setField(kafkaService, "clusterService", clusters);
        ReportController controller = new ReportController();
        ReflectionTestUtils.setField(controller, "reportHandlerManage", reportManage);
        ReflectionTestUtils.setField(controller, "kafkaReportService", kafkaService);
        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller).build();

        mockMvc.perform(post("/report/reportBySingle")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                      "reportName": "kafka_broker_bytes_in",
                      "organizationId": 7,
                      "clustersId": 9,
                      "brokerId": "2",
                      "startTime": "2026-08-30T00:00:00",
                      "endTime": "2026-08-31T00:00:00",
                      "order": "desc",
                      "limit": 50
                    }
                    """))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].broker_id").value("2"))
            .andExpect(jsonPath("$[0].value").value(42.5D));

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connection).prepareStatement(sql.capture());
        assertTrue(sql.getValue().contains("FROM kafka_broker_metrics"));
        assertTrue(sql.getValue().contains("\"bytes_in\" AS value"));
        assertTrue(sql.getValue().endsWith("ORDER BY time DESC LIMIT 50"));

        SingleGeneralReportDO unauthorized = new SingleGeneralReportDO();
        unauthorized.setReportName("kafka_broker_bytes_in");
        unauthorized.setOrganizationId(8L);
        unauthorized.setClustersId(9L);
        assertThrows(SecurityException.class,
            () -> kafkaService.query(unauthorized));
        mockMvc.perform(post("/report/reportBySingle").contentType(MediaType.APPLICATION_JSON)
                .content("{\"reportName\":\"kafka_broker_bytes_in\",\"organizationId\":8,\"clustersId\":9}"))
            .andExpect(status().isForbidden());
        mockMvc.perform(post("/report/reportBySingle").contentType(MediaType.APPLICATION_JSON)
                .content("{\"reportName\":\"kafka_broker_unknown\",\"organizationId\":7,\"clustersId\":9}"))
            .andExpect(status().isBadRequest());
        kafkaService.close();
    }
}
