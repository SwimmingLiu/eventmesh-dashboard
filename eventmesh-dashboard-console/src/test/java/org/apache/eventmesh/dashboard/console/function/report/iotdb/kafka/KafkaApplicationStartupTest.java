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

import org.apache.eventmesh.dashboard.console.EventMeshDashboardApplication;

import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaCollectConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaMetricCollect;
import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;
import org.apache.eventmesh.dashboard.console.spring.support.FunctionConfig;

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaApplicationStartupTest {

    @Test
    public void testApplicationStartsAndDatabaseChangesReconfigureKafka() {
        SpringApplication application = new SpringApplication(EventMeshDashboardApplication.class);
        application.setWebApplicationType(WebApplicationType.SERVLET);
        try (ConfigurableApplicationContext context = application.run(
            "--server.port=0",
            "--function.enabled=false",
            "--function.enabledReport=false",
            "--function.enabledHealth=false",
            "--function.enabledSync=false",
            "--spring.datasource.druid.driver-class-name=org.h2.Driver",
            "--spring.datasource.druid.url=jdbc:h2:mem:kafka_startup;MODE=MySQL;DB_CLOSE_DELAY=-1",
            "--spring.datasource.druid.username=sa",
            "--spring.datasource.druid.password=")) {
            assertTrue(context.isActive());
            verifyDatabaseChanges(context);
        }
    }
    private void verifyDatabaseChanges(ConfigurableApplicationContext context) {
        JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
        jdbc.execute("CREATE TABLE cluster (id BIGINT, organization_id BIGINT, cluster_type VARCHAR(80),"
            + " name VARCHAR(80), config VARCHAR(2000), jmx_properties VARCHAR(2000), status BIGINT, is_delete INT)");
        jdbc.execute("CREATE TABLE runtime (id BIGINT, organization_id BIGINT, cluster_id BIGINT,"
            + " cluster_type VARCHAR(80), host VARCHAR(100), port INT, jmx_port INT, runtime_index INT, status BIGINT, is_delete INT)");
        jdbc.update("INSERT INTO cluster VALUES (1, 1, 'STORAGE_KAFKA_CLUSTER', 'eventmesh-local-kafka', ?, ?, 1, 0)",
            "{\"bootstrapServers\":\"127.0.0.1:9092,127.0.0.1:9094\"}", "{\"jmxPort\":9999}");
        jdbc.update("INSERT INTO runtime VALUES (1, 1, 1, 'STORAGE_KAFKA_BROKER', '127.0.0.1', 9092, 9999, 0, 1, 0)");
        IotDBKafkaMetricModule module = Mockito.mock(IotDBKafkaMetricModule.class);
        Mockito.when(module.createCollect(Mockito.any())).thenAnswer(call -> Mockito.mock(KafkaMetricCollect.class));
        KafkaReportService service = context.getBean(KafkaReportService.class);
        ReflectionTestUtils.setField(service, "module", module);
        context.getBean(FunctionConfig.class).setEnabledReport(true);
        service.sync();
        Map<?, ?> tasks = (Map<?, ?>) ReflectionTestUtils.getField(service, "tasks");
        Runnable task = (Runnable) tasks.get(1L);
        Assertions.assertNotNull(task);
        task.run();
        jdbc.update("UPDATE runtime SET jmx_port=9998 WHERE id=1");
        service.sync();
        task.run();
        ArgumentCaptor<KafkaCollectConfig> configs = ArgumentCaptor.forClass(KafkaCollectConfig.class);
        Mockito.verify(module, Mockito.times(2)).createCollect(configs.capture());
        Assertions.assertEquals("127.0.0.1:9092,127.0.0.1:9094", configs.getValue().getBootstrapServers());
        Assertions.assertEquals(9998, configs.getValue().getBrokers().get(0).getPort());
        SingleGeneralReportDO query = new SingleGeneralReportDO();
        query.setOrganizationId(1L);
        query.setClustersId(1L);
        service.validate(query);
        jdbc.update("UPDATE cluster SET status=0 WHERE id=1");
        service.sync();
        task.run();
        Assertions.assertTrue(tasks.isEmpty());
        Assertions.assertThrows(SecurityException.class, () -> service.validate(query));
    }

}
