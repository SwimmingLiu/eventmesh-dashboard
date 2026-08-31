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

import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.apache.ibatis.session.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ReportHandlerManageTest {

    Configuration configuration = new Configuration();

    @Test
    public void test() {
        XMLLanguageDriver xmlLanguageDriver = new XMLLanguageDriver();

        String sql = """
            <script>
                select * from test where
                <if  test="id != null and id != 0">
                    id = #{id} and
                </if>
                status = ${status}
            </script>
            """;
        SqlSource sqlSource = xmlLanguageDriver.createSqlSource(configuration, sql, ReportHandlerManageTest.class);

        Map<String, Object> data = new HashMap<>();
        data.put("id", 1);
        data.put("status", 1);
        BoundSql s = sqlSource.getBoundSql(data);
        System.out.println(s.getSql());
    }

    @Test
    public void testQueryResultUsesReportNameAsResponseKey() {
        ReportEngine reportEngine = Mockito.mock(ReportEngine.class);
        Mockito.when(reportEngine.query(Mockito.any())).thenReturn(
            CompletableFuture.completedFuture(List.of(Map.of("value", 3L))));
        ReportHandlerManage manage = new ReportHandlerManage();
        manage.setReportEngine(reportEngine);
        SingleGeneralReportDO report = new SingleGeneralReportDO();
        report.setReportName("kafka_cluster_alive");
        report.setReportType("gauge");

        Map<String, List<Map<String, Object>>> result = manage.queryResultIsMap(List.of(report));

        Assert.assertEquals(3L, result.get("kafka_cluster_alive").get(0).get("value"));
    }
}
