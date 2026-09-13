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

import org.apache.eventmesh.dashboard.common.enums.MetadataType;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataDataManageSqlTest {

    @Test
    public void shouldGenerateStableMetadataResultSetColumns() {
        MetadataDataManage metadataDataManage = new MetadataDataManage();

        String sql = metadataDataManage.createSql(MetadataType.CLUSTER, "cluster", "organization_id", "name");

        assertTrue(sql.contains("as metadata_type"), sql);
        assertTrue(sql.contains("organization_id as super_id"), sql);
        assertTrue(sql.contains("name as name"), sql);
    }

    @Test
    public void shouldIndexMetadataReadFromTheResultSet() throws Exception {
        MetadataDataManage metadataDataManage = new MetadataDataManage();
        DruidDataSource dataSource = Mockito.mock(DruidDataSource.class);
        DruidPooledConnection connection = Mockito.mock(DruidPooledConnection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        LocalDateTime metadataTime = LocalDateTime.of(2026, 9, 13, 18, 0);

        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true, false);
        Mockito.when(resultSet.getString("metadata_type")).thenReturn("CLUSTER");
        Mockito.when(resultSet.getString("name")).thenReturn("cluster-name");
        Mockito.when(resultSet.getLong("id")).thenReturn(1L);
        Mockito.when(resultSet.getLong("super_id")).thenReturn(2L);
        Mockito.when(resultSet.getInt("is_delete")).thenReturn(0);
        Mockito.when(resultSet.getLong("status")).thenReturn(1L);
        Mockito.when(resultSet.getTimestamp("create_time")).thenReturn(Timestamp.valueOf(metadataTime));
        Mockito.when(resultSet.getTimestamp("update_time")).thenReturn(Timestamp.valueOf(metadataTime));

        ReflectionTestUtils.setField(metadataDataManage, "dataSource", dataSource);
        ReflectionTestUtils.setField(metadataDataManage, "sql", "select ?");
        ReflectionTestUtils.setField(metadataDataManage, "selectObjectCount", 1);
        ReflectionTestUtils.setField(metadataDataManage, "localDateTime", metadataTime);

        metadataDataManage.syncData();

        assertEquals(1L, metadataDataManage.getId(MetadataType.CLUSTER, 2L, "cluster-name"));
        Mockito.verify(statement).setObject(1, metadataTime);
    }
}
