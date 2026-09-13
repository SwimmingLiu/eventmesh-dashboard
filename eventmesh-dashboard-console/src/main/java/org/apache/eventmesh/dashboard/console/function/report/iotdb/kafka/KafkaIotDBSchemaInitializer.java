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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

final class KafkaIotDBSchemaInitializer {

    private final DataSource dataSource;

    KafkaIotDBSchemaInitializer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    void initialize() {
        List<String> statements = statements(loadScript());
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        } catch (SQLException ex) {
            throw new KafkaMetricStorageException("Unable to initialize Kafka metric tables", ex);
        }
    }

    String loadScript() {
        ClassLoader classLoader = KafkaIotDBSchemaInitializer.class.getClassLoader();
        try (InputStream input = classLoader.getResourceAsStream(KafkaIotDBSchema.INITIALIZATION_SCRIPT)) {
            if (input == null) {
                throw new IllegalStateException("Missing resource " + KafkaIotDBSchema.INITIALIZATION_SCRIPT);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new KafkaMetricStorageException("Unable to read Kafka metric initialization script", ex);
        }
    }

    List<String> statements(String script) {
        String withoutComments = Arrays.stream(script.split("\\R"))
            .map(String::trim)
            .filter(line -> !line.isEmpty() && !line.startsWith("--"))
            .reduce("", (left, right) -> left + System.lineSeparator() + right);
        return Arrays.stream(withoutComments.split(";"))
            .map(String::trim)
            .filter(sql -> !sql.isEmpty())
            .toList();
    }
}
