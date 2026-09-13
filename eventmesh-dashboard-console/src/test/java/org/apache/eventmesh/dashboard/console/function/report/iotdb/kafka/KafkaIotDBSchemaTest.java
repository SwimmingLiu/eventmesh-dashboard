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

import java.util.Locale;

import javax.sql.DataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class KafkaIotDBSchemaTest {

    @Test
    public void testInitializationScriptCoversEverySchemaColumn() {
        KafkaIotDBSchemaInitializer initializer =
            new KafkaIotDBSchemaInitializer(Mockito.mock(DataSource.class));
        String script = initializer.loadScript().toLowerCase(Locale.ROOT);
        String unquotedScript = script.replace("\"", "");

        Assertions.assertEquals(110, KafkaIotDBSchema.tables().stream().limit(7)
            .mapToInt(table -> table.fields().size()).sum());
        Assertions.assertTrue(script.contains("\"cluster_id\" string tag"));
        KafkaIotDBSchema.tables().forEach(table -> {
            Assertions.assertTrue(unquotedScript.contains("create table if not exists " + table.name()));
            table.tags().forEach(tag -> Assertions.assertTrue(unquotedScript.contains(tag + " string tag")));
            table.attributes().forEach((name, type) ->
                Assertions.assertTrue(unquotedScript.contains(name + " " + type.sqlType().toLowerCase(Locale.ROOT)
                    + " attribute")));
            table.fields().forEach((name, type) ->
                Assertions.assertTrue(unquotedScript.contains(name + " " + type.sqlType().toLowerCase(Locale.ROOT)
                    + " field")));
        });
        Assertions.assertEquals(11, initializer.statements(script).size());
    }
}
