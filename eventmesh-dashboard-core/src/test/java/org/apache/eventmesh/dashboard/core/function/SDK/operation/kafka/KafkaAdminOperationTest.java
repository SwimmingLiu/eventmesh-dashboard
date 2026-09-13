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

package org.apache.eventmesh.dashboard.core.function.SDK.operation.kafka;

import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateKakfaConfig;
import org.apache.eventmesh.dashboard.core.function.SDK.config.NetAddress;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaAdminOperationTest {

    private final KafkaAdminOperation operation = new KafkaAdminOperation();

    @Test
    public void testCreatesAdminClientFromSdkConfig() {
        CreateKakfaConfig config = new CreateKakfaConfig();
        config.setAdminProperties(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092",
            AdminClientConfig.CLIENT_ID_CONFIG, "dashboard-metrics-test"));

        AdminClient client = operation.createClient(config);

        Assertions.assertNotNull(client);
        operation.close(client);
    }

    @Test
    public void testRejectsBlankBootstrapServers() {
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> operation.createClient(new CreateKakfaConfig()));
    }

    @Test
    public void testCreatesAdminClientFromInheritedNetAddresses() {
        CreateKakfaConfig config = new CreateKakfaConfig();
        config.addNetAddress(NetAddress.create("127.0.0.1", 9092));

        AdminClient client = operation.createClient(config);

        Assertions.assertNotNull(client);
        operation.close(client);
    }
}
