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

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.enums.RemotingType;
import org.apache.eventmesh.dashboard.core.function.SDK.AbstractSDKOperation;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKMetadata;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKTypeEnum;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateKakfaConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@SDKMetadata(clusterType = {ClusterType.STORAGE_KAFKA_BROKER, ClusterType.STORAGE_KAFKA_RAFT}, remotingType = RemotingType.KAFKA, sdkTypeEnum = {
    SDKTypeEnum.ADMIN, SDKTypeEnum.PING})
public class KafkaAdminOperation extends AbstractSDKOperation<AdminClient, CreateKakfaConfig> {

    @Override
    public AdminClient createClient(CreateKakfaConfig clientConfig) {
        Objects.requireNonNull(clientConfig, "clientConfig");
        Map<String, Object> properties = new HashMap<>();
        if (clientConfig.getAdminProperties() != null) {
            properties.putAll(clientConfig.getAdminProperties());
        }
        String[] netAddresses = clientConfig.getNetAddresses();
        if (!properties.containsKey(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)
            && netAddresses != null && netAddresses.length > 0) {
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", netAddresses));
        }
        if (StringUtils.isBlank(Objects.toString(properties.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG), null))) {
            throw new IllegalArgumentException("Kafka bootstrap servers must not be blank");
        }
        return AdminClient.create(properties);
    }

    @Override
    public void close(AdminClient client) {
        client.close();
    }
}
