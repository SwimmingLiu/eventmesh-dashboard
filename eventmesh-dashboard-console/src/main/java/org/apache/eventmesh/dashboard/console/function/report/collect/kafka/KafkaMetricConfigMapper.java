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

package org.apache.eventmesh.dashboard.console.function.report.collect.kafka;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.console.entity.base.BaseIdEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.RuntimeEntity;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaCollectConfig.KafkaBrokerJmxConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Converts existing database fields into a stable collector connection snapshot. */
public final class KafkaMetricConfigMapper {

    private final ObjectMapper objectMapper;

    public KafkaMetricConfigMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public static boolean isKafka(ClusterType type) {
        return type == ClusterType.STORAGE_KAFKA_CLUSTER || type == ClusterType.STORAGE_KAFKA_BROKER
            || type == ClusterType.STORAGE_KAFKA_RAFT;
    }

    public static boolean isActive(BaseIdEntity entity) {
        return entity != null && Objects.equals(entity.getStatus(), 1L) && !Objects.equals(entity.getIsDelete(), 1);
    }

    public KafkaCollectConfig map(ClusterEntity cluster, List<RuntimeEntity> runtimes) {
        if (cluster.getId() == null || cluster.getOrganizationId() == null) {
            throw new IllegalArgumentException("Kafka cluster identity is incomplete");
        }
        final List<RuntimeEntity> brokers = runtimes.stream()
            .filter(KafkaMetricConfigMapper::isActive)
            .filter(runtime -> isKafka(runtime.getClusterType()))
            .filter(runtime -> Objects.equals(runtime.getClusterId(), cluster.getId())
                && Objects.equals(runtime.getOrganizationId(), cluster.getOrganizationId()))
            .sorted(Comparator.comparing(RuntimeEntity::getId)).toList();
        KafkaCollectConfig config = new KafkaCollectConfig();
        config.setOrganizationId(cluster.getOrganizationId());
        config.setClusterId(cluster.getId());
        config.setClusterName(cluster.getName());
        Map<String, String> admin = new LinkedHashMap<>(properties(cluster.getConfig()));
        String bootstrapServers = admin.remove("bootstrapServers");
        config.setAdminProperties(Map.copyOf(admin));
        Map<String, String> jmx = properties(cluster.getJmxProperties());
        List<String> addresses = new ArrayList<>();
        List<KafkaBrokerJmxConfig> endpoints = new ArrayList<>();
        for (RuntimeEntity broker : brokers) {
            if (broker.getHost() == null || broker.getHost().isBlank() || broker.getPort() == null
                || broker.getPort() < 1 || broker.getPort() > 65_535) {
                throw new IllegalArgumentException("Kafka broker address is incomplete");
            }
            String host = broker.getHost();
            addresses.add((host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host) + ':' + broker.getPort());
            Integer jmxPort = broker.getJmxPort();
            if (jmxPort == null && jmx.containsKey("jmxPort")) {
                jmxPort = Integer.valueOf(jmx.get("jmxPort"));
            }
            if (jmxPort != null && jmxPort > 0) {
                endpoints.add(endpoint(broker, jmxPort, jmx));
            }
        }
        config.setBootstrapServers(bootstrapServers == null || bootstrapServers.isBlank()
            ? admin.getOrDefault("bootstrap.servers", String.join(",", addresses)) : bootstrapServers);
        config.setBrokers(List.copyOf(endpoints));
        return config;
    }

    private KafkaBrokerJmxConfig endpoint(RuntimeEntity runtime, int jmxPort, Map<String, String> properties) {
        if (runtime.getRuntimeIndex() == null || runtime.getRuntimeIndex() < 0) {
            throw new IllegalArgumentException("Kafka JMX requires the broker runtimeIndex");
        }
        KafkaBrokerJmxConfig endpoint = new KafkaBrokerJmxConfig();
        endpoint.setBrokerId(runtime.getRuntimeIndex());
        endpoint.setHost(runtime.getHost());
        endpoint.setPort(jmxPort);
        endpoint.setUsername(properties.get("username"));
        endpoint.setPassword(properties.get("password"));
        String ssl = properties.getOrDefault("ssl", properties.getOrDefault("com.sun.management.jmxremote.ssl", "false"));
        if (!"true".equalsIgnoreCase(ssl) && !"false".equalsIgnoreCase(ssl)) {
            throw new IllegalArgumentException("Kafka JMX ssl must be true or false");
        }
        endpoint.setSsl(Boolean.parseBoolean(ssl));
        if (properties.containsKey("connectionTimeoutMillis")) {
            endpoint.setConnectionTimeoutMillis(Long.parseLong(properties.get("connectionTimeoutMillis")));
        }
        if (properties.containsKey("requestTimeoutMillis")) {
            endpoint.setRequestTimeoutMillis(Long.parseLong(properties.get("requestTimeoutMillis")));
        }
        return endpoint;
    }

    private Map<String, String> properties(String value) {
        if (value == null || value.isBlank()) {
            return Map.of();
        }
        try {
            return Map.copyOf(objectMapper.readValue(value, new TypeReference<Map<String, String>>() { }));
        } catch (IOException | RuntimeException e) {
            // Parse exceptions may contain credentials from the input; keep them out of the error message.
            throw new IllegalArgumentException("Invalid Kafka properties in database", e);
        }
    }
}
