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
import org.apache.eventmesh.dashboard.common.model.base.BaseSyncBase;
import org.apache.eventmesh.dashboard.console.function.report.ReportConfig.KafkaBrokerJmxConfig;
import org.apache.eventmesh.dashboard.console.function.report.ReportConfig.KafkaCollectConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.ManagedCollect;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKManage;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKTypeEnum;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateKakfaConfig;
import org.apache.eventmesh.dashboard.core.gather.jmx.JmxConnectionConfig;
import org.apache.eventmesh.dashboard.core.gather.kafka.collector.KafkaBrokerJmxEndpoint;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the Kafka Admin client and JMX endpoints for one configured cluster.
 */
public final class KafkaMetricCollect implements ManagedCollect {

    private final String organizationId;
    private final String clusterId;
    private final String clusterName;
    private final long intervalMillis;
    private final Admin sdkClient;
    private final List<KafkaBrokerJmxEndpoint> endpoints;
    private final KafkaMetricPersistenceCoordinator coordinator;
    private final Clock clock;
    private final SDKManage sdkManage;
    private final String sdkClientKey;
    private final AtomicBoolean collecting = new AtomicBoolean();

    private volatile Instant nextCollectionAt = Instant.EPOCH;

    public KafkaMetricCollect(KafkaCollectConfig config, KafkaMetricStore store) {
        this(config, store, Clock.systemUTC(), SDKManage.getInstance());
    }

    KafkaMetricCollect(KafkaCollectConfig config, KafkaMetricStore store, Clock clock) {
        this(config, store, clock, SDKManage.getInstance());
    }

    KafkaMetricCollect(KafkaCollectConfig config, KafkaMetricStore store, Clock clock, SDKManage sdkManage) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(store, "store");
        this.organizationId = requireId(config.getOrganizationId(), "organizationId");
        this.clusterId = requireId(config.getClusterId(), "clusterId");
        this.clusterName = requireText(config.getClusterName(), "clusterName");
        this.intervalMillis = requirePositive(config.getIntervalMillis(), "intervalMillis");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sdkManage = Objects.requireNonNull(sdkManage, "sdkManage");
        this.endpoints = createEndpoints(config.getBrokers());
        this.coordinator = new KafkaMetricPersistenceCoordinator(store);

        CreateKakfaConfig sdkConfig = new CreateKakfaConfig();
        Map<String, Object> adminProperties = new HashMap<>();
        if (config.getAdminProperties() != null) {
            adminProperties.putAll(config.getAdminProperties());
        }
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
            requireText(config.getBootstrapServers(), "bootstrapServers"));
        sdkConfig.setAdminProperties(adminProperties);
        KafkaMetricSDKMetadata sdkMetadata = new KafkaMetricSDKMetadata(config.getOrganizationId(), config.getClusterId());
        this.sdkClientKey = sdkMetadata.getUnique();
        this.sdkClient = this.sdkManage.createClient(SDKTypeEnum.ADMIN, sdkMetadata, sdkConfig,
            sdkMetadata.getClusterType());
    }

    public String clusterId() {
        return clusterId;
    }

    @Override
    public String key() {
        return "kafka:" + organizationId + ':' + clusterId;
    }

    /**
     * Collects at most once per configured interval and never overlaps a previous cycle.
     */
    @Override
    public void request() {
        Instant now = clock.instant();
        if (now.isBefore(nextCollectionAt) || !collecting.compareAndSet(false, true)) {
            return;
        }
        try {
            coordinator.collectAndStore(organizationId, clusterId, clusterName, sdkClient, endpoints);
        } finally {
            nextCollectionAt = clock.instant().plusMillis(intervalMillis);
            collecting.set(false);
        }
    }

    @Override
    public void close() {
        sdkManage.deleteClient(null, sdkClientKey, sdkClient);
    }

    private static List<KafkaBrokerJmxEndpoint> createEndpoints(List<KafkaBrokerJmxConfig> configs) {
        if (configs == null || configs.isEmpty()) {
            return List.of();
        }
        List<KafkaBrokerJmxEndpoint> result = new ArrayList<>(configs.size());
        for (KafkaBrokerJmxConfig config : configs) {
            JmxConnectionConfig.Builder builder = JmxConnectionConfig.builder(
                    requireText(config.getHost(), "broker.host"), config.getPort())
                .ssl(config.isSsl())
                .connectionTimeoutMillis(config.getConnectionTimeoutMillis())
                .requestTimeoutMillis(config.getRequestTimeoutMillis());
            boolean hasUsername = StringUtils.isNotBlank(config.getUsername());
            boolean hasPassword = StringUtils.isNotBlank(config.getPassword());
            if (hasUsername != hasPassword) {
                throw new IllegalArgumentException("broker username and password must be configured together");
            }
            if (hasUsername) {
                builder.credentials(config.getUsername(), config.getPassword());
            }
            result.add(new KafkaBrokerJmxEndpoint(config.getBrokerId(), builder.build()));
        }
        return List.copyOf(result);
    }

    private static String requireId(Long value, String name) {
        if (value == null || value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value.toString();
    }

    private static String requireText(String value, String name) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static long requirePositive(long value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be greater than zero");
        }
        return value;
    }

    private static final class KafkaMetricSDKMetadata extends BaseSyncBase {

        private final String instanceId = UUID.randomUUID().toString();

        private KafkaMetricSDKMetadata(Long organizationId, Long clusterId) {
            setId(clusterId);
            setOrganizationId(organizationId);
            setClusterId(clusterId);
            setClusterType(ClusterType.STORAGE_KAFKA_BROKER);
        }

        @Override
        public String nodeUnique() {
            return getClusterId().toString();
        }

        @Override
        public String getUnique() {
            return "KafkaMetricCollect-" + getOrganizationId() + '-' + getClusterId() + '-' + instanceId;
        }
    }
}
