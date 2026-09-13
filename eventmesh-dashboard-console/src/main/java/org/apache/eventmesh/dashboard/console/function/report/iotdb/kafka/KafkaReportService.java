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

import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.RuntimeEntity;
import org.apache.eventmesh.dashboard.console.function.report.ReportConfig.ReportEngineConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaCollectConfig;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaMetricCollect;
import org.apache.eventmesh.dashboard.console.function.report.collect.kafka.KafkaMetricConfigMapper;
import org.apache.eventmesh.dashboard.console.function.report.iotdb.IotDBReportEngine;
import org.apache.eventmesh.dashboard.console.function.report.model.SingleGeneralReportDO;
import org.apache.eventmesh.dashboard.console.service.cluster.ClusterService;
import org.apache.eventmesh.dashboard.console.service.cluster.RuntimeService;
import org.apache.eventmesh.dashboard.console.spring.support.FunctionConfig;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.druid.pool.DruidDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/** Reconciles Kafka collection tasks with the existing database services. */
@Slf4j
@Component
public class KafkaReportService {

    private static final long POLL_INTERVAL_MILLIS = 1_000L;
    private static final int MAX_CLUSTERS = 64;
    private static final int CONNECTION_WAIT_MILLIS = 5_000;
    private final FunctionConfig functionConfig;
    private final Map<Long, CollectionTask> tasks = new HashMap<>();
    @Autowired
    private ClusterService clusterService;
    @Autowired
    private RuntimeService runtimeService;
    @Autowired
    private ObjectMapper objectMapper;
    private DruidDataSource dataSource;
    private volatile IotDBKafkaMetricModule module;
    private ScheduledThreadPoolExecutor executor;
    private boolean closed;

    public KafkaReportService(FunctionConfig functionConfig) {
        this.functionConfig = functionConfig;
    }

    /** Called by FunctionManage's existing database synchronization cycle. */
    public synchronized void sync() {
        if (closed) {
            return;
        }
        if (!functionConfig.isEnabledReport()) {
            stopMissingTasks(Set.of());
            return;
        }
        try {
            // Incremental queries omit some deleted records; the existing full reads detect removals as well.
            List<ClusterEntity> clusters = clusterService.selectAll().stream()
                .filter(KafkaMetricConfigMapper::isActive)
                .filter(cluster -> KafkaMetricConfigMapper.isKafka(cluster.getClusterType())).toList();
            List<RuntimeEntity> runtimes = runtimeService.selectAll();
            Set<Long> activeIds = new HashSet<>();
            KafkaMetricConfigMapper mapper = new KafkaMetricConfigMapper(objectMapper);
            for (ClusterEntity cluster : clusters) {
                try {
                    KafkaCollectConfig config = mapper.map(cluster, runtimes);
                    if (config.getBootstrapServers().isBlank()) {
                        continue;
                    }
                    activeIds.add(cluster.getId());
                    reconcile(config);
                } catch (RuntimeException e) {
                    log.warn("Invalid Kafka collection configuration for cluster {} ({})", cluster.getId(), e.getClass().getSimpleName());
                }
            }
            stopMissingTasks(activeIds);
        } catch (RuntimeException e) {
            // Keep current tasks if the database snapshot could not be read; retry at the next synchronization.
            log.warn("Unable to synchronize Kafka collection configuration ({})", e.getClass().getSimpleName());
        }
    }

    private void reconcile(KafkaCollectConfig config) {
        CollectionTask task = tasks.get(config.getClusterId());
        if (task != null) {
            task.desiredConfig = config;
            return;
        }
        if (tasks.size() >= MAX_CLUSTERS) {
            throw new IllegalStateException("Too many Kafka report clusters");
        }
        initializeModule();
        if (executor == null) {
            AtomicInteger sequence = new AtomicInteger();
            executor = new ScheduledThreadPoolExecutor(MAX_CLUSTERS, runnable -> {
                Thread thread = new Thread(runnable, "kafka-report-" + sequence.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });
            executor.setRemoveOnCancelPolicy(true);
        }
        task = new CollectionTask(config);
        task.future = executor.scheduleWithFixedDelay(task, POLL_INTERVAL_MILLIS, POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        tasks.put(config.getClusterId(), task);
    }

    private void initializeModule() {
        if (module != null) {
            return;
        }
        ReportEngineConfig engine = functionConfig.getReportConfig() == null
            ? null : functionConfig.getReportConfig().getDefaultConfig();
        if (engine == null || !"iotdb".equals(engine.getEngineType())) {
            throw new IllegalArgumentException("Kafka metrics require an IoTDB default report engine");
        }
        DruidDataSource source = new DruidDataSource();
        try {
            IotDBReportEngine initializer = new IotDBReportEngine();
            initializer.setReportEngineConfig(engine);
            initializer.initDatabase();
            source.setUrl("jdbc:iotdb://" + engine.getEngineAddress() + "/eventmesh_dashboard?sql_dialect=table");
            source.setDriverClassName("org.apache.iotdb.jdbc.IoTDBDriver");
            source.setUsername("root");
            source.setPassword("root");
            source.setMaxActive(MAX_CLUSTERS);
            source.setMaxWait(CONNECTION_WAIT_MILLIS);
            source.init();
            IotDBKafkaMetricModule initialized = new IotDBKafkaMetricModule(source);
            initialized.initialize();
            dataSource = source;
            module = initialized;
        } catch (SQLException | RuntimeException e) {
            source.close();
            throw new IllegalStateException("Unable to initialize Kafka reporting", e);
        }
    }

    private void stopMissingTasks(Set<Long> activeIds) {
        tasks.entrySet().removeIf(entry -> {
            if (activeIds.contains(entry.getKey())) {
                return false;
            }
            CollectionTask task = entry.getValue();
            task.desiredConfig = null;
            task.future.cancel(false);
            executor.execute(task);
            return true;
        });
    }

    /** Recognizes Kafka requests even before the first database synchronization. */
    public boolean supports(SingleGeneralReportDO report) {
        return report != null && ((report.getKafkaDimension() != null && !report.getKafkaDimension().isBlank())
            || (report.getReportName() != null && report.getReportName().regionMatches(true, 0, "kafka_", 0, 6)));
    }

    public void validate(SingleGeneralReportDO report) {
        if (!functionConfig.isEnabledReport() || module == null) {
            throw new IllegalStateException("Kafka reporting is not enabled");
        }
        module.validate(report);
        ClusterEntity condition = new ClusterEntity();
        condition.setId(report.getClustersId());
        ClusterEntity cluster = clusterService.queryClusterById(condition);
        if (!KafkaMetricConfigMapper.isActive(cluster) || !KafkaMetricConfigMapper.isKafka(cluster.getClusterType())
            || !Objects.equals(cluster.getOrganizationId(), report.getOrganizationId())) {
            throw new SecurityException("Kafka metric query scope is not authorized");
        }
    }

    public List<Map<String, Object>> query(SingleGeneralReportDO report) {
        validate(report);
        return module.query(report);
    }

    @PreDestroy
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        stopMissingTasks(Set.of());
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(CONNECTION_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /** Applies connection changes on the cluster's worker so slow SDK calls cannot block database synchronization. */
    private final class CollectionTask implements Runnable {

        private volatile KafkaCollectConfig desiredConfig;
        private KafkaCollectConfig currentConfig;
        private KafkaMetricCollect collect;
        private ScheduledFuture<?> future;

        private CollectionTask(KafkaCollectConfig config) {
            desiredConfig = config;
        }

        @Override
        public synchronized void run() {
            try {
                KafkaCollectConfig desired = desiredConfig;
                if (!Objects.equals(desired, currentConfig)) {
                    closeCollector();
                    currentConfig = null;
                }
                if (desired == null) {
                    return;
                }
                if (collect == null) {
                    collect = module.createCollect(desired);
                    currentConfig = desired;
                }
                collect.request();
            } catch (RuntimeException e) {
                log.warn("Kafka collection task failed ({})", e.getClass().getSimpleName());
            } finally {
                if (desiredConfig == null) {
                    closeCollector();
                }
            }
        }

        private void closeCollector() {
            if (collect != null) {
                KafkaMetricCollect previous = collect;
                collect = null;
                previous.close();
            }
        }
    }
}
