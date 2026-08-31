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


package org.apache.eventmesh.dashboard.core.function.SDK;

import org.apache.eventmesh.dashboard.common.enums.ClusterSyncMetadataEnum;
import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.model.base.BaseSyncBase;
import org.apache.eventmesh.dashboard.common.model.metadata.RuntimeMetadata;
import org.apache.eventmesh.dashboard.common.util.ClasspathScanner;
import org.apache.eventmesh.dashboard.core.function.SDK.config.AbstractCreateSDKConfig;
import org.apache.eventmesh.dashboard.core.function.SDK.config.CreateSDKConfig;

import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;


/**
 * SDK manager is a singleton to manage all SDK clients, it is a facade to create, delete and get a client.
 */
public class SDKManage {

    /**
     * Initialise the SDKOperation object instance according to SDKTypeEnum.
     * <p>
     * key: SDKTypeEnum value: SDKOperation
     *
     * @see SDKTypeEnum
     * @see SDKOperation
     */
    private static final Map<ClusterType, Map<SDKTypeEnum, SDKMetadataWrapper>> CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP =
        new ConcurrentHashMap<>();
    private static final SDKManage INSTANCE = new SDKManage();

    // register all client create operation
    static {
        Set<Class<?>> interfaceSet = new HashSet<>();
        interfaceSet.add(SDKOperation.class);
        ClasspathScanner classpathScanner =
            ClasspathScanner.builder().base(SDKManage.class).subPath("/operation/**").interfaceSet(interfaceSet).build();
        try {
            List<Class<?>> classList = classpathScanner.getClazz();
            classList.forEach(SDKManage::createSDKMetadataWrapper);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * inner key is the unique key of a client, such as (ip + port) they are defined in CreateClientConfig
     * <p>
     * key: SDKTypeEnum value: A map collection is used with key being (ip+port) and value being client.
     *
     * @see CreateSDKConfig#getUniqueKey()
     */
    private final Map<String, ClientWrapper> clientMap = new ConcurrentHashMap<>();
    private final Map<String, ClientLock> clientLocks = new ConcurrentHashMap<>();
    private final Map<String, Map<Class<?>, AbstractClientInfo<Object>>> stringMapConcurrentHashMap = new ConcurrentHashMap<>();

    private SDKManage() {
    }

    @SuppressWarnings("unchecked")
    static void createSDKMetadataWrapper(Class<?> clazz) {
        SDKMetadata[] sdkMetadataArray = clazz.getAnnotationsByType(SDKMetadata.class);
        if (ArrayUtils.isEmpty(sdkMetadataArray)) {
            return;
        }

        try {
            SDKMetadata sdkMetadata = sdkMetadataArray[0];
            Class<?> multi = getCreateSDKConfigClass(clazz);
            for (ClusterType clusterType : sdkMetadata.clusterType()) {
                Map<SDKTypeEnum, SDKMetadataWrapper> map =
                    CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP.computeIfAbsent(clusterType, k -> new ConcurrentHashMap<>());
                SDKTypeEnum[] sdkTypeEnums = sdkMetadata.sdkTypeEnum();
                if (Objects.equals(sdkTypeEnums[0], SDKTypeEnum.ALL)) {
                    sdkTypeEnums = new SDKTypeEnum[] {SDKTypeEnum.ADMIN, SDKTypeEnum.PING, SDKTypeEnum.PRODUCER, SDKTypeEnum.CONSUMER};
                }
                for (SDKTypeEnum sdkTypeEnum : sdkTypeEnums) {
                    SDKMetadataWrapper sdkMetadataWrapper = new SDKMetadataWrapper();
                    sdkMetadataWrapper.sdkMetadata = sdkMetadata;
                    sdkMetadataWrapper.createSDKConfigClass = multi;
                    sdkMetadataWrapper.abstractSDKOperation = (AbstractSDKOperation<Object, CreateSDKConfig>) clazz.newInstance();
                    map.put(sdkTypeEnum, sdkMetadataWrapper);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Class<?> getCreateSDKConfigClass(Class<?> genericClass) {
        Type supercType = genericClass.getGenericSuperclass();
        if (supercType instanceof ParameterizedType type) {
            Type[] typeArguments = type.getActualTypeArguments();
            for (Type typeArgument : typeArguments) {
                Class<?> argument;
                if (typeArgument instanceof ParameterizedType parameterizedType) {
                    argument = (Class<?>) parameterizedType.getRawType();
                } else {
                    argument = (Class<?>) typeArgument;
                }
                for (; ; ) {
                    Class<?> superclass = argument.getSuperclass();
                    if (Objects.isNull(superclass)) {
                        break;
                    }
                    if (Objects.equals(superclass, AbstractCreateSDKConfig.class)) {
                        if (typeArgument instanceof Class<?>) {
                            return (Class<?>) typeArgument;
                        }
                    }
                    argument = superclass;
                }
            }
        }
        return null;
    }

    public static SDKManage getInstance() {
        return INSTANCE;
    }

    /**
     * cluster 模式下，有问题？
     *  TODO 去重。 如果去重
     *       1. 只识别 地址？
     *       2. 识别 整个 CreateSDKConfig
     */
    public <T> T createClient(SDKTypeEnum sdkTypeEnum, BaseSyncBase baseSyncBase,
        CreateSDKConfig config, ClusterType clusterType) {

        try {
            SDKMetadataWrapper sdkMetadataWrapper = sdkMetadataWrapper(clusterType, sdkTypeEnum);
            if (Objects.equals(sdkTypeEnum, SDKTypeEnum.PRODUCER) || Objects.equals(sdkTypeEnum, SDKTypeEnum.CONSUMER)) {
                return (T) sdkMetadataWrapper.abstractSDKOperation.createClient(config);
            }
            final String uniqueKey = baseSyncBase.getUnique();
            ClientLock clientLock = acquireClientLock(uniqueKey);
            try {
                Object object = sdkMetadataWrapper.abstractSDKOperation.createClient(config);
                ClientWrapper wrapper = new ClientWrapper();
                wrapper.setConfig(config);
                wrapper.setBaseSyncBase(baseSyncBase);
                wrapper.getClientMap().put(sdkTypeEnum, object);
                if (Objects.equals(SDKTypeEnum.ADMIN, sdkTypeEnum)) {
                    wrapper.getClientMap().put(SDKTypeEnum.PING, object);
                }
                ClientWrapper previous = clientMap.get(uniqueKey);
                if (previous != null) {
                    try {
                        closeAll(previous);
                    } catch (RuntimeException e) {
                        clientMap.remove(uniqueKey, previous);
                        try {
                            closeAll(wrapper);
                        } catch (RuntimeException closeException) {
                            e.addSuppressed(closeException);
                        }
                        throw e;
                    }
                }
                clientMap.put(uniqueKey, wrapper);
                return (T) object;
            } finally {
                releaseClientLock(uniqueKey, clientLock);
            }
        } catch (Exception e) {
            throw new RuntimeException("create client error", e);
        }
    }


    public void deleteClient(SDKTypeEnum sdkTypeEnum, String uniqueKey) {
        this.deleteClient(sdkTypeEnum, uniqueKey, null);
    }

    public void deleteClient(SDKTypeEnum sdkTypeEnum, String uniqueKey, Object expectedClient) {
        ClientLock clientLock = acquireClientLock(uniqueKey);
        try {
            ClientWrapper wrapper = this.clientMap.get(uniqueKey);
            if (wrapper == null) {
                return;
            }
            if (expectedClient != null && wrapper.getClientMap().values().stream()
                .noneMatch(client -> client == expectedClient)) {
                return;
            }
            if (Objects.isNull(sdkTypeEnum)) {
                this.clientMap.remove(uniqueKey);
                this.stringMapConcurrentHashMap.remove(uniqueKey);
                closeAll(wrapper);
                return;
            }
            Object client = wrapper.getClientMap().remove(sdkTypeEnum);
            if (client == null) {
                return;
            }
            wrapper.getClientMap().entrySet().removeIf(entry -> entry.getValue() == client);
            boolean empty = wrapper.getClientMap().isEmpty();
            try {
                close(wrapper, sdkTypeEnum, client);
            } finally {
                if (empty) {
                    this.clientMap.remove(uniqueKey, wrapper);
                    this.stringMapConcurrentHashMap.remove(uniqueKey);
                }
            }
        } finally {
            releaseClientLock(uniqueKey, clientLock);
        }
    }

    private ClientLock acquireClientLock(String uniqueKey) {
        ClientLock clientLock = clientLocks.compute(uniqueKey, (key, existing) -> {
            ClientLock result = existing == null ? new ClientLock() : existing;
            result.retain();
            return result;
        });
        clientLock.lock();
        return clientLock;
    }

    private void releaseClientLock(String uniqueKey, ClientLock clientLock) {
        clientLock.unlock();
        clientLocks.computeIfPresent(uniqueKey, (key, current) -> {
            if (current != clientLock) {
                return current;
            }
            int users = clientLock.release();
            return users == 0 && !clientMap.containsKey(uniqueKey) ? null : current;
        });
    }

    private SDKMetadataWrapper sdkMetadataWrapper(ClusterType clusterType, SDKTypeEnum sdkTypeEnum) {
        Map<SDKTypeEnum, SDKMetadataWrapper> operations = CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP.get(clusterType);
        if (operations == null || operations.get(sdkTypeEnum) == null) {
            throw new IllegalArgumentException(
                "No SDK operation registered for clusterType=" + clusterType + ", sdkType=" + sdkTypeEnum);
        }
        return operations.get(sdkTypeEnum);
    }

    private void closeAll(ClientWrapper wrapper) {
        Set<Object> closed = Collections.newSetFromMap(new IdentityHashMap<>());
        RuntimeException failure = null;
        for (Map.Entry<SDKTypeEnum, Object> entry : wrapper.getClientMap().entrySet()) {
            if (entry.getValue() == null || !closed.add(entry.getValue())) {
                continue;
            }
            try {
                close(wrapper, entry.getKey(), entry.getValue());
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        wrapper.getClientMap().clear();
        if (failure != null) {
            throw failure;
        }
    }

    @SuppressWarnings("unchecked")
    private void close(ClientWrapper wrapper, SDKTypeEnum sdkTypeEnum, Object client) {
        try {
            SDKMetadataWrapper metadata = sdkMetadataWrapper(wrapper.getBaseSyncBase().getClusterType(), sdkTypeEnum);
            metadata.abstractSDKOperation.close(client);
        } catch (Exception e) {
            throw new RuntimeException("close client error", e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getClient(SDKTypeEnum clientTypeEnum, String uniqueKey) {
        ClientLock clientLock = acquireClientLock(uniqueKey);
        try {
            ClientWrapper wrapper = clientMap.get(uniqueKey);
            return wrapper == null ? null : (T) wrapper.getClientMap().get(clientTypeEnum);
        } finally {
            releaseClientLock(uniqueKey, clientLock);
        }
    }

    public ClientWrapper getClientWrapper(String uniqueKey) {
        return clientMap.get(uniqueKey);
    }

    int managedClientLockCount() {
        return clientLocks.size();
    }

    /**
     * TODO
     *  此方法只提供 core 直接调用，如果是 console 调用，需要console 在封装一层。
     *  是否要 缓存 AbstractClientInfo 对象。但 cluster or runtime 卸载的时候需要删除.
     *  直接提供 console 的是否需要一个代理层
     */
    @SuppressWarnings("unchecked")
    public <T> T createAbstractClientInfo(Class<?> clazz, BaseSyncBase baseSyncBase) {
        try {
            String unique = baseSyncBase.getUnique();
            if (!baseSyncBase.isCluster() && ClusterSyncMetadataEnum.getClusterFramework(baseSyncBase.getClusterType()).isCAP()) {
                unique = ((RuntimeMetadata) baseSyncBase).clusterUnique();
            }
            Map<Class<?>, AbstractClientInfo<Object>> classMap =
                this.stringMapConcurrentHashMap.computeIfAbsent(unique, k -> new ConcurrentHashMap<>());
            if (classMap.containsKey(clazz)) {
                return (T) classMap.get(clazz);
            }

            AbstractClientInfo<Object> abstractClientInfo = (AbstractClientInfo<Object>) clazz.newInstance();
            abstractClientInfo.setClientWrapper(clientMap.get(unique));
            abstractClientInfo.setBaseSyncBase(baseSyncBase);
            classMap.put(clazz, abstractClientInfo);
            return (T) abstractClientInfo;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Class<?> getConfig(ClusterType clusterType, SDKTypeEnum sdkTypeEnum) {
        return CLUSTER_TYPE_MAP_CONCURRENT_HASH_MAP.get(clusterType).get(sdkTypeEnum).createSDKConfigClass;
    }


    static class SDKMetadataWrapper {

        private SDKMetadata sdkMetadata;

        private Class<?> createSDKConfigClass;

        private AbstractSDKOperation<Object, CreateSDKConfig> abstractSDKOperation;

    }

    private static final class ClientLock {

        private final ReentrantLock lock = new ReentrantLock();
        private final AtomicInteger users = new AtomicInteger();

        private void retain() {
            users.incrementAndGet();
        }

        private int release() {
            return users.decrementAndGet();
        }

        private void lock() {
            lock.lock();
        }

        private void unlock() {
            lock.unlock();
        }
    }
}
