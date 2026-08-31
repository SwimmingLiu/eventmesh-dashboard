/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eventmesh.dashboard.console.spring.support;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.enums.DeployStatusType;
import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.RuntimeEntity;
import org.apache.eventmesh.dashboard.console.service.cluster.ClusterRelationshipService;
import org.apache.eventmesh.dashboard.console.service.cluster.ClusterService;
import org.apache.eventmesh.dashboard.console.service.cluster.RuntimeService;
import org.apache.eventmesh.dashboard.console.service.connector.ResourcesConfigService;
import org.apache.eventmesh.dashboard.console.service.deploy.DeployScriptService;
import org.apache.eventmesh.dashboard.console.service.deploy.PortService;
import org.apache.eventmesh.dashboard.console.service.function.ConfigService;
import org.apache.eventmesh.dashboard.console.spring.support.RuntimeDeployService.AbstractRuntimeServiceTask;
import org.apache.eventmesh.dashboard.core.function.SDK.SDKManage;

import org.apache.commons.lang3.reflect.FieldUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import io.fabric8.kubernetes.client.KubernetesClient;

@RunWith(MockitoJUnitRunner.class)
public class RuntimeDeployServiceTest {


    private final RuntimeDeployService runtimeDeployService = new RuntimeDeployService();


    @Mock
    private ClusterService clusterService;

    @Mock
    private RuntimeService runtimeService;

    @Mock
    private ClusterRelationshipService clusterRelationshipService;

    @Mock
    private DeployScriptService deployScriptService;

    @Mock
    private ResourcesConfigService resourcesConfigService;

    @Mock
    private ConfigService configService;


    private final RuntimeEntity runtimeEntity = new RuntimeEntity();

    private MockedStatic<SDKManage> sdkManageMockedStatic;

    @Mock
    private SDKManage sdkManageMock;

    @Mock
    private KubernetesClient kubernetesClient;

    @Mock
    private PortService portService;


    @Before
    public void init() throws IllegalAccessException {
        runtimeEntity.setClusterId(10L);
        runtimeEntity.setId(10000L);

        FieldUtils.writeField(runtimeDeployService, "clusterService", clusterService, true);
        FieldUtils.writeField(runtimeDeployService, "runtimeService", runtimeService, true);
        FieldUtils.writeField(runtimeDeployService, "clusterRelationshipService", clusterRelationshipService, true);
        FieldUtils.writeField(runtimeDeployService, "deployScriptService", deployScriptService, true);
        FieldUtils.writeField(runtimeDeployService, "resourcesConfigService", resourcesConfigService, true);
        FieldUtils.writeField(runtimeDeployService, "configService", configService, true);
        FieldUtils.writeField(runtimeDeployService, "portService", portService, true);

        sdkManageMockedStatic = Mockito.mockStatic(SDKManage.class);
        sdkManageMockedStatic.when(SDKManage::getInstance).thenReturn(sdkManageMock);

        Mockito.when(sdkManageMock.getClient(Mockito.any(), Mockito.any())).thenReturn(kubernetesClient);


    }

    @After
    public void closeStaticMock() {
        if (sdkManageMockedStatic != null) {
            sdkManageMockedStatic.close();
        }
    }


    @Test
    public void test_CREATE_WAIT() throws IllegalAccessException {
        ClusterEntity clusterEntity = new ClusterEntity();
        clusterEntity.setId(1L);
        Mockito.when(clusterService.queryClusterById(Mockito.any())).thenReturn(clusterEntity);

        ClusterEntity kubeClusterEntity = new ClusterEntity();
        kubeClusterEntity.setId(2L);
        Mockito.when(clusterService.queryRelationshipClusterByClusterIdAndType(Mockito.any())).thenReturn(kubeClusterEntity);

        runtimeEntity.setClusterType(ClusterType.EVENTMESH_RUNTIME);
        runtimeEntity.setDeployStatusType(DeployStatusType.CREATE_WAIT);
        AbstractRuntimeServiceTask abstractRuntimeServiceTask = runtimeDeployService.createTask(runtimeEntity);
        FieldUtils.writeField(abstractRuntimeServiceTask, "runtimeEntity", runtimeEntity, true);
        abstractRuntimeServiceTask.run();
    }

    @Test
    public void test_CREATE_UNINSTALL() throws IllegalAccessException {
        ClusterEntity clusterEntity = new ClusterEntity();
        clusterEntity.setId(1L);
        Mockito.when(clusterService.queryClusterById(Mockito.any())).thenReturn(clusterEntity);

        ClusterEntity kubeClusterEntity = new ClusterEntity();
        kubeClusterEntity.setId(2L);
        Mockito.when(clusterService.queryRelationshipClusterByClusterIdAndType(Mockito.any())).thenReturn(kubeClusterEntity);

        runtimeEntity.setClusterType(ClusterType.EVENTMESH_RUNTIME);
        runtimeEntity.setDeployStatusType(DeployStatusType.UNINSTALL);
        AbstractRuntimeServiceTask abstractRuntimeServiceTask = runtimeDeployService.createTask(runtimeEntity);
        FieldUtils.writeField(abstractRuntimeServiceTask, "runtimeEntity", runtimeEntity, true);
        abstractRuntimeServiceTask.run();
    }
}
