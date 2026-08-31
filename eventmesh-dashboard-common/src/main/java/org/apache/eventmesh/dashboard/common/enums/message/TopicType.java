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

package org.apache.eventmesh.dashboard.common.enums.message;

/**
 *
 */
public enum TopicType {

    NORMAL,

    /**
     * 全局顺序消息
     */
    FIFO,

    /**
     * 分区有序
     */
    ORDERLY,

    /**
     * 延迟消息
     */
    DELAY,

    /**
     * 定时消息
     */
    TIMER,

    /**
     * 事务消息
     */
    TRANSACTION,

    /**
     * 轻量消息
     */
    LITE,
}
