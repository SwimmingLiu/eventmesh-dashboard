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

package org.apache.eventmesh.dashboard.core.gather.kafka.collector;

import org.apache.eventmesh.dashboard.core.gather.jmx.JmxConnectionConfig;

import java.util.Objects;

import lombok.Value;
import lombok.experimental.Accessors;

/**
 * JMX connection information for one Kafka broker.
 */
@Value
@Accessors(fluent = true)
public class KafkaBrokerJmxEndpoint {

    int brokerId;

    JmxConnectionConfig connectionConfig;

    public KafkaBrokerJmxEndpoint(int brokerId, JmxConnectionConfig connectionConfig) {
        if (brokerId < 0) {
            throw new IllegalArgumentException("brokerId must not be negative");
        }
        this.brokerId = brokerId;
        this.connectionConfig = Objects.requireNonNull(connectionConfig, "connectionConfig");
    }

    public String broker() {
        return Integer.toString(brokerId);
    }
}
