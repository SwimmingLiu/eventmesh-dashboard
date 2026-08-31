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

package org.apache.eventmesh.dashboard.core.gather.kafka.metrics;

import java.util.Objects;

public class KafkaMetricFailure {

    private String metricName;
    private String objectName;
    private String reason;

    public KafkaMetricFailure() {
    }

    public KafkaMetricFailure(String metricName, String objectName, String reason) {
        this.metricName = Objects.requireNonNull(metricName, "metricName");
        this.objectName = Objects.requireNonNull(objectName, "objectName");
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public String metricName() {
        return metricName;
    }

    public String objectName() {
        return objectName;
    }

    public String reason() {
        return reason;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }
}
