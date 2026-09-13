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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class KafkaJmxDescriptor {

    private final String objectNamePattern;
    private final String attribute;
    private final List<String> dimensionKeys;

    public KafkaJmxDescriptor(String objectNamePattern, String attribute, String... dimensionKeys) {
        this(objectNamePattern, attribute, Arrays.asList(dimensionKeys));
    }

    public KafkaJmxDescriptor(String objectNamePattern, String attribute, List<String> dimensionKeys) {
        this.objectNamePattern = Objects.requireNonNull(objectNamePattern, "objectNamePattern");
        this.attribute = Objects.requireNonNull(attribute, "attribute");
        this.dimensionKeys = Collections.unmodifiableList(new ArrayList<>(dimensionKeys));
    }

    public String objectNamePattern() {
        return objectNamePattern;
    }

    public String attribute() {
        return attribute;
    }

    public List<String> dimensionKeys() {
        return dimensionKeys;
    }
}
