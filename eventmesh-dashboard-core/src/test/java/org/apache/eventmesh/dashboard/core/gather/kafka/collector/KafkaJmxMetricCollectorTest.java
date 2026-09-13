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

import org.apache.eventmesh.dashboard.core.gather.jmx.JmxConnector;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.BrokerMetric;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.KafkaMetricCollection;
import org.apache.eventmesh.dashboard.core.gather.kafka.metrics.TopicMetric;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.junit.Assert;
import org.junit.Test;

public class KafkaJmxMetricCollectorTest {

    @Test
    public void testCollectTopicDimensions() throws Exception {
        MBeanServer server = MBeanServerFactory.createMBeanServer();
        register(server, "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=orders",
            Map.of("OneMinuteRate", 12.5D));
        register(server, "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=payments",
            Map.of("OneMinuteRate", 7.5D));

        KafkaMetricCollection result = new KafkaJmxMetricCollector().collect(JmxConnector.from(server), "broker-1:9999",
            List.of(TopicMetric.BYTES_IN));

        Assert.assertTrue(result.failures().isEmpty());
        Assert.assertEquals(2, result.topics().size());
        Assert.assertTrue(result.topics().stream().anyMatch(sample -> "orders".equals(sample.topic())
            && new BigDecimal("12.5").compareTo(sample.value()) == 0));
        Assert.assertTrue(result.brokers().isEmpty());
    }

    @Test
    public void testAggregateConnectionMetrics() throws Exception {
        MBeanServer server = MBeanServerFactory.createMBeanServer();
        register(server, "kafka.server:type=socket-server-metrics,listener=PLAINTEXT,networkProcessor=0",
            Map.of("connection-count", 2L));
        register(server, "kafka.server:type=socket-server-metrics,listener=PLAINTEXT,networkProcessor=1",
            Map.of("connection-count", 3L));

        KafkaMetricCollection result = new KafkaJmxMetricCollector().collect(JmxConnector.from(server), "broker-1:9999",
            List.of(BrokerMetric.CONNECTIONS_COUNT));

        Assert.assertTrue(result.failures().isEmpty());
        Assert.assertEquals(1, result.brokers().size());
        Assert.assertEquals(new BigDecimal("5"), result.brokers().get(0).value());
        Assert.assertEquals("broker-1:9999", result.brokers().get(0).broker());
        Assert.assertTrue(result.topics().isEmpty());
    }

    @Test
    public void testReportPartialFailure() throws Exception {
        MBeanServer server = MBeanServerFactory.createMBeanServer();
        register(server, "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", Map.of("OneMinuteRate", "bad"));

        KafkaMetricCollection result = new KafkaJmxMetricCollector().collect(JmxConnector.from(server), "broker-1:9999",
            List.of(BrokerMetric.BYTES_IN));

        Assert.assertEquals(0, result.sampleCount());
        Assert.assertEquals(1, result.failures().size());
        Assert.assertEquals("BytesIn", result.failures().get(0).metricName());
    }

    private void register(MBeanServer server, String objectName, Map<String, Object> attributes) throws Exception {
        server.registerMBean(new MetricMBean(attributes), new ObjectName(objectName));
    }

    private static final class MetricMBean implements DynamicMBean {

        private final Map<String, Object> attributes;

        private MetricMBean(Map<String, Object> attributes) {
            this.attributes = attributes;
        }

        @Override
        public Object getAttribute(String attribute) {
            return attributes.get(attribute);
        }

        @Override
        public void setAttribute(Attribute attribute) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AttributeList getAttributes(String[] attributeNames) {
            AttributeList result = new AttributeList();
            for (String attributeName : attributeNames) {
                result.add(new Attribute(attributeName, attributes.get(attributeName)));
            }
            return result;
        }

        @Override
        public AttributeList setAttributes(AttributeList attributes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object invoke(String actionName, Object[] params, String[] signature) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MBeanInfo getMBeanInfo() {
            MBeanAttributeInfo[] infos = attributes.entrySet().stream()
                .map(entry -> new MBeanAttributeInfo(entry.getKey(), entry.getValue().getClass().getName(), entry.getKey(), true, false, false))
                .toArray(MBeanAttributeInfo[]::new);
            return new MBeanInfo(getClass().getName(), getClass().getSimpleName(), infos, null, null, null);
        }
    }
}
