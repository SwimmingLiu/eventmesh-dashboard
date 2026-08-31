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

package org.apache.eventmesh.dashboard.core.gather.jmx;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class JmxConnectorTest {

    @Test
    public void testConnectionTimeoutAppliesToUnresponsiveRegistry() throws Exception {
        try (ServerSocket registry = new ServerSocket(0)) {
            Thread acceptor = new Thread(() -> acceptWithoutReply(registry), "unresponsive-rmi-registry");
            acceptor.setDaemon(true);
            acceptor.start();

            JmxConnectionConfig config = JmxConnectionConfig.builder("127.0.0.1", registry.getLocalPort())
                .connectionTimeoutMillis(100L)
                .build();
            long startNanos = System.nanoTime();
            try {
                JmxConnector.connect(config);
                Assert.fail("Expected the unresponsive registry connection to time out");
            } catch (IOException ex) {
                Assert.assertTrue(ex.getMessage(), ex.getMessage().contains("timed out"));
            }
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            Assert.assertTrue("Connection timeout took " + elapsedMillis + " ms", elapsedMillis < 2_000L);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectNonPositiveConnectionTimeout() {
        JmxConnectionConfig.builder("127.0.0.1", 9999).connectionTimeoutMillis(0L).build();
    }

    private void acceptWithoutReply(ServerSocket registry) {
        try (Socket ignored = registry.accept()) {
            Thread.sleep(5_000L);
        } catch (IOException ignored) {
            // The test closes the server socket after the timeout assertion.
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
