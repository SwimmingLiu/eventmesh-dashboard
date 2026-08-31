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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
                Assertions.fail("Expected the unresponsive registry connection to time out");
            } catch (IOException ex) {
                Assertions.assertTrue(ex.getMessage().contains("timed out"), ex.getMessage());
            }
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            Assertions.assertTrue(elapsedMillis < 2_000L,
                "Connection timeout took " + elapsedMillis + " ms");
        }
    }

    @Test
    public void testRejectNonPositiveConnectionTimeout() {
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> JmxConnectionConfig.builder("127.0.0.1", 9999).connectionTimeoutMillis(0L).build());
    }

    @Test
    public void testRepeatedTimeoutsKeepConnectionResourcesBounded() throws Exception {
        try (ServerSocket registry = new ServerSocket(0)) {
            Thread acceptor = new Thread(() -> acceptWithoutReply(registry), "unresponsive-rmi-registry-bounded");
            acceptor.setDaemon(true);
            acceptor.start();
            JmxConnectionConfig config = JmxConnectionConfig.builder("127.0.0.1", registry.getLocalPort())
                .connectionTimeoutMillis(50L)
                .build();

            for (int index = 0; index < 20; index++) {
                try {
                    JmxConnector.connect(config);
                    Assertions.fail("Expected the unresponsive registry connection to fail");
                } catch (IOException expected) {
                    // The first attempt times out; later attempts may observe that it is still running.
                }
            }

            Assertions.assertTrue(JmxConnector.pendingConnectionAttempts("127.0.0.1", registry.getLocalPort()) <= 1);
            Assertions.assertTrue(JmxConnector.pendingConnectionAttempts() <= 8);
            Assertions.assertTrue(JmxConnector.connectThreadCount() <= 8);
        }
    }

    @Test
    public void testConnectorThatFinishesAfterTimeoutIsEventuallyClosed() throws Exception {
        javax.management.remote.JMXConnector connector = Mockito.mock(javax.management.remote.JMXConnector.class);
        CountDownLatch closed = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(150L);
            while (System.nanoTime() < deadline) {
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException ignored) {
                    // Simulate an RMI connect call that does not honor cancellation.
                }
            }
            return null;
        }).when(connector).connect();
        Mockito.doAnswer(invocation -> {
            closed.countDown();
            return null;
        }).when(connector).close();

        Assertions.assertThrows(IOException.class,
            () -> JmxConnector.connectWithTimeout(connector, 20L, "test:eventual-close"));

        Assertions.assertTrue(closed.await(1L, TimeUnit.SECONDS));
        Mockito.verify(connector).close();

        javax.management.remote.JMXConnector replacement = Mockito.mock(javax.management.remote.JMXConnector.class);
        JmxConnector.connectWithTimeout(replacement, 100L, "test:eventual-close");
        Mockito.verify(replacement).connect();
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
