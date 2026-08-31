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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.rmi.ssl.SslRMIClientSocketFactory;

public final class JmxConnector implements AutoCloseable {

    private static final String JMX_SERVICE_URL_FORMAT = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    private static final String REQUEST_WAITING_TIMEOUT = "jmx.remote.x.request.waiting.timeout";

    private static final String RMI_CLIENT_SOCKET_FACTORY = "com.sun.jndi.rmi.factory.socket";

    private static final int MAX_CONNECT_THREADS = 8;

    private static final ThreadPoolExecutor CONNECT_EXECUTOR = new ThreadPoolExecutor(0, MAX_CONNECT_THREADS,
        60L, TimeUnit.SECONDS, new SynchronousQueue<>(), daemonThreadFactory("eventmesh-jmx-connect"),
        new ThreadPoolExecutor.AbortPolicy());

    private static final ThreadPoolExecutor CLOSE_EXECUTOR = new ThreadPoolExecutor(0, 2,
        60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(32), daemonThreadFactory("eventmesh-jmx-close"),
        new ThreadPoolExecutor.CallerRunsPolicy());

    private static final Map<String, ConnectAttempt> CONNECT_ATTEMPTS = new ConcurrentHashMap<>();

    private final javax.management.remote.JMXConnector remoteConnector;

    private final MBeanServerConnection mbeanConnection;

    private JmxConnector(javax.management.remote.JMXConnector remoteConnector, MBeanServerConnection mbeanConnection) {
        this.remoteConnector = remoteConnector;
        this.mbeanConnection = Objects.requireNonNull(mbeanConnection, "mbeanConnection");
    }

    public static JmxConnector connect(JmxConnectionConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        final String url = String.format(JMX_SERVICE_URL_FORMAT, config.host(), config.port());
        Map<String, Object> environment = new HashMap<>();
        environment.put(REQUEST_WAITING_TIMEOUT, config.requestTimeoutMillis());
        if (config.username() != null) {
            environment.put(javax.management.remote.JMXConnector.CREDENTIALS,
                new String[] {config.username(), config.password()});
        }
        if (config.ssl()) {
            SslRMIClientSocketFactory socketFactory = new SslRMIClientSocketFactory();
            environment.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, socketFactory);
            environment.put(RMI_CLIENT_SOCKET_FACTORY, socketFactory);
        }
        javax.management.remote.JMXConnector connector = JMXConnectorFactory.newJMXConnector(
            new JMXServiceURL(url), environment);
        try {
            connectWithTimeout(connector, config.connectionTimeoutMillis(), url);
            return new JmxConnector(connector, connector.getMBeanServerConnection());
        } catch (AbandonedConnectionException ex) {
            throw ex;
        } catch (IOException | RuntimeException ex) {
            closeAsync(connector);
            throw ex;
        }
    }

    static void connectWithTimeout(javax.management.remote.JMXConnector connector, long timeoutMillis,
        String endpoint) throws IOException {
        ConnectAttempt connectAttempt = new ConnectAttempt();
        if (CONNECT_ATTEMPTS.putIfAbsent(endpoint, connectAttempt) != null) {
            throw new IOException("A previous JMX connection attempt is still running for " + endpoint);
        }
        Future<?> connectFuture;
        try {
            connectFuture = CONNECT_EXECUTOR.submit(() -> {
                try {
                    connector.connect();
                    return null;
                } finally {
                    if (connectAttempt.finish()) {
                        closeQuietly(connector);
                    }
                    releaseConnectAttempt(endpoint, connectAttempt);
                }
            });
        } catch (RejectedExecutionException e) {
            releaseConnectAttempt(endpoint, connectAttempt);
            throw new IOException("JMX connection executor is at capacity", e);
        }
        try {
            connectFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            if (connectAttempt.abandon()) {
                closeAsync(connector);
            }
            throw new AbandonedConnectionException("JMX connection timed out after " + timeoutMillis + " ms", ex);
        } catch (InterruptedException ex) {
            if (connectAttempt.abandon()) {
                closeAsync(connector);
            }
            Thread.currentThread().interrupt();
            throw new AbandonedConnectionException("Interrupted while connecting to JMX", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Unable to connect to JMX", cause);
        }
    }

    private static void releaseConnectAttempt(String endpoint, ConnectAttempt attempt) {
        CONNECT_ATTEMPTS.remove(endpoint, attempt);
    }

    private static void closeAsync(javax.management.remote.JMXConnector connector) {
        CLOSE_EXECUTOR.execute(() -> closeQuietly(connector));
    }

    private static ThreadFactory daemonThreadFactory(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, prefix + '-' + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static void closeQuietly(javax.management.remote.JMXConnector connector) {
        try {
            connector.close();
        } catch (IOException ignored) {
            // Preserve the original connection exception.
        }
    }

    public static JmxConnector from(MBeanServerConnection connection) {
        return new JmxConnector(null, connection);
    }

    static int pendingConnectionAttempts() {
        return CONNECT_ATTEMPTS.size();
    }

    static int pendingConnectionAttempts(String host, int port) {
        String endpoint = String.format(JMX_SERVICE_URL_FORMAT, host, port);
        return CONNECT_ATTEMPTS.containsKey(endpoint) ? 1 : 0;
    }

    static int connectThreadCount() {
        return CONNECT_EXECUTOR.getPoolSize();
    }

    public Set<ObjectName> queryNames(ObjectName pattern) throws IOException {
        return mbeanConnection.queryNames(pattern, null);
    }

    public Object getAttribute(ObjectName objectName, String attribute)
        throws InstanceNotFoundException, AttributeNotFoundException, ReflectionException, MBeanException, IOException {
        return mbeanConnection.getAttribute(objectName, attribute);
    }

    @Override
    public void close() throws IOException {
        if (remoteConnector != null) {
            closeAsync(remoteConnector);
        }
    }

    private static final class ConnectAttempt {

        private boolean abandoned;
        private boolean finished;
        private boolean cleanupClaimed;

        private synchronized boolean abandon() {
            abandoned = true;
            return claimCleanup();
        }

        private synchronized boolean finish() {
            finished = true;
            return claimCleanup();
        }

        private boolean claimCleanup() {
            if (abandoned && finished && !cleanupClaimed) {
                cleanupClaimed = true;
                return true;
            }
            return false;
        }
    }

    private static final class AbandonedConnectionException extends IOException {

        private AbandonedConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
