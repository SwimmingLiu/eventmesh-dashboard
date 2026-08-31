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

import java.util.Objects;

public final class JmxConnectionConfig {

    private static final long DEFAULT_CONNECTION_TIMEOUT_MILLIS = 10_000L;

    private static final long DEFAULT_REQUEST_TIMEOUT_MILLIS = 10_000L;

    private final String host;

    private final int port;

    private final String username;

    private final String password;

    private final boolean ssl;

    private final long connectionTimeoutMillis;

    private final long requestTimeoutMillis;

    private JmxConnectionConfig(Builder builder) {
        this.host = Objects.requireNonNull(builder.host, "host");
        if (builder.host.trim().isEmpty()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (builder.port < 1 || builder.port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        if (builder.connectionTimeoutMillis < 1) {
            throw new IllegalArgumentException("connectionTimeoutMillis must be greater than zero");
        }
        if (builder.requestTimeoutMillis < 1) {
            throw new IllegalArgumentException("requestTimeoutMillis must be greater than zero");
        }
        this.port = builder.port;
        this.username = builder.username;
        this.password = builder.password;
        this.ssl = builder.ssl;
        this.connectionTimeoutMillis = builder.connectionTimeoutMillis;
        this.requestTimeoutMillis = builder.requestTimeoutMillis;
    }

    public static Builder builder(String host, int port) {
        return new Builder(host, port);
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public boolean ssl() {
        return ssl;
    }

    public long connectionTimeoutMillis() {
        return connectionTimeoutMillis;
    }

    public long requestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public String instance() {
        return host + ":" + port;
    }

    public static final class Builder {

        private final String host;

        private final int port;

        private String username;

        private String password;

        private boolean ssl;

        private long connectionTimeoutMillis = DEFAULT_CONNECTION_TIMEOUT_MILLIS;

        private long requestTimeoutMillis = DEFAULT_REQUEST_TIMEOUT_MILLIS;

        private Builder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public Builder credentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder ssl(boolean ssl) {
            this.ssl = ssl;
            return this;
        }

        public Builder connectionTimeoutMillis(long connectionTimeoutMillis) {
            this.connectionTimeoutMillis = connectionTimeoutMillis;
            return this;
        }

        public Builder requestTimeoutMillis(long requestTimeoutMillis) {
            this.requestTimeoutMillis = requestTimeoutMillis;
            return this;
        }

        public JmxConnectionConfig build() {
            if ((username == null) != (password == null)) {
                throw new IllegalArgumentException("username and password must be configured together");
            }
            return new JmxConnectionConfig(this);
        }
    }
}
