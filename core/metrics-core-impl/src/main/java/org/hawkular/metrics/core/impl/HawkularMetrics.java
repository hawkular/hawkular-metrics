/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.core.impl;

import java.util.HashMap;
import java.util.Map;

import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.impl.cassandra.MetricsServiceCassandra;

/**
 * @author John Sanda
 */
public class HawkularMetrics {

    public static class Builder {

        private final Map<String, String> options;

        public Builder() {
            String cassandraCqlPortString = System.getenv("CASSANDRA_CQL_PORT");
            if (cassandraCqlPortString == null) {
                cassandraCqlPortString = System.getProperty("hawkular-metrics.cassandra-cql-port", "9042");
            }

            String cassandraNodes = System.getenv("CASSANDRA_NODES");
            if (cassandraNodes == null) {
                cassandraNodes = System.getProperty("hawkular-metrics.cassandra-nodes", "127.0.0.1");
            }

            options = new HashMap<>();
            options.put("cqlport", cassandraCqlPortString);
            options.put("nodes", cassandraNodes);
            options.put("keyspace", System.getProperty("cassandra.keyspace", "hawkular_metrics"));
        }

        public Builder withOptions(Map<String,String> options) {
            this.options.putAll(options);
            return this;
        }

        public Builder withCQLPort(int port) {
            options.put("cqlport", Integer.toString(port));
            return this;
        }

        public Builder withKeyspace(String keyspace) {
            options.put("keyspace", keyspace);
            return this;
        }

        public Builder withNodes(String... nodes) {
            StringBuilder buffer = new StringBuilder();
            for (String node : nodes) {
                buffer.append(node).append(",");
            }
            if (buffer.length() > 0) {
                buffer.deleteCharAt(buffer.length() - 1);
            }
            options.put("nodes", buffer.toString());
            return this;
        }

        public MetricsService build() {
            MetricsService metricsService = new MetricsServiceCassandra();
            metricsService.startUp(options);

            return metricsService;
        }

    }
}
