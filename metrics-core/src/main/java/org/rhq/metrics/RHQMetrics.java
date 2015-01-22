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
package org.rhq.metrics;

import java.util.HashMap;
import java.util.Map;

import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.impl.cassandra.MetricsServiceCassandra;
import org.rhq.metrics.impl.memory.MemoryMetricsService;

/**
 * @author John Sanda
 */
public class RHQMetrics {

    public static class Builder {

        private enum DataStoreType {
            Cassandra, EmbeddedCassandra, InMemory
        };

        private DataStoreType dataStoreType = DataStoreType.EmbeddedCassandra;

        private final Map<String, String> options;

        public Builder() {
            String cassandraCqlPortString = System.getenv("CASSANDRA_CQL_PORT");
            if (cassandraCqlPortString == null) {
                cassandraCqlPortString = System.getProperty("rhq-metrics.cassandra-cql-port", "9042");
            }

            String cassandraNodes = System.getenv("CASSANDRA_NODES");
            if (cassandraNodes == null) {
                cassandraNodes = System.getProperty("rhq-metrics.cassandra-nodes", "127.0.0.1");
            }

            options = new HashMap<>();
            options.put("cqlport", cassandraCqlPortString);
            options.put("nodes", cassandraNodes);
            options.put("keyspace", System.getProperty("cassandra.keyspace", "rhq_metrics"));
        }

        public Builder withOptions(Map<String,String> options) {
            this.options.putAll(options);
            return this;
        }

        public Builder withInMemoryDataStore() {
            dataStoreType = DataStoreType.InMemory;
            return this;
        }

        public Builder withEmbeddedCassandraDataStore() {
            dataStoreType = DataStoreType.EmbeddedCassandra;
            return this;
        }

        public Builder withCassandraDataStore() {
            dataStoreType = DataStoreType.Cassandra;
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
            MetricsService metricsService;

            if (DataStoreType.Cassandra.equals(dataStoreType)) {
                metricsService = new MetricsServiceCassandra();
            } else if (DataStoreType.InMemory.equals(dataStoreType)) {
                metricsService = new MemoryMetricsService();
            } else if (DataStoreType.EmbeddedCassandra.equals(dataStoreType)) {
                metricsService = new MetricsServiceCassandra(true);
            } else {
                metricsService = new MetricsServiceCassandra();
            }

            metricsService.startUp(options);

            return metricsService;
        }

    }

    private final MetricsService metricsService;

    private RHQMetrics(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

//    public ListenableFuture<Void> addData(NumericData data) {
//        return metricsService.addNumericData(ImmutableSet.of(data));
//    }

//    public ListenableFuture<Void> addData(Set<NumericData> data) {
//        return metricsService.addNumericData(data);
//    }

//    public ListenableFuture<List<NumericData>> findData(String id, long start, long end) {
//        return metricsService.findData(id, start, end);
//    }

    public void shutdown() {
        metricsService.shutdown();
    }

}
