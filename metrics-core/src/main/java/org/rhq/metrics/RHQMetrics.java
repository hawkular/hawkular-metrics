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

        private boolean usingCassandra;

        private Map<String, String> options;

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
            options.put("keyspace", "rhq_metrics");
        }

        public Builder withOptions(Map<String,String> options) {
            this.options.putAll(options);
            return this;
        }

        public Builder withInMemoryDataStore() {
            usingCassandra = false;
            return this;
        }

        public Builder withCassandraDataStore() {
            usingCassandra = true;
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

            if (usingCassandra) {
                metricsService = new MetricsServiceCassandra();
            } else {
                metricsService = new MemoryMetricsService();
            }
            metricsService.startUp(options);

            return metricsService;
        }

    }
}
