/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.api.jaxrs.config;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Parameter and flags definitions.
 *
 * @author Thomas Segismont
 * @see Configurable
 */
public enum ConfigurationKey {

    //CORS
    ALLOWED_CORS_ORIGINS("hawkular.metrics.allowed-cors-origins", "*", "ALLOWED_CORS_ORIGINS", false),
    ALLOWED_CORS_ACCESS_CONTROL_ALLOW_HEADERS("hawkular.metrics.allowed-cors-access-control-allow-headers",
                    null, " ALLOWED_CORS_ACCESS_CONTROL_ALLOW_HEADERS", false),

    //Response cache-control header
    CACHE_CONTROL_HEADER("hawkular.metrics.cache-control-header", "no-cache", " CACHE_CONTROL_HEADER", false),

    //Cassandra
    CASSANDRA_NODES("hawkular.metrics.cassandra.nodes", "127.0.0.1", "CASSANDRA_NODES", false),
    CASSANDRA_CQL_PORT("hawkular.metrics.cassandra.cql-port", "9042", "CASSANDRA_CQL_PORT", false),
    CASSANDRA_KEYSPACE("hawkular.metrics.cassandra.keyspace", "hawkular_metrics", null, false),
    CASSANDRA_REPLICATION_FACTOR("hawkular.metrics.cassandra.replication-factor", "1", "CASSANDRA_REPLICATION_FACTOR",
            false),
    CASSANDRA_CLUSTER_CONNECTION_ATTEMPTS("hawkular.metrics.cassandra.cluster.connection-attempts", "5",
            "CASSANDRA_CLUSTER_CONNECTION_ATTEMPTS", false),
    CASSANDRA_CLUSTER_CONNECTION_MAX_DELAY("hawkular.metrics.cassandra.cluster.connection-delay", "30000",
            "CASSANDR_CLUSTER_CONNECTION_DELAY", false),
    CASSANDRA_RESETDB("hawkular.metrics.cassandra.resetdb", null, null, true),
    CASSANDRA_USESSL("hawkular.metrics.cassandra.use-ssl", "false", "CASSANDRA_USESSL", false),
    CASSANDRA_MAX_CONN_HOST("hawkular.metrics.cassandra.max-connections-per-host", "10", "CASSANDRA_MAX_CONN_HOST",
            false),
    CASSANDRA_MAX_REQUEST_CONN("hawkular.metrics.cassandra.max-requests-per-connection", "1024",
            "CASSANDRA_MAX_REQUEST_CONN", false),
    CASSANDRA_MAX_QUEUE_SIZE("hawkular.metrics.cassandra.max-queue-size", "256", "CASSANDRA_MAX_QUEUE_SIZE", false),
    CASSANDRA_REQUEST_TIMEOUT("hawkular.metrics.cassandra.request-timeout", "12000", "CASSANDRA_REQUEST_TIMEOUT",
            false),
    CASSANDRA_CONNECTION_TIMEOUT("hawkular.metrics.cassandra.connection-timeout", "5000",
            "CASSANDRA_CONNECTION_TIMEOUT", false),
    CASSANDRA_SCHEMA_REFRESH_INTERVAL("hawkular.metrics.cassandra.schema.refresh-interval", "1000",
            "CASSANDRA_SCHEMA_REFRESH_INTERVAL", false),
    PAGE_SIZE("hawkular.metrics.page-size", "1000", "PAGE_SIZE", false),
    COMPRESSION_QUERY_PAGE_SIZE("hawkular.metrics.compression.page-size", "1000", "COMPRESSION_PAGE_SIZE", false),
    COMPRESSION_JOB_ENABLED("hawkular.metrics.jobs.compression.enabled", null, "COMPRESSION_JOB_ENABLED", false),
    WAIT_FOR_SERVICE("hawkular.metrics.waitForService", null, null, true),
    DEFAULT_TTL("hawkular.metrics.default-ttl", "7", "DEFAULT_TTL", false),
    JMX_REPORTING_ENABLED("hawkular.metrics.jmx-reporting-enabled", null, "JMX_REPORTING_ENABLED", true),

    //Admin
    ADMIN_TOKEN("hawkular.metrics.admin-token", null, "ADMIN_TOKEN", false),
    ADMIN_TENANT("hawkular.metrics.admin-tenant", "admin", "ADMIN_TENANT", false),
    METRICS_REPORTING_HOSTNAME("hawkular.metrics.reporting.hostname", null, "METRICS_REPORTING_HOSTNAME", false),
    METRICS_REPORTING_ENABLED("hawkular.metrics.reporting.enabled", null, "METRICS_REPORTING_ENABLED", true),
    METRICS_REPORTING_COLLECTION_INTERVAL("hawkular.metrics.reporting.collection-interval", "300",
            "METRICS_REPORTING_COLLECTION_INTERVAL", false),

    // Request logging properties
    // Useful for debugging
    REQUEST_LOGGING_LEVEL("hawkular.metrics.request.logging.level", null, "REQUEST_LOGGING_LEVEL", false),
    REQUEST_LOGGING_LEVEL_WRITES("hawkular.metrics.request.logging.level.writes", null, "REQUEST_LOGGING_LEVEL_WRITES",
            false),

    INGEST_MAX_RETRIES("hawkular.metrics.ingestion.retry.max-retries", null, "INGEST_MAX_RETRIES", false),
    INGEST_MAX_RETRY_DELAY("hawkular.metrics.ingestion.retry.max-delay", null, "INGEST_MAX_RETRY_DELAY", false),

    //Alerting
    METRICS_PUBLISH_BUFFER_SIZE("hawkular.metrics.publish-buffer-size", "100", "METRICS_PUBLISH_BUFFER_SIZE", false),
    METRICS_PUBLISH_PERIOD("hawkular.metrics.publish-period", "2000", "METRICS_PUBLISH_PERIOD", false),
    DISABLE_METRICS_FORWARDING("hawkular.metrics.disable-metrics-forwarding", null, "DISABLE_METRICS_FORWARDING", true),
    DISABLE_PUBLISH_FILTERING("hawkular.metrics.disable-publish-filtering", null, "DISABLE_PUBLISH_FILTERING", true);

    private final String name;
    private final String env;
    private final String defaultValue;
    private final boolean flag;

    /**
     * @param name         string representation when set in file or as system property
     * @param defaultValue default value for parameters, null for flags
     * @param env          string representation when set in environment
     * @param flag         true if the value does not matter
     */
    ConfigurationKey(String name, String defaultValue, String env, boolean flag) {
        checkArgument(name != null, "name is null");
        checkArgument(!flag || defaultValue == null, "Config flags can't have a default value");
        this.name = name;
        this.env = env;
        this.defaultValue = defaultValue;
        this.flag = flag;
    }

    /**
     * @return name when set in file or as system property
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * @return the default value, or null if none, or the parameter is just a flag
     */
    public String defaultValue() {
        return defaultValue;
    }

    /**
     * @return name when set in environment, or null if env binding is not supported
     */
    public String toEnvString() {
        return env;
    }

    /**
     * @return true if the value does not matter
     */
    public boolean isFlag() {
        return flag;
    }
}
