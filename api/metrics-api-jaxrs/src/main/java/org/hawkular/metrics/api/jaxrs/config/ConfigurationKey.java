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
package org.hawkular.metrics.api.jaxrs.config;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Parameter and flags definitions.
 *
 * @author Thomas Segismont
 * @see Configurable
 */
public enum ConfigurationKey {

    CASSANDRA_NODES("hawkular-metrics.cassandra-nodes", "127.0.0.1", "CASSANDRA_NODES", false),
    CASSANDRA_CQL_PORT("hawkular-metrics.cassandra-cql-port", "9042", "CASSANDRA_CQL_PORT", false),
    CASSANDRA_KEYSPACE("cassandra.keyspace", "hawkular_metrics", null, false),
    CASSANDRA_RESETDB("cassandra.resetdb", null, null, true),
    WAIT_FOR_SERVICE("hawkular.metrics.waitForService", null, null, true),
    TASK_SCHEDULER_TIME_UNITS("hawkular.scheduler.time-units", "minutes", "SCHEDULER_TIME_UNITS", false);

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
