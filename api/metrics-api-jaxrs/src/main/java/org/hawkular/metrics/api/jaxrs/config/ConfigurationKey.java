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

/**
 * Application configuration keys.
 *
 * @author Thomas Segismont
 * @see Configurable
 */
public enum ConfigurationKey {

    CASSANDRA_CQL_PORT("hawkular-metrics.cassandra-cql-port"),
    CASSANDRA_NODES("hawkular-metrics.cassandra-nodes"),
    CASSANDRA_KEYSPACE("cassandra.keyspace");

    private String externalForm;

    ConfigurationKey(String externalForm) {
        this.externalForm = externalForm;
    }

    public String getExternalForm() {
        return externalForm;
    }
}
