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
package org.hawkular.metrics.clients.ptrans;

/**
 * PTrans configuration keys.
 *
 * @author Thomas Segismont
 */
public enum ConfigurationKey {
    /** Services to start. */
    SERVICES("services"),
    /** Multiplexed  UDP port. */
    UDP_PORT("port.udp"),
    /** Multiplexed  TCP port. */
    TCP_PORT("port.tcp"),
    /** Ganglia port. */
    GANGLIA_PORT("ganglia.port"),
    /** Ganglia group. */
    GANGLIA_GROUP("ganglia.group"),
    /** Explicit multicast interface. */
    GANGLIA_MULTICAST_INTERFACE("multicast.interface"),
    /** UDP port for statsd type of messages. */
    STATSD_PORT("statsd.port"),
    /** UDP port for collectd type of messages. */
    COLLECTD_PORT("collectd.port"),
    /** TCP port for graphite type of messages. */
    GRAPHITE_PORT("graphite.port"),
    /** REST endpoint. */
    REST_URL("rest.url"),
    /** If present the HTTP proxy to use. */
    HTTP_PROXY("http.proxy"),
    /** Tenant. */
    TENANT("tenant"),
    /** Capacity of the buffer where incoming metrics are queued before sending to the backend. */
    BUFFER_CAPACITY("buffer.capacity"),
    /** Size of the metric batches sent to the backend. */
    BATCH_SIZE("batch.size"),
    /** Maximum number of HTTP connections used to send metrics to the backend. */
    REST_MAX_CONNECTIONS("rest.max.connections");

    private final String externalForm;

    ConfigurationKey(String externalForm) {
        this.externalForm = externalForm;
    }

    /**
     * @return string representation of this configuration key
     */
    @Override
    public String toString() {
        return externalForm;
    }
}
