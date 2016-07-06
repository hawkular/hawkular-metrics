/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
    /**
     * Services to start.
     */
    SERVICES("services"),
    /**
     * Multiplexed  UDP port.
     */
    SERVICES_UDP_PORT("services.port.udp"),
    /**
     * Multiplexed  TCP port.
     */
    SERVICES_TCP_PORT("services.port.tcp"),
    /**
     * Ganglia port.
     */
    SERVICES_GANGLIA_PORT("services.ganglia.port"),
    /**
     * Ganglia group.
     */
    SERVICES_GANGLIA_GROUP("services.ganglia.group"),
    /**
     * Explicit multicast interface.
     */
    SERVICES_GANGLIA_MULTICAST_INTERFACE("services.multicast.interface"),
    /**
     * UDP port for statsd type of messages.
     */
    SERVICES_STATSD_PORT("services.statsd.port"),
    /**
     * UDP port for collectd type of messages.
     */
    SERVICES_COLLECTD_PORT("services.collectd.port"),
    /**
     * TCP port for graphite type of messages.
     */
    SERVICES_GRAPHITE_PORT("services.graphite.port"),
    /**
     * Metrics service url.
     */
    METRICS_URL("metrics.url"),
    /**
     * Tenant Header switch. Older versions of Hawkular reject requests having a tenant header.
     */
    METRICS_TENANT_SEND("metrics.tenant.send"),
    /**
     * Tenant selection. Ignored if {@link #METRICS_TENANT_SEND} property is set to false.
     */
    METRICS_TENANT("metrics.tenant"),
    /**
     * Authentication switch.
     */
    METRICS_AUTH_ENABLED("metrics.auth.enabled"),
    /**
     * Authentication id.
     */
    METRICS_AUTH_ID("metrics.auth.id"),
    /**
     * Authentication secret.
     */
    METRICS_AUTH_SECRET("metrics.auth.secret"),
    /**
     * Maximum number of HTTP connections used to send metrics to the backend.
     */
    METRICS_MAX_CONNECTIONS("metrics.max-connections"),
    /**
     * Size of the metric batches sent to the backend.
     */
    METRICS_BATCH_SIZE("metrics.batch-size");

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
