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
    /**
     * Services to start
     */
    SERVICES("services"),
    /** Multiplexed  UDP port**/
    UDP_PORT("port.udp"),
    /** Multiplexed  TCP port**/
    TCP_PORT("port.tcp"),
    /** Ganglia port **/
    GANGLIA_PORT("ganglia.port"),
    /** Ganglia group **/
    GANGLIA_GROUP("ganglia.group"),
    /** Explicit multicast interface **/
    GANGLIA_MULTICAST_INTERFACE("multicast.interface"),
    /** UDP port for statsd type of messages **/
    STATSD_PORT("statsd.port"),
    /** UDP port for collectd type of messages **/
    COLLECTD_PORT("collectd.port"),
    /** Minimum batch size of metrics to be forwarded (from one source) **/
    BATCH_SIZE("batch.size"),
    /** Maximum time (in seconds) a batch of metrics can stay unchanged before it is forwarded (from one source) **/
    BATCH_DELAY("batch.delay"),
    /** REST endpoint **/
    REST_URL("rest.url"),
    /** Close connection to rest-server after this many requests **/
    REST_CLOSE_AFTER_REQUESTS("rest.close-after"),
    /** Maximum number of metrics to spool if the server is not reachable **/
    SPOOL_SIZE("spool.size");

    private final String externalForm;

    ConfigurationKey(String externalForm) {
        this.externalForm = externalForm;
    }

    /**
     * @return string representation of this configuration key
     */
    public String getExternalForm() {
        return externalForm;
    }
}
