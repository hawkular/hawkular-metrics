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
package org.rhq.metrics.clients.ptrans;

import static org.rhq.metrics.clients.ptrans.ConfigurationKey.BATCH_SIZE;
import static org.rhq.metrics.clients.ptrans.ConfigurationKey.COLLECTD_PORT;
import static org.rhq.metrics.clients.ptrans.ConfigurationKey.GANGLIA_GROUP;
import static org.rhq.metrics.clients.ptrans.ConfigurationKey.GANGLIA_MULTICAST_INTERFACE;
import static org.rhq.metrics.clients.ptrans.ConfigurationKey.GANGLIA_PORT;
import static org.rhq.metrics.clients.ptrans.ConfigurationKey.REST_CLOSE_AFTER_REQUESTS;
import static org.rhq.metrics.clients.ptrans.ConfigurationKey.REST_URL;
import static org.rhq.metrics.clients.ptrans.ConfigurationKey.SPOOL_SIZE;
import static org.rhq.metrics.clients.ptrans.ConfigurationKey.STATSD_PORT;
import static org.rhq.metrics.clients.ptrans.ConfigurationKey.TCP_PORT;
import static org.rhq.metrics.clients.ptrans.ConfigurationKey.UDP_PORT;

import java.net.URI;
import java.util.Properties;

/**
 * PTrans configuration holder.
 *
 * @author Thomas Segismont
 */
public class Configuration {
    private final int udpPort;
    private final int tcpPort;
    private final int gangliaPort;
    private final String gangliaGroup;
    private final int statsDport;
    private final int collectdPort;
    private final String multicastIfOverride;
    private final int minimumBatchSize;
    private final URI restUrl;
    private final int restCloseAfterRequests;
    private final int spoolSize;

    private Configuration(int udpPort, int tcpPort, int gangliaPort, String gangliaGroup, int statsDport,
        int collectdPort, String multicastIfOverride, int minimumBatchSize, URI restUrl, int restCloseAfterRequests,
        int spoolSize) {
        this.udpPort = udpPort;
        this.tcpPort = tcpPort;
        this.gangliaPort = gangliaPort;
        this.statsDport = statsDport;
        this.collectdPort = collectdPort;
        this.gangliaGroup = gangliaGroup;
        this.multicastIfOverride = multicastIfOverride;
        this.minimumBatchSize = minimumBatchSize;
        this.restUrl = restUrl;
        this.restCloseAfterRequests = restCloseAfterRequests;
        this.spoolSize = spoolSize;
    }

    public static Configuration from(Properties properties) {
        int udpPort = getIntProperty(properties, UDP_PORT, 5140);
        int tcpPort = getIntProperty(properties, TCP_PORT, 5140);
        int gangliaPort = getIntProperty(properties, GANGLIA_PORT, 8649);
        String gangliaGroup = properties.getProperty(GANGLIA_GROUP.getExternalForm(), "239.2.11.71");
        String multicastIfOverride = properties.getProperty(GANGLIA_MULTICAST_INTERFACE.getExternalForm());
        int statsDport = getIntProperty(properties, STATSD_PORT, 8125);
        int collectdPort = getIntProperty(properties, COLLECTD_PORT, 25826);
        int minimumBatchSize = getIntProperty(properties, BATCH_SIZE, 5);
        URI restUrl = URI.create(properties.getProperty(REST_URL.getExternalForm(),
            "http://localhost:8080/rhq-metrics/metrics"));
        int restCloseAfterRequests = getIntProperty(properties, REST_CLOSE_AFTER_REQUESTS, 200);
        int spoolSize = getIntProperty(properties, SPOOL_SIZE, 10000);
        return new Configuration(udpPort, tcpPort, gangliaPort, gangliaGroup, statsDport, collectdPort,
            multicastIfOverride, minimumBatchSize, restUrl, restCloseAfterRequests, spoolSize);
    }

    private static int getIntProperty(Properties properties, ConfigurationKey key, int defaultValue) {
        String property = properties.getProperty(key.getExternalForm());
        if (property == null) {
            return defaultValue;
        }
        return Integer.parseInt(property);
    }

    public int getUdpPort() {
        return udpPort;
    }

    public int getTcpPort() {
        return tcpPort;
    }

    public int getGangliaPort() {
        return gangliaPort;
    }

    public int getStatsDport() {
        return statsDport;
    }

    public int getCollectdPort() {
        return collectdPort;
    }

    public String getGangliaGroup() {
        return gangliaGroup;
    }

    public String getMulticastIfOverride() {
        return multicastIfOverride;
    }

    public int getMinimumBatchSize() {
        return minimumBatchSize;
    }

    public URI getRestUrl() {
        return restUrl;
    }

    public int getRestCloseAfterRequests() {
        return restCloseAfterRequests;
    }

    public int getSpoolSize() {
        return spoolSize;
    }
}