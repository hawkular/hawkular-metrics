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

import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.BATCH_SIZE;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.BUFFER_CAPACITY;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.COLLECTD_PORT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.GANGLIA_GROUP;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.GANGLIA_MULTICAST_INTERFACE;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.GANGLIA_PORT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.GRAPHITE_PORT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.HTTP_PROXY;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.REST_MAX_CONNECTIONS;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.REST_URL;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.STATSD_PORT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.TCP_PORT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.TENANT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.UDP_PORT;

import java.net.URI;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * PTrans configuration holder. An instance can be created with
 * {@link org.hawkular.metrics.clients.ptrans.Configuration#from(java.util.Properties)}. The caller should make sure
 * the configuration object is valid ({@link #isValid()}) and, in case it's not, can get indications of errors by
 * calling {@link #getValidationMessages()}.
 *
 * @author Thomas Segismont
 */
public class Configuration {
    private final Set<Service> services;
    private final int udpPort;
    private final int tcpPort;
    private final int gangliaPort;
    private final String gangliaGroup;
    private final int statsDport;
    private final int collectdPort;
    private final int graphitePort;
    private final String multicastIfOverride;
    private final URI restUrl;
    private final URI httpProxy;
    private final String tenant;
    private final int bufferCapacity;
    private final int batchSize;
    private final int restMaxConnections;
    private final Set<String> validationMessages;

    private Configuration(
            Set<Service> services,
            int udpPort,
            int tcpPort,
            int gangliaPort,
            String gangliaGroup,
            int statsDport,
            int collectdPort,
            int graphitePort,
            String multicastIfOverride,
            URI restUrl,
            URI httpProxy,
            String tenant,
            int bufferCapacity,
            int batchSize,
            int restMaxConnections,
            Set<String> validationMessages
    ) {
        this.services = services;
        this.udpPort = udpPort;
        this.tcpPort = tcpPort;
        this.gangliaPort = gangliaPort;
        this.statsDport = statsDport;
        this.collectdPort = collectdPort;
        this.gangliaGroup = gangliaGroup;
        this.graphitePort = graphitePort;
        this.multicastIfOverride = multicastIfOverride;
        this.restUrl = restUrl;
        this.httpProxy = httpProxy;
        this.tenant = tenant;
        this.bufferCapacity = bufferCapacity;
        this.batchSize = batchSize;
        this.restMaxConnections = restMaxConnections;
        this.validationMessages = Collections.unmodifiableSet(validationMessages);
    }

    public static Configuration from(Properties properties) {
        Set<String> validationMessages = new HashSet<>();
        Set<Service> services = getServices(properties, validationMessages);
        int udpPort = getIntProperty(properties, UDP_PORT, 5140);
        int tcpPort = getIntProperty(properties, TCP_PORT, 5140);
        int gangliaPort = getIntProperty(properties, GANGLIA_PORT, 8649);
        String gangliaGroup = properties.getProperty(GANGLIA_GROUP.toString(), "239.2.11.71");
        String multicastIfOverride = properties.getProperty(GANGLIA_MULTICAST_INTERFACE.toString());
        int statsDport = getIntProperty(properties, STATSD_PORT, 8125);
        int collectdPort = getIntProperty(properties, COLLECTD_PORT, 25826);
        int graphitePort = getIntProperty(properties, GRAPHITE_PORT, 2003);
        URI restUrl = URI.create(properties.getProperty(REST_URL.toString(),
                "http://localhost:8080/hawkular/metrics/gauges/data"));
        String proxyString = properties.getProperty(HTTP_PROXY.toString());
        URI httpProxy = null;
        if (proxyString != null && !proxyString.trim().isEmpty()) {
            httpProxy = URI.create(proxyString);
        }
        String tenant = properties.getProperty(TENANT.toString(), "default");
        int bufferCapacity = getIntProperty(properties, BUFFER_CAPACITY, 10000);
        int batchSize = getIntProperty(properties, BATCH_SIZE, 50);
        int restMaxConnections = getIntProperty(properties, REST_MAX_CONNECTIONS, 10);
        return new Configuration(
                services,
                udpPort,
                tcpPort,
                gangliaPort,
                gangliaGroup,
                statsDport,
                collectdPort,
                graphitePort,
                multicastIfOverride,
                restUrl,
                httpProxy,
                tenant,
                bufferCapacity,
                batchSize,
                restMaxConnections,
                validationMessages
        );
    }

    private static Set<Service> getServices(Properties properties, Set<String> validationMessages) {
        String servicesProperty = properties.getProperty(SERVICES.toString());
        if (servicesProperty == null) {
            validationMessages.add(String.format(Locale.ROOT, "Property %s not found", SERVICES.toString()));
            return Collections.emptySet();
        }
        Set<Service> services = EnumSet.noneOf(Service.class);
        StringTokenizer tokenizer = new StringTokenizer(servicesProperty, ",");
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken().trim();
            if (token.isEmpty()) {
                continue;
            }
            Service service = Service.findByExternalForm(token);
            if (service == null) {
                validationMessages.add(String.format(Locale.ROOT, "Unknown service %s", token));
                continue;
            }
            services.add(service);
        }
        if (services.isEmpty()) {
            validationMessages.add("Empty services list");
        }
        return services;
    }

    private static int getIntProperty(Properties properties, ConfigurationKey key, int defaultValue) {
        String property = properties.getProperty(key.toString());
        if (property == null) {
            return defaultValue;
        }
        return Integer.parseInt(property);
    }

    /**
     * @return true if this configuration is valid, false otherwise
     */
    public boolean isValid() {
        return validationMessages.isEmpty();
    }

    /**
     * @return a set of messages describing errors in configuration, empty if configuration is valid
     */
    public Set<String> getValidationMessages() {
        return validationMessages;
    }

    public Set<Service> getServices() {
        return services;
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

    public int getGraphitePort() {
        return graphitePort;
    }

    public String getGangliaGroup() {
        return gangliaGroup;
    }

    public String getMulticastIfOverride() {
        return multicastIfOverride;
    }

    public URI getRestUrl() {
        return restUrl;
    }

    public URI getHttpProxy() {
        return httpProxy;
    }

    public String getTenant() {
        return tenant;
    }

    public int getBufferCapacity() {
        return bufferCapacity;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getRestMaxConnections() {
        return restMaxConnections;
    }
}