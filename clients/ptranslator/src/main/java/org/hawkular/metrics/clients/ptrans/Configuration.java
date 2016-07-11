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

import static java.util.stream.Collectors.toList;

import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.METRICS_AUTH_ENABLED;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.METRICS_AUTH_ID;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.METRICS_AUTH_SECRET;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.METRICS_BATCH_SIZE;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.METRICS_MAX_CONNECTIONS;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.METRICS_TENANT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.METRICS_TENANT_SEND;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.METRICS_URL;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES_COLLECTD_PORT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES_GANGLIA_GROUP;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES_GANGLIA_MULTICAST_INTERFACE;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES_GANGLIA_PORT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES_GRAPHITE_PORT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES_STATSD_PORT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES_TCP_PORT;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES_UDP_PORT;

import java.net.URI;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

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
    private final String multicastIfOverride;
    private final int statsDport;
    private final int collectdPort;
    private final int graphitePort;
    private final URI metricsUrl;
    private final boolean sendTenant;
    private final String tenant;
    private final boolean authEnabled;
    private final String authId;
    private final String authSecret;
    private final JsonObject httpHeaders;
    private final int maxConnections;
    private final int batchSize;
    private final Set<String> validationMessages;

    private Configuration(Set<Service> services, int udpPort, int tcpPort, int gangliaPort, String gangliaGroup,
                          String multicastIfOverride, int statsDport, int collectdPort, int graphitePort,
                          URI metricsUrl, boolean sendTenant, String tenant, boolean authEnabled, String authId,
                          String authSecret, JsonObject httpHeaders, int maxConnections, int batchSize,
                          Set<String> validationMessages) {
        this.services = services;
        this.udpPort = udpPort;
        this.tcpPort = tcpPort;
        this.gangliaPort = gangliaPort;
        this.gangliaGroup = gangliaGroup;
        this.multicastIfOverride = multicastIfOverride;
        this.statsDport = statsDport;
        this.collectdPort = collectdPort;
        this.graphitePort = graphitePort;
        this.metricsUrl = metricsUrl;
        this.sendTenant = sendTenant;
        this.tenant = tenant;
        this.authEnabled = authEnabled;
        this.authId = authId;
        this.authSecret = authSecret;
        this.httpHeaders = httpHeaders;
        this.maxConnections = maxConnections;
        this.batchSize = batchSize;
        this.validationMessages = Collections.unmodifiableSet(validationMessages);
    }

    public static Configuration from(Properties properties) {
        Set<String> validationMessages = new HashSet<>();
        Set<Service> services = getServices(properties, validationMessages);
        int udpPort = getIntProperty(properties, SERVICES_UDP_PORT, 5140);
        int tcpPort = getIntProperty(properties, SERVICES_TCP_PORT, 5140);
        int gangliaPort = getIntProperty(properties, SERVICES_GANGLIA_PORT, 8649);
        String gangliaGroup = properties.getProperty(SERVICES_GANGLIA_GROUP.toString(), "239.2.11.71");
        String multicastIfOverride = properties.getProperty(SERVICES_GANGLIA_MULTICAST_INTERFACE.toString());
        int statsDport = getIntProperty(properties, SERVICES_STATSD_PORT, 8125);
        int collectdPort = getIntProperty(properties, SERVICES_COLLECTD_PORT, 25826);
        int graphitePort = getIntProperty(properties, SERVICES_GRAPHITE_PORT, 2003);
        URI metricsUrl = URI.create(properties.getProperty(METRICS_URL.toString(),
                "http://localhost:8080/hawkular/metrics/gauges/raw"));
        boolean sendTenant = getBooleanProperty(properties, METRICS_TENANT_SEND.toString(), true);
        String tenant = properties.getProperty(METRICS_TENANT.toString(), "default");
        boolean authEnabled = getBooleanProperty(properties, METRICS_AUTH_ENABLED.toString(), false);
        String authId = properties.getProperty(METRICS_AUTH_ID.toString());
        String authSecret = properties.getProperty(METRICS_AUTH_SECRET.toString());
        JsonObject httpHeaders = getHttpHeaders(properties);
        int maxConnections = getIntProperty(properties, METRICS_MAX_CONNECTIONS, 10);
        int batchSize = getIntProperty(properties, METRICS_BATCH_SIZE, 50);
        return new Configuration(
                services,
                udpPort,
                tcpPort,
                gangliaPort,
                gangliaGroup,
                multicastIfOverride,
                statsDport,
                collectdPort,
                graphitePort,
                metricsUrl,
                sendTenant,
                tenant,
                authEnabled,
                authId,
                authSecret,
                httpHeaders,
                maxConnections,
                batchSize,
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

    private static boolean getBooleanProperty(Properties properties, String name, boolean defaultValue) {
        String property = properties.getProperty(name);
        if (property == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(property);
    }

    private static JsonObject getHttpHeaders(Properties properties) {
        JsonObject jsonObject = new JsonObject();
        Set<Object> keys = properties.keySet();
        String namePrefix = "metrics.http.header.";
        String nameSuffix = ".name";
        Set<String> headerIds = keys.stream().filter(k -> k instanceof String).map(String.class::cast)
                .filter(name -> name.startsWith(namePrefix) && name.endsWith(nameSuffix))
                .map(name -> name.substring(namePrefix.length(), name.length() - nameSuffix.length()))
                .collect(Collectors.toSet());
        headerIds.forEach(headerId -> {
            String headerName = properties.getProperty(namePrefix + headerId + nameSuffix);
            String valuePrefix = namePrefix + headerId + ".value.";
            List<Integer> headerPriorities = keys.stream().filter(k -> k instanceof String).map(String.class::cast)
                    .filter(name -> name.startsWith(valuePrefix))
                    .map(name -> name.substring(valuePrefix.length()))
                    .filter(orderStr -> orderStr.matches("\\d{1,2}"))
                    .map(Integer::valueOf)
                    .sorted()
                    .collect(toList());
            if (!headerPriorities.isEmpty()) {
                if (headerPriorities.size() == 1) {
                    String headerValue = properties.getProperty(valuePrefix + headerPriorities.iterator().next());
                    jsonObject.put(headerName, headerValue);
                } else {
                    JsonArray headerValues = new JsonArray();
                    headerPriorities.forEach(headerPriority -> {
                        String headerValue = properties.getProperty(valuePrefix + headerPriority);
                        headerValues.add(headerValue);
                    });
                    jsonObject.put(headerName, headerValues);
                }
            }
        });
        return jsonObject;
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

    public URI getMetricsUrl() {
        return metricsUrl;
    }

    public boolean isSendTenant() {
        return sendTenant;
    }

    public String getTenant() {
        return tenant;
    }

    public boolean isAuthEnabled() {
        return authEnabled;
    }

    public String getAuthId() {
        return authId;
    }

    public String getAuthSecret() {
        return authSecret;
    }

    public JsonObject getHttpHeaders() {
        return httpHeaders;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public int getBatchSize() {
        return batchSize;
    }
}