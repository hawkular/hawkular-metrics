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

package org.hawkular.metrics.api.jaxrs;

import static java.util.stream.Collectors.toSet;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;

import org.hawkular.metrics.api.jaxrs.util.MetricRegistryProvider;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;

/**
 * Create and compute driver usage metrics, for each connected host. Metric names follow this pattern:
 * <em>driver-usage-&lt;host&gt;_&lt;port&gt;-&lt;metric name&gt;</em>. For example:
 * <em>driver-usage-127.0.0.1_9042-connections</em>.
 *
 * @author Thomas Segismont
 */
@ApplicationScoped
public class DriverUsageMetricsManager {

    private enum DriverUsageMetric {
        /**
         * Number of connections.
         */
        CONNECTIONS("-connections") {
            @Override
            Gauge<?> createGauge(Session session, String hostname) {
                return new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        Session.State state = session.getState();
                        return getHost(state, hostname).map(state::getOpenConnections).orElse(0);
                    }
                };
            }
        },
        /**
         * Number of unfinished requests.
         */
        LOAD("-load") {
            @Override
            Gauge<?> createGauge(Session session, String hostname) {
                return new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        Session.State state = session.getState();
                        return getHost(state, hostname).map(state::getInFlightQueries).orElse(0);
                    }
                };
            }
        },
        /**
         * Maximum number of requests which can be executed concurrently.
         */
        LOAD_MAX("-load-max") {
            @Override
            Gauge<?> createGauge(Session session, String hostname) {
                return new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        Session.State state = session.getState();
                        return getHost(state, hostname).map((host) -> {
                            Configuration configuration = session.getCluster().getConfiguration();
                            PoolingOptions poolingOptions = configuration.getPoolingOptions();
                            HostDistance distance = configuration.getPolicies().getLoadBalancingPolicy().distance(host);
                            int connections = state.getOpenConnections(host);
                            return connections * poolingOptions.getMaxRequestsPerConnection(distance);
                        }).orElse(0);
                    }
                };
            }
        };

        static final String PREFIX = "driver-usage-";

        static String getHostKey(Host host) {
            return host.getSocketAddress().getHostString() + "_" + host.getSocketAddress().getPort();
        }

        static Optional<Host> getHost(Session.State state, String hostname) {
            return state.getConnectedHosts().stream().filter(h -> getHostKey(h).equals(hostname)).findFirst();
        }

        String suffix;

        DriverUsageMetric(String suffix) {
            this.suffix = suffix;
        }

        abstract Gauge<?> createGauge(Session session, String hostname);
    }

    /**
     * Must be called when cluster state changes (e.g. host goes down, host added, ...etc).
     */
    public void updateDriverUsageMetrics(Session session) {
        MetricRegistry metricRegistry = MetricRegistryProvider.INSTANCE.getMetricRegistry();
        Session.State state = session.getState();

        Set<String> expectedHosts = state.getConnectedHosts().stream()
                .map(DriverUsageMetric::getHostKey)
                .collect(toSet());
        Set<String> actualHosts = new HashSet<>();
        for (String name : metricRegistry.getGauges().keySet()) {
            if (name.startsWith(DriverUsageMetric.PREFIX)) {
                for (DriverUsageMetric driverUsageMetric : DriverUsageMetric.values()) {
                    if (name.endsWith(driverUsageMetric.suffix)) {
                        int beginIndex = DriverUsageMetric.PREFIX.length();
                        int endIndex = name.length() - driverUsageMetric.suffix.length();
                        actualHosts.add(name.substring(beginIndex, endIndex));
                    }
                }
            }
        }

        Set<String> toRemoveHosts = new HashSet<>(actualHosts);
        toRemoveHosts.removeAll(expectedHosts);
        toRemoveHosts.forEach(hostkey -> {
            for (DriverUsageMetric metric : DriverUsageMetric.values()) {
                metricRegistry.remove(DriverUsageMetric.PREFIX + hostkey + metric.suffix);
            }
        });

        Set<String> toCreateHosts = new HashSet<>(expectedHosts);
        toCreateHosts.removeAll(actualHosts);
        state.getConnectedHosts().stream()
                .map(DriverUsageMetric::getHostKey)
                .filter(toCreateHosts::contains)
                .forEach(hostkey -> {
                    for (DriverUsageMetric driverUsageMetric : DriverUsageMetric.values()) {
                        String metricName = DriverUsageMetric.PREFIX + hostkey + driverUsageMetric.suffix;
                        Gauge<?> gauge = driverUsageMetric.createGauge(session, hostkey);
                        metricRegistry.register(metricName, gauge);
                    }
                });
    }

    /**
     * Must be called when session is being closed.
     */
    public void removeDriverUsageMetrics() {
        MetricRegistry metricRegistry = MetricRegistryProvider.INSTANCE.getMetricRegistry();
        metricRegistry.removeMatching((name, metric) -> name.startsWith(DriverUsageMetric.PREFIX));
    }
}
