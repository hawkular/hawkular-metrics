/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.dropwizard;

import java.util.Optional;

import com.codahale.metrics.Gauge;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;

/**
 * This class is responsible for registering C* driver metrics with the {@link HawkularMetricRegistry metric registry}
 * so that they get persisted.
 *
 * @author jsanda
 */
public class CassandraDriverMetrics {

    private static final String SCOPE = "com.datastax.driver";

    private static final String LOAD_TYPE = "Load";

    private static final String ERROR_TYPE = "Error";

    private HawkularMetricRegistry metricsRegistry;

    private Session session;

    public CassandraDriverMetrics(Session session, HawkularMetricRegistry metricsRegistry) {
        this.session = session;
        this.metricsRegistry = metricsRegistry;
    }

    public void registerAll() {
        Metrics driverMetrics = session.getCluster().getMetrics();

        registerLoadMetrics(driverMetrics);
        registerErrorMetrics(driverMetrics);
        registerPerHostMetrics();
    }

    private void registerErrorMetrics(Metrics driverMetrics) {
        metricsRegistry.register("ConnectionErrors", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getConnectionErrors());
        metricsRegistry.register("AuthenticationErrors", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getAuthenticationErrors());
        metricsRegistry.register("WriteTimeouts", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getWriteTimeouts());
        metricsRegistry.register("ReadTimeouts", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getReadTimeouts());
        metricsRegistry.register("Unavailables", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getUnavailables());
        metricsRegistry.register("ClientTimeouts", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getClientTimeouts());
        metricsRegistry.register("OtherErrors", SCOPE, ERROR_TYPE, driverMetrics.getErrorMetrics().getOthers());
        metricsRegistry.register("Retries", SCOPE, ERROR_TYPE, driverMetrics.getErrorMetrics().getRetries());
        metricsRegistry.register("RetriesOnReadTimeout", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getRetriesOnReadTimeout());
        metricsRegistry.register("RetriesOnWriteTimeout", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getRetriesOnWriteTimeout());
        metricsRegistry.register("RetriesOnUnavailable", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getRetriesOnUnavailable());
        metricsRegistry.register("RetriesOnClientTimeout", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getRetriesOnReadTimeout());
        metricsRegistry.register("RetriesOnConnectionError", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getRetriesOnConnectionError());
        metricsRegistry.register("RetriesOnOtherErrors", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getRetriesOnOtherErrors());
        metricsRegistry.register("Ignores", SCOPE, ERROR_TYPE, driverMetrics.getErrorMetrics().getIgnores());
        metricsRegistry.register("IgnoresOnReadTimeout", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getIgnoresOnReadTimeout());
        metricsRegistry.register("IgnoresOnWriteTimeout", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getIgnoresOnWriteTimeout());
        metricsRegistry.register("IgnoresOnUnavailable", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getIgnoresOnUnavailable());
        metricsRegistry.register("IgnoresOnClientTimeout", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getIgnoresOnClientTimeout());
        metricsRegistry.register("IgnoresOnConnectionErrors", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getIgnoresOnConnectionError());
        metricsRegistry.register("IgnoresOnOtherErrors", SCOPE, ERROR_TYPE,
                driverMetrics.getErrorMetrics().getIgnoresOnOtherErrors());
    }

    private void registerLoadMetrics(Metrics driverMetrics) {
        metricsRegistry.register("Requests", SCOPE, LOAD_TYPE, driverMetrics.getRequestsTimer());
        metricsRegistry.register("KnownHosts", SCOPE, LOAD_TYPE, driverMetrics.getKnownHosts());
        metricsRegistry.register("ConnectedToHosts", SCOPE, LOAD_TYPE, driverMetrics.getConnectedToHosts());
        metricsRegistry.register("OpenConnections", SCOPE, LOAD_TYPE, driverMetrics.getOpenConnections());
        metricsRegistry.register("TrashedConnections", SCOPE, LOAD_TYPE, driverMetrics.getTrashedConnections());
        metricsRegistry.register("ExecutorQueueDepth", SCOPE, LOAD_TYPE, driverMetrics.getExecutorQueueDepth());
        metricsRegistry.register("BlockingExecutorQueueDepth", SCOPE, LOAD_TYPE,
                driverMetrics.getBlockingExecutorQueueDepth());
        metricsRegistry.register("ReconnectionSchedulerQueueSize", SCOPE, LOAD_TYPE,
                driverMetrics.getReconnectionSchedulerQueueSize());
        metricsRegistry.register("TaskSchedulerQueueSize", SCOPE, LOAD_TYPE,
                driverMetrics.getTaskSchedulerQueueSize());
    }

    private void registerPerHostMetrics() {
        session.getCluster().getMetadata().getAllHosts().forEach(this::registerPerHostMetrics);
        session.getCluster().register(new Host.StateListener() {
            @Override
            public void onAdd(Host host) {
                registerPerHostMetrics(host);
            }

            @Override
            public void onUp(Host host) {

            }

            @Override
            public void onDown(Host host) {

            }

            @Override
            public void onRemove(Host host) {
                String hostname = getHostKey(host);
                metricsRegistry.removeMatching((name, metric) -> name.startsWith(hostname));
            }

            @Override
            public void onRegister(Cluster cluster) {

            }

            @Override
            public void onUnregister(Cluster cluster) {

            }
        });
    }

    private void registerPerHostMetrics(Host host) {
        String hostname = getHostKey(host);
        metricsRegistry.register("OpenConnections_" + hostname, SCOPE, LOAD_TYPE, createOpenConnections(hostname));
        metricsRegistry.register("Load_" + hostname, SCOPE, LOAD_TYPE, createLoad(hostname));
        metricsRegistry.register("MaxLoad_" + hostname, SCOPE, LOAD_TYPE, createMaxLoad(hostname));
    }

    private String getHostKey(Host host) {
        return host.getSocketAddress().getHostString() + ":" + host.getSocketAddress().getPort();
    }

    private Optional<Host> getHost(Session.State state, String hostname) {
        return state.getConnectedHosts().stream().filter(h -> getHostKey(h).equals(hostname)).findFirst();
    }

    private Gauge<Integer> createOpenConnections(String hostname) {
        return () -> {
            Session.State state = session.getState();
            return getHost(state, hostname).map(state::getOpenConnections).orElse(0);
        };
    }

    private Gauge<Integer> createLoad(String hostname) {
        return () -> {
            Session.State state = session.getState();
            return getHost(state, hostname).map(state::getInFlightQueries).orElse(0);
        };
    }

    private Gauge<Integer> createMaxLoad(String hostname) {
        return () -> {
            Session.State state = session.getState();
            return getHost(state, hostname).map((host) -> {
                Configuration configuration = session.getCluster().getConfiguration();
                PoolingOptions poolingOptions = configuration.getPoolingOptions();
                HostDistance distance = configuration.getPolicies().getLoadBalancingPolicy().distance(host);
                int connections = state.getOpenConnections(host);
                return connections * poolingOptions.getMaxRequestsPerConnection(distance);
            }).orElse(0);
        };
    }

}
