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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_CQL_PORT;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_KEYSPACE;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_MAX_CONN_HOST;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_MAX_REQUEST_CONN;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_NODES;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_RESETDB;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_USESSL;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.DEFAULT_TTL;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.DISABLE_METRICS_JMX;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.WAIT_FOR_SERVICE;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.net.ssl.SSLContext;

import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;
import org.hawkular.metrics.api.jaxrs.log.RestLogger;
import org.hawkular.metrics.api.jaxrs.log.RestLogging;
import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.api.jaxrs.util.JobSchedulerFactory;
import org.hawkular.metrics.api.jaxrs.util.MetricRegistryProvider;
import org.hawkular.metrics.core.jobs.JobsService;
import org.hawkular.metrics.core.jobs.JobsServiceImpl;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.DataAccessImpl;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.impl.TestScheduler;
import org.hawkular.metrics.schema.SchemaService;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Bean created on startup to manage the lifecycle of the {@link MetricsService} instance shared in application scope.
 *
 * @author John Sanda
 * @author Thomas Segismont
 */
@ApplicationScoped
@Eager
public class MetricsServiceLifecycle {
    private static final RestLogger log = RestLogging.getRestLogger(MetricsServiceLifecycle.class);

    /**
     * @see #getState()
     */
    public enum State {
        STARTING, STARTED, STOPPING, STOPPED, FAILED
    }

    private MetricsServiceImpl metricsService;

    private final ScheduledExecutorService lifecycleExecutor;

    private Scheduler scheduler;

    private JobsServiceImpl jobsService;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_CQL_PORT)
    private String cqlPort;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_NODES)
    private String nodes;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_KEYSPACE)
    private String keyspace;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_RESETDB)
    private String resetDb;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_MAX_CONN_HOST)
    private String maxConnectionsPerHost;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_MAX_REQUEST_CONN)
    private String maxRequestsPerConnection;

    @Inject
    @Configurable
    @ConfigurationProperty(WAIT_FOR_SERVICE)
    private String waitForService;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_USESSL)
    private String cassandraUseSSL;

    @Inject
    @Configurable
    @ConfigurationProperty(DEFAULT_TTL)
    private String defaultTTL;

    @Inject
    @Configurable
    @ConfigurationProperty(DISABLE_METRICS_JMX)
    private String disableMetricsJmxReporting;

    @Inject
    @ServiceReady
    Event<ServiceReadyEvent> metricsServiceReady;

    private volatile State state;
    private int connectionAttempts;
    private Session session;
    private JmxReporter jmxReporter;

    private DataAccess dataAcces;

    MetricsServiceLifecycle() {
        ThreadFactory threadFactory = r -> {
            Thread thread = Executors.defaultThreadFactory().newThread(r);
            thread.setName(MetricsService.class.getSimpleName().toLowerCase(Locale.ROOT) + "-lifecycle-thread");
            return thread;
        };
        // All lifecycle operations will be executed on a single thread to avoid synchronization issues
        lifecycleExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        state = State.STARTING;
    }

    /**
     * Returns the lifecycle state of the {@link MetricsService} shared in application scope.
     *
     * @return lifecycle state of the shared {@link MetricsService}
     */
    public State getState() {
        return state;
    }

    @PostConstruct
    void init() {
        lifecycleExecutor.submit(this::startMetricsService);
        if (Boolean.parseBoolean(waitForService)
            // "hawkular-metrics.backend" is not a real Metrics configuration parameter (there's a single
            // MetricsService implementation, which is backed by Cassandra).
            // But it's been used historically to wait for the service to be available before completing the deployment.
            // Therefore, we still use it here for backward compatibililty.
            // TODO remove when Hawkular build has been updated to use the eager startup flag
            || "embedded_cassandra".equals(System.getProperty("hawkular.backend"))) {
            long start = System.nanoTime();
            while (state == State.STARTING
                   // Give up after a minute. The deployment won't be failed and we'll continue to try to start the
                   // service in the background.
                   && NANOSECONDS.convert(1, MINUTES) > System.nanoTime() - start) {
                Uninterruptibles.sleepUninterruptibly(1, SECONDS);
            }
        }
    }

    private void startMetricsService() {
        if (state != State.STARTING) {
            return;
        }
        log.infoInitializing();
        connectionAttempts++;
        try {
            session = createSession();
        } catch (Exception t) {
            Throwable rootCause = Throwables.getRootCause(t);

            // to get around HWKMETRICS-415
            if (rootCause.getLocalizedMessage().equals(this.nodes + ": unknown error")) {
                log.warnCouldNotConnectToCassandra("Could not resolve hostname: " + rootCause.getLocalizedMessage());
            } else {
                log.warnCouldNotConnectToCassandra(rootCause.getLocalizedMessage());
            }

            // cycle between original and more wait time - avoid waiting huge amounts of time
            long delay = 1L + ((connectionAttempts - 1L) % 4L);
            log.warnRetryingConnectingToCassandra(connectionAttempts, delay);
            lifecycleExecutor.schedule(this::startMetricsService, delay, SECONDS);
            return;
        }
        try {
            // When this class was first introduced, I said that the schema management
            // should stay in MetricsServiceImpl, and now I am putting it back here. OK, so
            // I deserve some criticism; however, I still think it should be done that way.
            // I made this change temporarily because the schema for metrics and for the
            // task scheduling service are declared and created in the same place. That
            // will change at some point though because the task scheduling service will
            // probably move to the hawkular-commons repo.
            initSchema();
            dataAcces = new DataAccessImpl(session);

            ConfigurationService configurationService = new ConfigurationService();
            configurationService.init(new RxSessionImpl(session));

            metricsService = new MetricsServiceImpl();
            metricsService.setDataAccess(dataAcces);
            metricsService.setConfigurationService(configurationService);
            metricsService.setDefaultTTL(getDefaultTTL());

            MetricRegistry metricRegistry = MetricRegistryProvider.INSTANCE.getMetricRegistry();
            if (!Boolean.parseBoolean(disableMetricsJmxReporting)) {
                jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("hawkular.metrics").build();
                jmxReporter.start();
            }
            metricsService.startUp(session, keyspace, false, false, metricRegistry);

            metricsServiceReady.fire(new ServiceReadyEvent(metricsService.insertedDataEvents()));

            initJobsService();

            Configuration configuration = session.getCluster().getConfiguration();
            LoadBalancingPolicy loadBalancingPolicy = configuration.getPolicies().getLoadBalancingPolicy();
            PoolingOptions poolingOptions = configuration.getPoolingOptions();
            lifecycleExecutor.scheduleAtFixedRate(() -> {
                if (log.isDebugEnabled()) {
                    Session.State state = session.getState();
                    for (Host host : state.getConnectedHosts()) {
                        HostDistance distance = loadBalancingPolicy.distance(host);
                        int connections = state.getOpenConnections(host);
                        int inFlightQueries = state.getInFlightQueries(host);
                        log.debugf("%s connections=%d, current load=%d, max load=%d%n", host, connections,
                                inFlightQueries, connections * poolingOptions.getMaxRequestsPerConnection(distance));
                    }
                }
            }, 5, 5, TimeUnit.SECONDS);

            state = State.STARTED;
            log.infoServiceStarted();

        } catch (Exception e) {
            log.fatalCannotConnectToCassandra(e);
            state = State.FAILED;
        } finally {
            if (state != State.STARTED && metricsService != null) {
                try {
                    metricsService.shutdown();
                } catch (Exception e) {
                    log.errorCouldNotCloseServiceInstance(e);
                }
            }
        }
    }

    private Session createSession() {
        Cluster.Builder clusterBuilder = new Cluster.Builder();
        int port;
        try {
            port = Integer.parseInt(cqlPort);
        } catch (NumberFormatException nfe) {
            String defaultPort = CASSANDRA_CQL_PORT.defaultValue();
            log.warnInvalidCqlPort(cqlPort, defaultPort);
            port = Integer.parseInt(defaultPort);
        }
        clusterBuilder.withPort(port);
        Arrays.stream(nodes.split(",")).forEach(clusterBuilder::addContactPoint);

        if (Boolean.parseBoolean(cassandraUseSSL)) {
            SSLOptions sslOptions = null;
            try {
                String[] defaultCipherSuites = { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" };
                sslOptions = JdkSSLOptions.builder().withSSLContext(SSLContext.getDefault())
                        .withCipherSuites(defaultCipherSuites).build();
                clusterBuilder.withSSL(sslOptions);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("SSL support is required but is not available in the JVM.", e);
            }
        }

        if (Boolean.parseBoolean(disableMetricsJmxReporting)) {
            clusterBuilder.withoutJMXReporting();
        }

        int newMaxConnections;
        try {
            newMaxConnections = Integer.parseInt(maxConnectionsPerHost);
        } catch (NumberFormatException nfe) {
            String defaultMaxConnections = CASSANDRA_MAX_CONN_HOST.defaultValue();
            log.warnInvalidMaxConnections(maxConnectionsPerHost, defaultMaxConnections);
            newMaxConnections = Integer.parseInt(defaultMaxConnections);
        }
        int newMaxRequests;
        try {
            newMaxRequests = Integer.parseInt(maxRequestsPerConnection);
        } catch (NumberFormatException nfe) {
            String defaultMaxRequests = CASSANDRA_MAX_REQUEST_CONN.defaultValue();
            log.warnInvalidMaxRequests(maxRequestsPerConnection, defaultMaxRequests);
            newMaxRequests = Integer.parseInt(defaultMaxRequests);
        }
        clusterBuilder.withPoolingOptions(new PoolingOptions()
                .setMaxConnectionsPerHost(HostDistance.LOCAL, newMaxConnections)
                .setMaxConnectionsPerHost(HostDistance.REMOTE, newMaxConnections)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, newMaxRequests)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, newMaxRequests)
        );

        Cluster cluster = clusterBuilder.build();
        cluster.init();
        Session createdSession = null;
        try {
            createdSession = cluster.connect("system");
            return createdSession;
        } finally {
            if (createdSession == null) {
                cluster.close();
            }
        }
    }

    private void initSchema() {
        SchemaService schemaService = new SchemaService();
        schemaService.run(session, keyspace, Boolean.parseBoolean(resetDb));
        session.execute("USE " + keyspace);
    }

    private int getDefaultTTL() {
        try {
            return Integer.parseInt(defaultTTL);
        } catch (NumberFormatException e) {
            log.warnInvalidDefaultTTL(defaultTTL, DEFAULT_TTL.defaultValue());
            return Integer.parseInt(DEFAULT_TTL.defaultValue());
        }
    }

    private void initJobsService() {
        RxSession rxSession = new RxSessionImpl(session);
        jobsService = new JobsServiceImpl();
        jobsService.setMetricsService(metricsService);
        jobsService.setSession(rxSession);
        scheduler = new JobSchedulerFactory().getJobScheduler(rxSession);
        jobsService.setScheduler(scheduler);
        jobsService.start();
    }

    /**
     * @return a {@link MetricsService} instance to share in application scope
     */
    @Produces
    @ApplicationScoped
    public MetricsService getMetricsService() {
        return metricsService;
    }

    @Produces
    @ApplicationScoped
    public JobsService getJobsService() {
        return jobsService;
    }

    @Produces
    @ApplicationScoped
    public TestScheduler getTestScheduler() {
        if (scheduler instanceof TestScheduler) {
            return (TestScheduler) scheduler;
        }
        throw new RuntimeException(TestScheduler.class.getName() + " is not available in this deployment");
    }

    @PreDestroy
    void destroy() {
        Future<?> stopFuture = lifecycleExecutor.submit(this::stopServices);
        try {
            Futures.get(stopFuture, 1, MINUTES, Exception.class);
        } catch (Exception e) {
            log.errorShutdownProblem(e);
        }
        lifecycleExecutor.shutdown();
    }

    private void stopServices() {
        state = State.STOPPING;
        try {
            // The order here is important. We need to shutdown jobsService first so that any running jobs can finish
            // gracefully.
            if (jobsService != null) {
                jobsService.shutdown();
            }

            if (metricsService != null) {
                metricsService.shutdown();
            }
            if (jmxReporter != null) {
                jmxReporter.stop();
            }
        } finally {
            state = State.STOPPED;
        }
    }
}
