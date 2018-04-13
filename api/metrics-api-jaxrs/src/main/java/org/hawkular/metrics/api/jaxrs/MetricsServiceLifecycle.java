/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
import static java.util.stream.Collectors.toList;

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.ADMIN_TENANT;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.ADMIN_TOKEN;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_CLUSTER_CONNECTION_ATTEMPTS;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_CLUSTER_CONNECTION_MAX_DELAY;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_CONNECTION_TIMEOUT;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_CQL_PORT;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_KEYSPACE;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_MAX_CONN_HOST;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_MAX_QUEUE_SIZE;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_MAX_REQUEST_CONN;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_NODES;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_REPLICATION_FACTOR;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_REQUEST_TIMEOUT;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_RESETDB;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_SCHEMA_REFRESH_INTERVAL;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_USESSL;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.COMPRESSION_JOB_ENABLED;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.COMPRESSION_QUERY_PAGE_SIZE;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.DEFAULT_TTL;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.INGEST_MAX_RETRIES;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.INGEST_MAX_RETRY_DELAY;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.JMX_REPORTING_ENABLED;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.METRICS_REPORTING_COLLECTION_INTERVAL;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.METRICS_REPORTING_ENABLED;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.METRICS_REPORTING_HOSTNAME;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.PAGE_SIZE;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.WAIT_FOR_SERVICE;

import java.lang.management.ManagementFactory;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Event;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.net.ssl.SSLContext;

import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationKey;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;
import org.hawkular.metrics.api.jaxrs.dropwizard.RESTMetrics;
import org.hawkular.metrics.api.jaxrs.log.RestLogger;
import org.hawkular.metrics.api.jaxrs.log.RestLogging;
import org.hawkular.metrics.api.jaxrs.util.CassandraClusterNotUpException;
import org.hawkular.metrics.api.jaxrs.util.JobSchedulerFactory;
import org.hawkular.metrics.api.jaxrs.util.ManifestInformation;
import org.hawkular.metrics.api.jaxrs.util.MetricRegistryProvider;
import org.hawkular.metrics.core.dropwizard.CassandraDriverMetrics;
import org.hawkular.metrics.core.dropwizard.DropWizardReporter;
import org.hawkular.metrics.core.dropwizard.HawkularMetricRegistry;
import org.hawkular.metrics.core.dropwizard.HawkularMetricsRegistryListener;
import org.hawkular.metrics.core.dropwizard.HawkularObjectNameFactory;
import org.hawkular.metrics.core.dropwizard.MetricNameService;
import org.hawkular.metrics.core.jobs.CompressData;
import org.hawkular.metrics.core.jobs.JobsService;
import org.hawkular.metrics.core.jobs.JobsServiceImpl;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.DataAccessImpl;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.core.util.GCGraceSecondsManager;
import org.hawkular.metrics.model.CassandraStatus;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.impl.TestScheduler;
import org.hawkular.metrics.schema.SchemaService;
import org.hawkular.metrics.sysconfig.Configuration;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;

import com.codahale.metrics.JmxReporter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Bean created on startup to manage the lifecycle of the {@link MetricsService} instance shared in application scope.
 *
 * @author John Sanda
 * @author Thomas Segismont
 */
@ApplicationScoped
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
    @ConfigurationProperty(CASSANDRA_CLUSTER_CONNECTION_ATTEMPTS)
    private String clusterConnectionAttempts;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_CLUSTER_CONNECTION_MAX_DELAY)
    private String clusterConnectionDelay;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_REPLICATION_FACTOR)
    private String replicationFactorProp;

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
    @ConfigurationProperty(CASSANDRA_MAX_QUEUE_SIZE)
    private String maxQueueSize;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_REQUEST_TIMEOUT)
    private String requestTimeout;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_CONNECTION_TIMEOUT)
    private String connectionTimeout;

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
    @ConfigurationProperty(CASSANDRA_SCHEMA_REFRESH_INTERVAL)
    private String schemaRefreshInterval;

    @Inject
    @Configurable
    @ConfigurationProperty(DEFAULT_TTL)
    private String defaultTTL;

    @Inject
    @Configurable
    @ConfigurationProperty(JMX_REPORTING_ENABLED)
    private String jmxReportingEnabled;

    @Inject
    @Configurable
    @ConfigurationProperty(ADMIN_TOKEN)
    private String adminToken;

    @Inject
    @Configurable
    @ConfigurationProperty(ADMIN_TENANT)
    private String adminTenant;

    @Inject
    @Configurable
    @ConfigurationProperty(INGEST_MAX_RETRIES)
    private String ingestMaxRetries;

    @Inject
    @Configurable
    @ConfigurationProperty(INGEST_MAX_RETRY_DELAY)
    private String ingestMaxRetryDelay;

    @Inject
    @Configurable
    @ConfigurationProperty(PAGE_SIZE)
    private String pageSize;

    @Inject
    @Configurable
    @ConfigurationProperty(COMPRESSION_QUERY_PAGE_SIZE)
    private String compressionPageSize;

    @Inject
    @Configurable
    @ConfigurationProperty(COMPRESSION_JOB_ENABLED)
    private String compressionJobEnabled;

    @Inject
    @Configurable
    @ConfigurationProperty(METRICS_REPORTING_HOSTNAME)
    private String metricsReportingHostname;

    @Inject
    @Configurable
    @ConfigurationProperty(METRICS_REPORTING_ENABLED)
    private String metricsReportingEnabled;

    @Inject
    @Configurable
    @ConfigurationProperty(METRICS_REPORTING_COLLECTION_INTERVAL)
    private String collectionIntervalConfig;

    @Inject
    @ServiceReady
    Event<ServiceReadyEvent> metricsServiceReady;

    @Inject
    private ManifestInformation manifestInfo;

    @Resource(lookup = "java:jboss/infinispan/cache/hawkular-metrics/locks")
    private Cache<String, String> locksCache;


    @Inject
    RESTMetrics restMetrics;

    private volatile State state;
    private int connectionAttempts;
    private Session session;
    private JmxReporter jmxReporter;
    private ConfigurationService configurationService;
    private DataAccess dataAcces;
    private GCGraceSecondsManager gcGraceSecondsManager;

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

    public List<CassandraStatus> getCassandraStatus() {
        return session.getCluster().getMetadata().getAllHosts().stream()
                .map(host -> {
                    CassandraStatus status;
                    if (host.isUp()) {
                        status = new CassandraStatus(host.getAddress().getHostName(), "up");
                    } else {
                        status = new CassandraStatus(host.getAddress().getHostName(), "down");
                    }
                    return status;
                })
                .collect(toList());
    }

    void eagerInit(@Observes @Initialized(ApplicationScoped.class) Object event) {
        // init
    }

    @PostConstruct
    void init() {
        log.infof("Hawkular Metrics version: %s", manifestInfo.getAttributes().get("Implementation-Version"));

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
            waitForAllNodesToBeUp();

            initSchema();
            dataAcces = new DataAccessImpl(session);

            configurationService = new ConfigurationService();
            configurationService.init(new RxSessionImpl(session));

            persistAdminToken();
            updateIngestionConfiguration();
            updateCompressionJobConfiguration();

            metricsService = new MetricsServiceImpl();
            metricsService.setDataAccess(dataAcces);
            metricsService.setConfigurationService(configurationService);
            metricsService.setDefaultTTL(getDefaultTTL());

            MetricNameService metricNameService;
            if (metricsReportingHostname == null) {
                metricNameService = new MetricNameService(adminTenant);
            } else {
                metricNameService = new MetricNameService(metricsReportingHostname, adminTenant);
            }

            HawkularMetricRegistry metricRegistry = MetricRegistryProvider.INSTANCE.getMetricRegistry();
            metricRegistry.setMetricNameService(metricNameService);

            restMetrics.setMetricNameService(metricNameService);
            restMetrics.initMetrics();

            metricsService.setMetricNameService(metricNameService);
            metricsService.startUp(session, keyspace, false, false, metricRegistry);

            HawkularMetricsRegistryListener metricsRegistryListener = new HawkularMetricsRegistryListener();
            metricsRegistryListener.setMetricNameService(metricNameService);
            metricsRegistryListener.setMetricRegistry(metricRegistry);
            metricsRegistryListener.setMetricsService(metricsService);
            metricRegistry.addListener(metricsRegistryListener);

            new CassandraDriverMetrics(session, metricRegistry).registerAll();

            initJobsService();

            if (Boolean.valueOf(metricsReportingEnabled)) {
                DropWizardReporter reporter = new DropWizardReporter(metricRegistry, metricNameService, metricsService);
                int interval = Integer.getInteger(collectionIntervalConfig, 180);
                reporter.start(interval, SECONDS);
            }

            initGCGraceSecondsManager();

            if (Boolean.parseBoolean(jmxReportingEnabled)) {
                HawkularObjectNameFactory JMXObjNameFactory = new HawkularObjectNameFactory(metricRegistry);
                JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
                        .inDomain("org.hawkular.metrics")
                        .createsObjectNamesWith(JMXObjNameFactory)
                        .build();
                jmxReporter.start();
            }
            state = State.STARTED;
            log.infoServiceStarted();

        } catch (CassandraClusterNotUpException e) {
            log.fatal("It appears that some nodes in the Cassandra cluster are not up. Start up cannot proceed");
            state = State.FAILED;

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
                String[] defaultCipherSuites = {"TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"};
                sslOptions = JdkSSLOptions.builder().withSSLContext(SSLContext.getDefault())
                        .withCipherSuites(defaultCipherSuites).build();
                clusterBuilder.withSSL(sslOptions);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("SSL support is required but is not available in the JVM.", e);
            }
        }

        clusterBuilder.withoutJMXReporting();

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
        int driverRequestTimeout;
        try {
            driverRequestTimeout = Integer.parseInt(requestTimeout);
        } catch (NumberFormatException e) {
            driverRequestTimeout = Integer.parseInt(CASSANDRA_REQUEST_TIMEOUT.defaultValue());
            log.warnInvalidRequestTimeout(requestTimeout, CASSANDRA_REQUEST_TIMEOUT.defaultValue());
        }
        int driverConnectionTimeout;
        try {
            driverConnectionTimeout = Integer.parseInt(connectionTimeout);
        } catch (NumberFormatException e) {
            driverConnectionTimeout = Integer.parseInt(CASSANDRA_CONNECTION_TIMEOUT.defaultValue());
            log.warnInvalidConnectionTimeout(connectionTimeout, CASSANDRA_CONNECTION_TIMEOUT.defaultValue());
        }
        int driverSchemaRefreshInterval;
        try {
            driverSchemaRefreshInterval = Integer.parseInt(schemaRefreshInterval);
        } catch (NumberFormatException e) {
            driverSchemaRefreshInterval = Integer.parseInt(CASSANDRA_SCHEMA_REFRESH_INTERVAL.defaultValue());
            log.warnInvalidSchemaRefreshInterval(schemaRefreshInterval,
                    CASSANDRA_SCHEMA_REFRESH_INTERVAL.defaultValue());
        }
        int driverPageSize;
        try {
            if (pageSize == null) {
                driverPageSize = Integer.parseInt(PAGE_SIZE.defaultValue());
            } else {
                driverPageSize = Integer.parseInt(pageSize);
            }
        } catch (NumberFormatException e) {
            driverPageSize = Integer.parseInt(PAGE_SIZE.defaultValue());
        }
        int driverMaxQueueSize;
        try {
            driverMaxQueueSize = Integer.parseInt(maxQueueSize);
        } catch (NumberFormatException e) {
            log.warnf("Invalid value [%s] for Cassandra driver max queue size", maxQueueSize);
            driverMaxQueueSize = Integer.parseInt(CASSANDRA_MAX_QUEUE_SIZE.defaultValue());
        }
        clusterBuilder.withPoolingOptions(new PoolingOptions()
                .setMaxConnectionsPerHost(HostDistance.LOCAL, newMaxConnections)
                .setCoreConnectionsPerHost(HostDistance.LOCAL, newMaxConnections)
                .setMaxConnectionsPerHost(HostDistance.REMOTE, newMaxConnections)
                .setCoreConnectionsPerHost(HostDistance.REMOTE, newMaxConnections)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, newMaxRequests)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, newMaxRequests)
                .setMaxQueueSize(driverMaxQueueSize)
        ).withSocketOptions(new SocketOptions()
                .setReadTimeoutMillis(driverRequestTimeout)
                .setConnectTimeoutMillis(driverConnectionTimeout)
        ).withQueryOptions(new QueryOptions()
                .setFetchSize(driverPageSize)
                .setDefaultIdempotence(true)
                .setRefreshSchemaIntervalMillis(driverSchemaRefreshInterval)
        ).withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build(), false));

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

    private void waitForAllNodesToBeUp() throws CassandraClusterNotUpException {
        boolean isReady = false;
        int attempts = Integer.parseInt(CASSANDRA_CLUSTER_CONNECTION_ATTEMPTS.defaultValue());
        long delay = 2000;
        long maxDelay = Long.parseLong(CASSANDRA_CLUSTER_CONNECTION_MAX_DELAY.defaultValue());
        try {
            attempts = Integer.parseInt(clusterConnectionAttempts);
        } catch (NumberFormatException e) {
            log.infof("Invalid value for %s. Using default of %d", CASSANDRA_CLUSTER_CONNECTION_ATTEMPTS.name(),
                    attempts);
        }
        try {
            maxDelay = Long.parseLong(clusterConnectionDelay);
        } catch (NumberFormatException e) {
            log.infof("Invalid value for %s. Using default of %d", CASSANDRA_CLUSTER_CONNECTION_MAX_DELAY.name(),
                    delay);
        }

        while (!isReady && !Thread.currentThread().isInterrupted() && attempts-- >= 0) {
            isReady = true;
            for (Host host : session.getCluster().getMetadata().getAllHosts()) {
                if (!host.isUp()) {
                    isReady = false;
                    log.warnf("Cassandra node %s may not be up yet. Waiting %s ms for node to come up", host, delay);
                    try {
                        Thread.sleep(delay);
                        delay = Math.min(delay * 2, maxDelay);
                    } catch(InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    break;
                }
            }
        }
        if (!isReady) {
            throw new CassandraClusterNotUpException("It appears that not all nodes in the Cassandra cluster are up " +
                    "after " + attempts + " checks. Schema updates cannot proceed without all nodes being up.");
        }
    }

    private void initSchema() {
        AtomicReference<Integer> replicationFactor = new AtomicReference<>();
        try {
            replicationFactor.set(Integer.parseInt(replicationFactorProp));
        } catch (NumberFormatException e) {
            log.warnf("Invalid value [%s] for Cassandra replication_factor. Using default value of 1",
                    replicationFactorProp);
            replicationFactor.set(1);
        }

        AdvancedCache<String, String> cache = locksCache.getAdvancedCache();
        DistributedLock schemaLock = new DistributedLock(cache, "cassalog");
        schemaLock.lockAndThen(30000, () -> {
            SchemaService schemaService = new SchemaService();
            schemaService.run(session, keyspace, Boolean.parseBoolean(resetDb), replicationFactor.get());
            session.execute("USE " + keyspace);
            logReplicationFactor();
        });
    }

    private void logReplicationFactor() {
        ResultSet resultSet = session.execute(
                "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = '" + keyspace + "'");
        if (resultSet.isExhausted()) {
            log.warnf("Unable to determine replication_factor for keyspace %s", keyspace);
        }
        Map<String, String> replication = resultSet.one().getMap(0, String.class, String.class);
        log.infof("The keyspace %s is using a replication_factor of %s", keyspace,
                replication.get("replication_factor"));
    }

    /**
     * This should be called after the schema is initialized and all updates are done.
     */
    private void initGCGraceSecondsManager() {
        gcGraceSecondsManager = new GCGraceSecondsManager(new RxSessionImpl(session), keyspace, configurationService);
        gcGraceSecondsManager.maybeUpdateGCGraceSeconds();
    }

    private int getDefaultTTL() {
        try {
            return Integer.parseInt(defaultTTL);
        } catch (NumberFormatException e) {
            log.warnInvalidDefaultTTL(defaultTTL, DEFAULT_TTL.defaultValue());
            return Integer.parseInt(DEFAULT_TTL.defaultValue());
        }
    }

    private int parseIntConfig(String value, ConfigurationKey configKey) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return Integer.parseInt(configKey.defaultValue());
        }
    }

    private boolean parseBooleanConfig(String value, ConfigurationKey configKey) {
        try {
            return Boolean.parseBoolean(value);
        } catch (NumberFormatException e) {
            return Boolean.parseBoolean(configKey.defaultValue());
        }
    }

    private void initJobsService() {

        RxSession rxSession = new RxSessionImpl(session);
        jobsService = new JobsServiceImpl();
        jobsService.setMetricsService(metricsService);
        jobsService.setConfigurationService(configurationService);
        jobsService.setSession(rxSession);
        scheduler = new JobSchedulerFactory().getJobScheduler(rxSession);
        jobsService.setScheduler(scheduler);

        DistributedLock jobsLock = new DistributedLock(locksCache.getAdvancedCache(), "background-jobs");
        jobsLock.lockAndThen(jobsService::start);

        registerMBean("JobsService", jobsService);
    }

    private void registerMBean(String name, Object service) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            String fullName = String.format("%s:type=%s", service.getClass().getPackage().getName(), name);
            ObjectName serviceName = new ObjectName(fullName);
            mbs.registerMBean(service, serviceName);
        } catch (MalformedObjectNameException | MBeanRegistrationException | NotCompliantMBeanException
                | InstanceAlreadyExistsException e) {
            log.error("Could not initialize JMX MBean", e);
        }
    }

    private void persistAdminToken() {
        if (adminToken != null && !adminToken.trim().isEmpty()) {
            String hashedAdminToken = Hashing.sha256().newHasher().putString(adminToken, Charsets.UTF_8).hash()
                    .toString();
            configurationService.save("org.hawkular.metrics", "admin.token", hashedAdminToken);
        }
    }

    private void updateIngestionConfiguration() {
        Map<String, String> properties = new HashMap<>();
        if (ingestMaxRetries != null) {
            try {
                Integer.parseInt(ingestMaxRetries);
                properties.put("ingestion.retry.max-retries", ingestMaxRetries);
            } catch (NumberFormatException e) {
                log.warnInvalidIngestMaxRetries(ingestMaxRetries);
            }
        }
        if (ingestMaxRetryDelay != null) {
            try {
                Long.parseLong(ingestMaxRetryDelay);
                properties.put("ingestion.retry.max-delay", ingestMaxRetryDelay);
            } catch (NumberFormatException e) {
                log.warnInvalidIngestMaxRetryDelay(ingestMaxRetryDelay);
            }
        }
        if (!properties.isEmpty()) {
            Configuration config = new Configuration("org.hawkular.metrics", properties);
            configurationService.save(config).toCompletable().await(10, SECONDS);
        }
    }

    private void updateCompressionJobConfiguration() {
        if (compressionPageSize != null) {
            configurationService.save(CompressData.CONFIG_ID, "page-size", compressionPageSize)
                    .toCompletable()
                    .await(10, SECONDS);
        } else {
            String pageSizeConfig = configurationService.load(CompressData.CONFIG_ID, "page-size")
                    .toBlocking().firstOrDefault(null);
            if (pageSizeConfig == null) {
                configurationService.save(CompressData.CONFIG_ID, "page-size",
                        COMPRESSION_QUERY_PAGE_SIZE.defaultValue())
                        .toCompletable()
                        .await(10, SECONDS);
            }
        }

        if (compressionJobEnabled != null) {
            configurationService.save(CompressData.CONFIG_ID, "enabled", compressionJobEnabled);
        }
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
    public ConfigurationService getConfigurationService() {
        return configurationService;
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
