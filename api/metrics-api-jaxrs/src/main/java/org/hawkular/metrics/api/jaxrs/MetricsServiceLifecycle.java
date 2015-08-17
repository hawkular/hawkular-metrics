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
package org.hawkular.metrics.api.jaxrs;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_CQL_PORT;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_KEYSPACE;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_NODES;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_RESETDB;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_USESSL;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.USE_VIRTUAL_CLOCK;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.WAIT_FOR_SERVICE;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;
import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.api.jaxrs.util.VirtualClock;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.impl.CreateTenants;
import org.hawkular.metrics.core.impl.DataAccess;
import org.hawkular.metrics.core.impl.DataAccessImpl;
import org.hawkular.metrics.core.impl.DateTimeService;
import org.hawkular.metrics.core.impl.GenerateRate;
import org.hawkular.metrics.core.impl.MetricsServiceImpl;
import org.hawkular.metrics.schema.SchemaManager;
import org.hawkular.metrics.tasks.api.AbstractTrigger;
import org.hawkular.metrics.tasks.api.Task2;
import org.hawkular.metrics.tasks.api.TaskScheduler;
import org.hawkular.metrics.tasks.impl.Queries;
import org.hawkular.metrics.tasks.impl.TaskSchedulerImpl;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

import rx.Subscription;
import rx.functions.Action1;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

/**
 * Bean created on startup to manage the lifecycle of the {@link MetricsService} instance shared in application scope.
 *
 * @author John Sanda
 * @author Thomas Segismont
 */
@ApplicationScoped
@Eager
public class MetricsServiceLifecycle {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsServiceLifecycle.class);

    /**
     * @see #getState()
     */
    public enum State {
        STARTING, STARTED, STOPPING, STOPPED, FAILED
    }

    private MetricsServiceImpl metricsService;

    private TaskScheduler taskScheduler;

    private final ScheduledExecutorService lifecycleExecutor;

    private VirtualClock virtualClock;

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
    @ConfigurationProperty(WAIT_FOR_SERVICE)
    private String waitForService;

    @Inject
    @Configurable
    @ConfigurationProperty(USE_VIRTUAL_CLOCK)
    private String useVirtualClock;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_USESSL)
    private String cassandraUseSSL;

    private volatile State state;
    private int connectionAttempts;
    private Session session;

    private DataAccess dataAcces;

    private Map<? super Action1<Task2>, Subscription> jobs = new HashMap<>();

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
        LOG.info("Initializing metrics service");
        connectionAttempts++;
        try {
            session = createSession();
        } catch (Exception t) {
            Throwable rootCause = Throwables.getRootCause(t);
            LOG.warn("Could not connect to Cassandra cluster - assuming its not up yet: ",
                    rootCause.getLocalizedMessage());
            // cycle between original and more wait time - avoid waiting huge amounts of time
            long delay = 1L + ((connectionAttempts - 1L) % 4L);
            LOG.warn("[{}] Retrying connecting to Cassandra cluster in [{}]s...", connectionAttempts, delay);
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
            initTaskScheduler();

            metricsService = new MetricsServiceImpl();
            metricsService.setDataAccess(dataAcces);
            metricsService.setTaskScheduler(taskScheduler);
            metricsService.setDateTimeService(createDateTimeService());

            // TODO Set up a managed metric registry
            // We want a managed registry that can be shared by the JAX-RS endpoint and the core. Then we can expose
            // the registered metrics in various ways such as new REST endpoints, JMX, or via different
            // com.codahale.metrics.Reporter instances.
            metricsService.startUp(session, keyspace, false, false, new MetricRegistry());
            LOG.info("Metrics service started");

            initJobs();

            state = State.STARTED;
        } catch (Exception t) {
            LOG.error("An error occurred trying to connect to the Cassandra cluster", t);
            state = State.FAILED;
        } finally {
            if (state != State.STARTED) {
                try {
                    metricsService.shutdown();
                } catch (Exception ignore) {
                    LOG.error("Could not shutdown the metricsService instance: ", ignore);
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
            LOG.warn("Invalid CQL port '{}', not a number. Will use a default of {}", cqlPort, defaultPort);
            port = Integer.parseInt(defaultPort);
        }
        clusterBuilder.withPort(port);
        Arrays.stream(nodes.split(",")).forEach(clusterBuilder::addContactPoint);

        if (Boolean.parseBoolean(cassandraUseSSL)) {
            clusterBuilder.withSSL();
        }

        Cluster cluster = null;
        Session createdSession = null;
        try {
            cluster = clusterBuilder.build();
            createdSession = cluster.connect("system");
            return createdSession;
        } finally {
            if (createdSession == null && cluster != null) {
                cluster.close();
            }
        }
    }

    private void initSchema() {
        SchemaManager schemaManager = new SchemaManager(session);
        if (Boolean.parseBoolean(resetDb)) {
            schemaManager.dropKeyspace(keyspace);
        }
        schemaManager.createSchema(keyspace);
        session.execute("USE " + keyspace);
    }

    private void initTaskScheduler() {
        taskScheduler = new TaskSchedulerImpl(new RxSessionImpl(session), new Queries(session));
        if (Boolean.valueOf(useVirtualClock.toLowerCase())) {
            // We do not want to start the task scheduler when we are using the virtual
            // clock. Instead we want to wait to start it until a client sets the virtual
            // clock; otherwise, we will get a MissingBackpressureException.
            TestScheduler scheduler = Schedulers.test();
            scheduler.advanceTimeTo(System.currentTimeMillis(), MILLISECONDS);
            virtualClock = new VirtualClock(scheduler);
            AbstractTrigger.now = scheduler::now;
            ((TaskSchedulerImpl) taskScheduler).setTickScheduler(scheduler);
        } else {
            taskScheduler.start();
        }
    }

    private void initJobs() {
        GenerateRate generateRates = new GenerateRate(metricsService);
        CreateTenants createTenants = new CreateTenants(metricsService, dataAcces);

        jobs.put(generateRates, taskScheduler.getTasks().filter(task -> task.getName().equals(GenerateRate.TASK_NAME))
                .subscribe(generateRates));
        jobs.put(createTenants, taskScheduler.getTasks().filter(task -> task.getName().equals(CreateTenants.TASK_NAME))
                .subscribe(createTenants));
    }

    private DateTimeService createDateTimeService() {
        DateTimeService dateTimeService = new DateTimeService();
        if (Boolean.valueOf(useVirtualClock.toLowerCase())) {
            dateTimeService.now = () -> new DateTime(virtualClock.now());
        }
        return dateTimeService;
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
    public VirtualClock getVirtualClock() {
        return virtualClock;
    }

    @Produces
    @ApplicationScoped
    public TaskScheduler getTaskScheduler() {
        return taskScheduler;
    }

    @PreDestroy
    void destroy() {
        Future stopFuture = lifecycleExecutor.submit(this::stopMetricsService);
        try {
            Futures.get(stopFuture, 1, MINUTES, Exception.class);
        } catch (Exception ignore) {
            LOG.error("Unexcepted exception while shutting down, ", ignore);
        }
        lifecycleExecutor.shutdown();
    }

    private void stopMetricsService() {
        state = State.STOPPING;
        metricsService.shutdown();
        taskScheduler.shutdown();
        jobs.values().forEach(Subscription::unsubscribe);
        if (session != null) {
            try {
                session.close();
                session.getCluster().close();
            } finally {
                state = State.STOPPED;
            }
        }
    }
}
