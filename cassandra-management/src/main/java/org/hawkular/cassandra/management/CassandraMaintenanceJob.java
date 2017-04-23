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
package org.hawkular.cassandra.management;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hawkular.cassandra.management.commands.CleanupCommand;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import rx.Completable;
import rx.Observable;
import rx.functions.Func1;

/**
 * This job performs maintenance on the Cassandra cluster. Currently there is initial support for running cleanup, and
 * we will add support for running anti-entropy repair. {@link CassandraManager} maintains an event log for relevant
 * events in the Cassandra cluster, and the job looks at that log to determine what if any maintenance needs to be
 * performed.
 *
 * @author jsanda
 */
public class CassandraMaintenanceJob implements Func1<JobDetails, Completable> {

    private static Logger logger = Logger.getLogger(CassandraMaintenanceJob.class);

    public static final String JOB_NAME = "CassandraMaintenanceJob";

    public static final String CONFIG_ID = "org.hawkular.metrics.jobs." + JOB_NAME;

    private ConfigurationService configurationService;

    private EventDataAccess eventDataAccess;

    private RxSession session;

    public CassandraMaintenanceJob(RxSession session, EventDataAccess eventDataAccess,
            ConfigurationService configurationService) {
        this.session = session;
        this.eventDataAccess = eventDataAccess;
        this.configurationService = configurationService;
    }

    @Override
    public Completable call(JobDetails jobDetails) {
        logger.info("Starting Cassandra cluster maintenance");
        Stopwatch stopwatch = Stopwatch.createStarted();

        String jmxPort = jobDetails.getParameters().get("jmxPort");
        String keyspace = jobDetails.getParameters().get("keyspace");

        return eventDataAccess.findEventsForBucket(getCurrentBucket())
                .collect(ClusterState::new, (state, event) -> {
                    try {
                        String hostname = event.getDetails().get("hostname");
                        InetAddress address = InetAddress.getByName(hostname);
                        state.setLastNodeAdded(address);
                    } catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    }
                })
                .flatMap(state -> {
                    if (state.getLastNodeAdded() == null) {
                        return Observable.empty();
                    }
                    return getNodes().filter(address -> !address.equals(state.getLastNodeAdded()));
                })
                .concatMap(address -> createExecutionContexts(address, getTables(keyspace), jmxPort, keyspace))
                .flatMap(context -> new CleanupCommand().run(context))
                .toCompletable()
                .concatWith(Completable.defer(() -> configurationService.save(CONFIG_ID, "lastRun",
                        Long.toString(jobDetails.getTrigger().getTriggerTime())).toCompletable()))
                .doOnCompleted(() -> {
                    stopwatch.stop();
                    logger.infof("Finished Cassandra cluster maintenance in %d ms",
                            stopwatch.elapsed(TimeUnit.MILLISECONDS));
                })
                .doOnError(t -> logger.warn("Cassandra cluster maintenance has been aborted to to an unexpected error",
                        t));
    }

    private long getCurrentBucket() {
        return DateTimeService.current24HourTimeSlice().getMillis();
    }

    private Observable<InetAddress> getNodes() {
        return Observable.from(session.getCluster().getMetadata().getAllHosts()).map(Host::getAddress);
    }

    private Observable<String> getTables(String keyspace) {
        return Observable.from(session.getCluster().getMetadata().getKeyspace(keyspace).getTables())
                .map(TableMetadata::getName)
                .cache();
    }

    private Observable<Map<String, String>> createExecutionContexts(InetAddress node, Observable<String> tables,
            String jmxPort, String keyspace) {
        return tables.map(table -> ImmutableMap.of(
                "hostname", node.getHostName(),
                "jmxPort", jmxPort,
                "keyspace", keyspace,
                "table", table
        ));
    }
}
