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

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.google.common.collect.ImmutableMap;

/**
 * CassandraManager is intended to be the primary interface to the rest of Hawkular Metrics. It takes care of
 * scheduling the maintenance job and it updates an event log for the Cassandra cluster. That event log in turn is used
 * to determine what if any maintenance needs to be performed.
 *
 * @author jsanda
 */
public class CassandraManager {

    private static Logger logger = Logger.getLogger(CassandraManager.class);

    public static final short NODE_ADDED = 1;

    private RxSession session;

    private EventDataAccess eventDataAccess;

    private ConfigurationService configurationService;

    private String keyspace;

    private String jmxPort;

    private Scheduler jobScheduler;

    public CassandraManager(RxSession session, Scheduler scheduler, ConfigurationService configurationService) {
        this.session = session;
        eventDataAccess = new EventDataAccess(session);
        jobScheduler = scheduler;
        this.configurationService = configurationService;

        session.getCluster().register(new Host.StateListener() {
            @Override
            public void onAdd(Host host) {
                logger.debugf("Cassandra node %s has joined the cluster", host);
                nodeAdded(host);
            }

            @Override
            public void onUp(Host host) {

            }

            @Override
            public void onDown(Host host) {

            }

            @Override
            public void onRemove(Host host) {

            }

            @Override
            public void onRegister(Cluster cluster) {

            }

            @Override
            public void onUnregister(Cluster cluster) {

            }
        });
    }

    public void setupMaintenanceJob() {
        CassandraMaintenanceJob job = new CassandraMaintenanceJob(session, eventDataAccess, configurationService);
        jobScheduler.register(CassandraMaintenanceJob.JOB_NAME, job);
//        Trigger trigger = new RepeatingTrigger.Builder()
//                .withTriggerTime(DateTimeService.current24HourTimeSlice().plusDays(1).getMillis())
//                .withInterval(1, TimeUnit.DAYS)
//                .build();
        Trigger trigger = new RepeatingTrigger.Builder()
                .withInterval(5, TimeUnit.MINUTES)
                .build();
        // TODO Do not hard code job parameters
        // I have hard coded the parameters for now while I do some initial testing.
        Map<String, String> parameters = ImmutableMap.of("keyspace", "hawkular_metrics", "jmxPort", "7100");
        jobScheduler.scheduleJob(CassandraMaintenanceJob.JOB_NAME, CassandraMaintenanceJob.JOB_NAME, parameters,
                trigger).subscribe(
                        details -> logger.infof("Scheduled %s", details),
                        t -> logger.warnf(t, "Failed to schedule %s", CassandraMaintenanceJob.JOB_NAME)
                );
    }

    private void nodeAdded(Host host) {
        Date bucket = DateTimeService.current24HourTimeSlice().toDate();
        Map<String, String> details = ImmutableMap.of("hostname", host.getAddress().getHostName());
        eventDataAccess.addEvent(new Event(EventType.NODE_ADDED, details)).subscribe(
                () -> {},
                t -> logger.warnf(t, "Failed to log node added event for %s", host)
        );
    }

}
