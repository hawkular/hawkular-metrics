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
package org.hawkular.metrics.scheduler.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.schema.SchemaService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeSuite;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;

import rx.Observable;

/**
 * @author jsanda
 */
public class JobSchedulerTest {
    protected static Session session;
    protected static RxSession rxSession;
    private static PreparedStatement findJob;
    private static PreparedStatement findScheduledJobs;
    private static PreparedStatement findFinishedJobs;
    private static PreparedStatement getActiveTimeSlices;
    private static PreparedStatement addActiveTimeSlice;

    @BeforeSuite
    public static void initSuite() {
        Cluster cluster = Cluster.builder().addContactPoints("127.0.0.01").build();
        String keyspace = System.getProperty("keyspace", "hawkulartest");
        session = cluster.connect("system");
        rxSession = new RxSessionImpl(session);

        boolean resetdb = Boolean.valueOf(System.getProperty("resetdb", "true"));

        SchemaService schemaService = new SchemaService();
        schemaService.run(session, keyspace, resetdb);

        session.execute("USE " + keyspace);

        findJob = session.prepare("SELECT type, name, params, trigger FROM jobs WHERE id = ?")
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        findScheduledJobs = session.prepare("SELECT job_id FROM scheduled_jobs_idx WHERE time_slice = ?");
        findFinishedJobs = session.prepare("SELECT job_id FROM finished_jobs_idx WHERE time_slice = ?");
        getActiveTimeSlices = session.prepare("SELECT distinct time_slice FROM active_time_slices");
        addActiveTimeSlice = session.prepare("INSERT INTO active_time_slices (time_slice) VALUES (?)");
    }

    protected void resetSchema() throws Exception {
        CountDownLatch truncationFinished = new CountDownLatch(1);

        Observable.merge(
                truncateTable("jobs"),
                truncateTable("scheduled_jobs_idx"),
                truncateTable("finished_jobs_idx"),
                truncateTable("active_time_slices"),
                truncateTable("locks")
        ).subscribe(
                resultSet -> {},
                t -> fail("Truncating tables failed", t),
                truncationFinished::countDown
        );

        truncationFinished.await();
    }

    private Observable<ResultSet> truncateTable(String table) {
        return rxSession.execute("TRUNCATE " + table);
    }

    protected Set<DateTime> getActiveTimeSlices() {
        return session.execute(getActiveTimeSlices.bind()).all().stream().map(row -> new DateTime(row.getTimestamp(0)))
                .collect(Collectors.toSet());
    }

    protected static void setActiveTimeSlices(DateTime... timeSlices) {
        for (DateTime timmeSlice : timeSlices) {
            session.execute(addActiveTimeSlice.bind(timmeSlice.toDate()));
        }
    }

    protected static Set<UUID> getScheduledJobs(DateTime timeSlice) {
        return getJobs(timeSlice, findScheduledJobs);
    }

    protected static Set<UUID> getFinishedJobs(DateTime timeSlice) {
        return getJobs(timeSlice, findFinishedJobs);
    }

    private static Set<UUID> getJobs(DateTime timeSlice, PreparedStatement query) {
        return session.execute(query.bind(timeSlice.toDate())).all().stream().map(row -> row.getUUID(0))
                .collect(Collectors.toSet());
    }

    protected static void assertJobEquals(JobDetails expected) {
        ResultSet resultSet = session.execute(findJob.bind(expected.getJobId()));
        assertFalse(resultSet.isExhausted(), "Failed to find " + expected + " in jobs table");
        Row row = resultSet.one();

        assertEquals(row.getString(0), expected.getJobType(), "The job type does not match");
        assertEquals(row.getString(1), expected.getJobName(), "The job name does not match");
        assertEquals(row.getMap(2, String.class, String.class), expected.getParameters(),
                "The job parameters do not match");

        UDTValue udtValue = row.getUDTValue(3);
        int triggerType = 0;
        if (expected.getTrigger() instanceof SingleExecutionTrigger) {
            triggerType = 0;
        } else if (expected.getTrigger() instanceof RepeatingTrigger) {
            triggerType = 1;
        } else {
            fail(expected.getTrigger().getClass().getName() + " is not a recognized trigger type");
        }
        assertEquals(udtValue.getInt(0), triggerType, "The trigger type does not match");
        assertEquals(udtValue.getLong(1), expected.getTrigger().getTriggerTime(), "The trigger time does not match");
    }

}
