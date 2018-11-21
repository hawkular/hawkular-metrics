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
package org.hawkular.metrics.scheduler.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.schema.SchemaService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeSuite;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;

import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author jsanda
 */
public class JobSchedulerTest {
    protected static Session session;
    protected static RxSession rxSession;
    private static PreparedStatement findFinishedJobs;
    private static PreparedStatement getActiveTimeSlices;
    private static PreparedStatement addActiveTimeSlice;

    protected static JobsService jobsService;

    @BeforeSuite
    public static void initSuite() {
        Cluster cluster = Cluster.builder()
                .addContactPoints("127.0.0.01")
                .withQueryOptions(new QueryOptions().setRefreshSchemaIntervalMillis(0))
                .build();
        String keyspace = System.getProperty("keyspace", "hawkulartest");
        session = cluster.connect("system");
        rxSession = new RxSessionImpl(session);

        boolean resetdb = Boolean.valueOf(System.getProperty("resetdb", "true"));

        SchemaService schemaService = new SchemaService();
        schemaService.run(session, keyspace, resetdb);

        session.execute("USE " + keyspace);

        jobsService = new JobsService(rxSession);

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

    protected static Set<JobDetailsImpl> getScheduledJobs(DateTime timeSlice) {
        List<JobDetailsImpl> jobs = jobsService.findScheduledJobsForTime(timeSlice.toDate(), Schedulers.computation())
                .toList()
                .toBlocking()
                .firstOrDefault(null);
        assertNotNull(jobs);
        return ImmutableSet.copyOf(jobs);
    }

    protected static JobDetails getScheduledJob(JobDetails details) {
        return getScheduledJobs(new DateTime(details.getTrigger().getTriggerTime()))
                .stream()
                .filter(scheduled -> scheduled.getJobId().equals(details.getJobId()))
                .findFirst()
                .orElseGet(() -> null);
    }

    protected static Set<UUID> getFinishedJobs(DateTime timeSlice) {
        return session.execute(findFinishedJobs.bind(timeSlice.toDate())).all().stream().map(row -> row.getUUID(0))
                .collect(Collectors.toSet());
    }

    protected static void assertJobEquals(JobDetails expected) {
        JobDetails actual = getScheduledJob(expected);

        assertNotNull(actual);
        assertEquals(actual.getJobType(), expected.getJobType(), "The job type does not match");
        assertEquals(actual.getJobName(), expected.getJobName(), "The job name does not match");
        assertEquals(actual.getParameters(), expected.getParameters(),
                "The job parameters do not match");
        assertEquals(actual.getTrigger(), expected.getTrigger(), "The triggers do not match");
    }

}
