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

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.util.Date;
import java.util.Map;

import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.metrics.schema.SchemaService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableMap;

/**
 * @author jsanda
 */
public class JobSchedulingTest {

    protected static Session session;

    protected static RxSession rxSession;

    protected static SchedulerImpl jobScheduler;

    private static PreparedStatement findActiveQueue;

    private static PreparedStatement insertActiveQueue;

    private static PreparedStatement findJob;

    @BeforeSuite
    public static void initSuite() {
        Cluster cluster = Cluster.builder().addContactPoints("127.0.0.01").build();
        String keyspace = System.getProperty("keyspace", "hawkulartest");
        session = cluster.connect("system");
        rxSession = new RxSessionImpl(session);

        SchemaService schemaService = new SchemaService();
        schemaService.run(session, keyspace, true);

        session.execute("USE " + keyspace);

        jobScheduler = new SchedulerImpl(rxSession);

        findActiveQueue = session.prepare(
                "SELECT value FROM system_settings WHERE key = 'org.hawkular.metrics.scheduler.active-queue'")
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        insertActiveQueue = session.prepare(
                "INSERT INTO system_settings (key, value) VALUES ('org.hawkular.metrics.scheduler.active-queue', ?)")
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        findJob = session.prepare("SELECT type, name, params, trigger FROM jobs WHERE id = ?")
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }

    @Test
    public void scheduleSingleExecutionJob() {
        DateTime activeQueue = currentMinute().plusMinutes(1);
        setActiveQueue(activeQueue);
        Trigger trigger = new SingleExecutionTrigger.Builder()
                .withTriggerTime(activeQueue.plusMinutes(5).getMillis())
                .build();
        String type = "Test-Job";
        String name = "Test Job";
        Map<String, String> params = ImmutableMap.of("x", "1", "y", "2");
        JobDetails details = jobScheduler.scheduleJob(type, name, params, trigger).toBlocking().firstOrDefault(null);

        assertNotNull(details);
        assertJobEquals(details);
    }

    protected static void setActiveQueue(DateTime dateTime) {
        session.execute(insertActiveQueue.bind(Long.toString(dateTime.getMillis())));
    }

    protected static Date findActiveQueue() {
        ResultSet resultSet = session.execute(findActiveQueue.bind());
        if (resultSet.isExhausted()) {
            fail("The [org.hawkular.metrics.scheduler.active-queue] system setting should be set!");
        }
        return resultSet.all().get(0).getTimestamp(0);
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
        assertEquals(udtValue.getInt(0), 0, "The trigger type does not match");
        assertEquals(udtValue.getLong(1), expected.getTrigger().getTriggerTime(), "The trigger time does not match");
    }

}
