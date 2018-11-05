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
package org.hawkular.metrics.core.jobs;

import static java.util.Arrays.asList;

import static org.hawkular.metrics.core.jobs.TempDataCompressor.JOB_NAME;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hawkular.metrics.core.service.BaseITest;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.core.service.TestDataAccessFactory;
import org.hawkular.metrics.core.service.transformers.DataPointDecompressTransformer;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.Tenant;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.impl.TestScheduler;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;

import rx.Observable;
import rx.observers.TestSubscriber;

/**
 * Test the compression ETL jobs
 *
 * @author Michael Burman
 */
public class CompressDataJobITest extends BaseITest {

    private static Logger logger = Logger.getLogger(CompressDataJobITest.class);

    private static AtomicInteger tenantCounter = new AtomicInteger();
    private MetricsServiceImpl metricsService;
    private DataAccess dataAccess;
    private JobsServiceImpl jobsService;
    private ConfigurationService configurationService;
    private TestScheduler jobScheduler;
    private PreparedStatement resetConfig;
    private PreparedStatement resetConfig2;

    private boolean firstExecute = true;

    private JobDetails compressionJob;

    @BeforeClass
    public void initClass() {
        dataAccess = TestDataAccessFactory.newInstance(session);

        resetConfig = session.prepare("DELETE FROM sys_config WHERE config_id = 'org.hawkular.metrics.jobs." +
                JOB_NAME + "'");

        resetConfig2 = session.prepare("DELETE FROM sys_config WHERE config_id = 'org.hawkular.metrics.jobs." +
                TempTableCreator.JOB_NAME + "'");

        session.execute(resetConfig.bind());
        session.execute(resetConfig2.bind());

        configurationService = new ConfigurationService();
        configurationService.init(rxSession);

        metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(dataAccess);
        metricsService.setConfigurationService(configurationService);
        metricsService.startUp(session, getKeyspace(), true, metricRegistry);

        jobScheduler = new TestScheduler(rxSession);
        jobScheduler.truncateTables(getKeyspace());

        jobsService = new JobsServiceImpl();
        jobsService.setSession(rxSession);
        jobsService.setScheduler(jobScheduler);
        jobsService.setMetricsService(metricsService);
        jobsService.setConfigurationService(configurationService);
        List<JobDetails> jobDetails = jobsService.start();

        JobDetails tableCreator =
                jobDetails.stream().filter(d -> d.getJobName().equalsIgnoreCase(TempTableCreator.JOB_NAME))
                        .findFirst().get();

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(details -> {
            if(details.getJobName().equals(TempTableCreator.JOB_NAME)) {
                latch.countDown();
            }
        });

        jobScheduler.advanceTimeTo(tableCreator.getTrigger().getTriggerTime());
        jobScheduler.advanceTimeBy(1);

        try {
            assertTrue(latch.await(25, TimeUnit.SECONDS)); // Wait for tables to be ready
            Thread.sleep(3000); // Wait for the prepared statements to be initialized even in Travis
        } catch (InterruptedException e) {
            assertTrue(false);
        }

        compressionJob = jobDetails
                .stream()
                .filter(details -> details.getJobName().equals(JOB_NAME))
                .findFirst().get();

        long nextStart = LocalDateTime.ofInstant(Instant.ofEpochMilli(jobScheduler.now()), ZoneOffset.UTC)
                .with(DateTimeService.startOfNextOddHour())
                .toInstant(ZoneOffset.UTC).toEpochMilli();

        CountDownLatch latch2 = new CountDownLatch(1);
        jobScheduler.onJobFinished(details -> {
            if(details.getJobName().equals(JOB_NAME)) {
                latch2.countDown();
            }
        });

        jobScheduler.advanceTimeTo(nextStart);
        jobScheduler.advanceTimeBy(1);
        assertNotNull(compressionJob);
        try {
            assertTrue(latch.await(25, TimeUnit.SECONDS)); // Wait for first compression to pass
        } catch (InterruptedException e) {
            assertTrue(false);
        }
    }

    @BeforeMethod
    public void initTest(Method method) {
        logger.debug("Starting [" + method.getName() + "]");

        if(!firstExecute) {
            jobScheduler.advanceTimeBy(120);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
    }

    @AfterClass(alwaysRun = true)
    public void shutdown() {
        dataAccess.shutdown();
    }

    @Test(priority = 1)
    public void testCompressJob() throws Exception {
        long now = jobScheduler.now();

        DateTime start = DateTimeService.getTimeSlice(new DateTime(now, DateTimeZone.UTC).minusHours(2),
                Duration.standardHours(2)).plusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = nextTenantId() + now;

        MetricId<Double> mId = new MetricId<>(tenantId, GAUGE, "m1");

        doAction(() -> metricsService.createTenant(new Tenant(tenantId), false));

        Metric<Double> m1 = new Metric<>(mId, asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(end.getMillis(), 4.4)));

        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(jobDetails -> {
            if(jobDetails.getJobName().equals(JOB_NAME)) {
                latch.countDown();
            }
        });

        jobScheduler.advanceTimeTo(compressionJob.getTrigger().getTriggerTime());
        jobScheduler.advanceTimeBy(1);

        assertTrue(latch.await(25, TimeUnit.SECONDS));
        long startSlice = DateTimeService.getTimeSlice(start.getMillis(), Duration.standardHours(2));
        long endSlice = DateTimeService.getTimeSlice(jobScheduler.now(), Duration.standardHours(2));

        Observable<Row> compressedRows = dataAccess.findCompressedData(mId, startSlice, endSlice, 0, Order.ASC);

        TestSubscriber<Row> testSubscriber = new TestSubscriber<>();
        compressedRows.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();

        List<Row> rows = testSubscriber.getOnNextEvents();
        assertEquals(1, rows.size());
        ByteBuffer c_value = rows.get(0).getBytes("c_value");
        ByteBuffer tags = rows.get(0).getBytes("tags");

        assertNotNull(c_value);
        assertNull(tags);

        firstExecute = false;
    }

    private <T> void testCompressResults(MetricType<T> type, Metric<T> metric, DateTime start) throws
            Exception {
        if (metric.getDataPoints() != null && !metric.getDataPoints().isEmpty()) {
            doAction(() -> metricsService.addDataPoints(type, Observable.just(metric)));
        }

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(jobDetails -> {
            if(jobDetails.getJobName().equals(JOB_NAME)) {
                latch.countDown();
            }
        });

        jobScheduler.advanceTimeBy(1);

        assertTrue(latch.await(25, TimeUnit.SECONDS));
        long startSlice = DateTimeService.getTimeSlice(start.getMillis(), Duration.standardHours(2));
        long endSlice = DateTimeService.getTimeSlice(start.plusHours(1).plusMinutes(59).getMillis(), Duration
                .standardHours(2));

        DataPointDecompressTransformer<T> decompressor = new DataPointDecompressTransformer<>(type, Order.ASC, 0, start
                .getMillis(), start.plusMinutes(30).getMillis());

        Observable<DataPoint<T>> dataPoints = dataAccess.findCompressedData(metric.getMetricId(), startSlice, endSlice,
                0, Order.ASC).compose(decompressor);

        TestSubscriber<DataPoint<T>> pointTestSubscriber = new TestSubscriber<>();
        dataPoints.subscribe(pointTestSubscriber);
        pointTestSubscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        pointTestSubscriber.assertCompleted();
        List<DataPoint<T>> compressedPoints = pointTestSubscriber.getOnNextEvents();

        assertEquals(metric.getDataPoints(), compressedPoints);
    }

    @Test(dependsOnMethods={"testCompressJob"})
    public void testGaugeCompress() throws Exception {
        long now = jobScheduler.now();

        DateTime start = DateTimeService.getTimeSlice(new DateTime(now, DateTimeZone.UTC).minusHours(2),
                Duration.standardHours(2)).plusMinutes(30);

        DateTime end = start.plusMinutes(20);
        String tenantId = nextTenantId() + now;

        MetricId<Double> mId = new MetricId<>(tenantId, GAUGE, "m1");

        doAction(() -> metricsService.createTenant(new Tenant(tenantId), false));

        Metric<Double> m1 = new Metric<>(mId, asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(end.getMillis(), 4.4)));

        testCompressResults(GAUGE, m1, start);
    }

    @Test(dependsOnMethods={"testCompressJob"})
    public void testCounterCompress() throws Exception {
        long now = jobScheduler.now();

        DateTime start = DateTimeService.getTimeSlice(new DateTime(now, DateTimeZone.UTC).minusHours(2),
                Duration.standardHours(2)).plusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = nextTenantId() + now;

        MetricId<Long> mId = new MetricId<>(tenantId, COUNTER, "m2");

        doAction(() -> metricsService.createTenant(new Tenant(tenantId), false));

        Metric<Long> m1 = new Metric<>(mId, asList(
                new DataPoint<>(start.getMillis(), 1L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2L),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3L),
                new DataPoint<>(end.getMillis(), 4L)));

        testCompressResults(COUNTER, m1, start);
    }

    @Test(dependsOnMethods={"testCompressJob"})
    public void testAvailabilityCompress() throws Exception {
        long now = jobScheduler.now(); // I need to advance the triggerTime of compression job also

        DateTime start = DateTimeService.getTimeSlice(new DateTime(now, DateTimeZone.UTC).minusHours(2),
                Duration.standardHours(2)).plusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = nextTenantId() + now;

        MetricId<AvailabilityType> mId = new MetricId<>(tenantId, AVAILABILITY, "m3");

        doAction(() -> metricsService.createTenant(new Tenant(tenantId), false));

        Metric<AvailabilityType> m1 = new Metric<>(mId, asList(
                new DataPoint<>(start.getMillis(), AvailabilityType.UP),
                new DataPoint<>(start.plusMinutes(2).getMillis(), AvailabilityType.DOWN),
                new DataPoint<>(start.plusMinutes(4).getMillis(), AvailabilityType.DOWN),
                new DataPoint<>(end.getMillis(), AvailabilityType.UP)));

        testCompressResults(AVAILABILITY, m1, start);
    }

    @Test(dependsOnMethods={"testCompressJob"})
    public void testGaugeWithTags() throws Exception {
        long now = jobScheduler.now();

        DateTime start = DateTimeService.getTimeSlice(new DateTime(now, DateTimeZone.UTC).minusHours(2),
                Duration.standardHours(2)).plusMinutes(30);

        DateTime end = start.plusMinutes(20);
        String tenantId = nextTenantId() + now;

        MetricId<Double> mId = new MetricId<>(tenantId, GAUGE, "m1");

        doAction(() -> metricsService.createTenant(new Tenant(tenantId), false));

        Metric<Double> m1 = new Metric<>(mId, asList(
                new DataPoint<>(start.getMillis(), 1.1, ImmutableMap.of("a", "b")),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3, ImmutableMap.of("d", "e")),
                new DataPoint<>(end.getMillis(), 4.4)));

        testCompressResults(GAUGE, m1, start);
    }

    @Test(dependsOnMethods={"testCompressJob"})
    public void testCompressRetentionIndex() throws Exception {
        long now = jobScheduler.now();

        DateTime start = DateTimeService.getTimeSlice(new DateTime(now, DateTimeZone.UTC).minusHours(2),
                Duration.standardHours(2)).plusMinutes(30);

        DateTime end = start.plusMinutes(20);
        String tenantId = nextTenantId() + now;

        doAction(() -> metricsService.createTenant(new Tenant(tenantId), false));

        //create a metric definition but delete the expiration index entry
        //to ensure that an expiration entry is not created without data points
        MetricId<Double> mId2 = new MetricId<>(tenantId, GAUGE, "m2");
        Metric<Double> m2 = new Metric<>(mId2);
        doAction(() -> metricsService.createMetric(m2, true));

        MetricId<Double> mId = new MetricId<>(tenantId, GAUGE, "m1");
        Metric<Double> m1 = new Metric<>(mId, asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(end.getMillis(), 4.4)));
        testCompressResults(GAUGE, m1, start);
    }

    private String nextTenantId() {
        return "T" + tenantCounter.getAndIncrement();
    }
}
