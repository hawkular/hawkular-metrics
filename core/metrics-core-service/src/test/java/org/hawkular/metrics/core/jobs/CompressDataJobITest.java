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
import java.util.List;
import java.util.UUID;
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

    private static long TIMEOUT = 25;

    private static AtomicInteger tenantCounter = new AtomicInteger();
    private MetricsServiceImpl metricsService;
    private DataAccess dataAccess;
    private JobsServiceImpl jobsService;
    private ConfigurationService configurationService;
    private TestScheduler jobScheduler;

    private JobDetails compressionJob;

    private long triggerTime;

    /**
     * A new job scheduler is initialized for each test method. This method performs some set up that can be avoided for
     * each test. The TempTableCreator job needs to run in order to create the raw, temp tables. It is run in this
     * method, and the compression job is not run. This is done to avoid unnecessary work and to better facilitate
     * testing things in isolation.
     */
    @BeforeClass
    public void initClass() throws Exception {
        // Since the temp table creator job does not run for every test method, we do not want to drop the temp table
        // at the end of the compression job. Doing so could result in subsequent test failures.
        dataAccess = TestDataAccessFactory.newInstance(session, DateTimeService.now.get(), false);

        configurationService = new ConfigurationService();
        configurationService.init(rxSession);

        metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(dataAccess);
        metricsService.setConfigurationService(configurationService);
        metricsService.startUp(session, getKeyspace(), true, metricRegistry);

        // Remove job configurations that might be left over from any other test runs. This has to be done; otherwise,
        // new instances of the jobs will not get created and scheduled.
        removeJobConfig(TempTableCreator.CONFIG_ID);
        removeJobConfig(TempDataCompressor.CONFIG_ID);

        // This is a bit of hack that will suppress the compression job from getting scheduled. If JobsManager finds
        // a configuration for a job in the sys_config table, it will not schedule the job because it then assumes that
        // the job is already scheduled. We do this because we only want to create the temp tables that are needed for
        // the test methods.
        saveJobConfigJobId(TempDataCompressor.CONFIG_ID, UUID.randomUUID().toString());

        jobScheduler = new TestScheduler(rxSession);
        jobScheduler.truncateTables(getKeyspace());

        List<JobDetails> jobDetails = jobsManager.installJobs();

        jobsService = new JobsServiceImpl();
        jobsService.setSession(rxSession);
        jobsService.setScheduler(jobScheduler);
        jobsService.setMetricsService(metricsService);
        jobsService.setConfigurationService(configurationService);
        jobsService.start();

        JobDetails tableCreator =
                jobDetails.stream().filter(d -> d.getJobName().equalsIgnoreCase(TempTableCreator.JOB_NAME))
                        .findFirst().orElse(null);
        assertNotNull(tableCreator);

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(details -> {
            if (details.getJobName().equals(TempTableCreator.JOB_NAME)) {
                latch.countDown();
            }
        });

        jobScheduler.advanceTimeTo(tableCreator.getTrigger().getTriggerTime());
        jobScheduler.advanceTimeBy(1);

        assertTrue(latch.await(TIMEOUT, TimeUnit.SECONDS));

        // We have to remove this configuration; otherwise, the job will not get scheduled and the initTest method
        // will fail.
        removeJobConfig(TempDataCompressor.CONFIG_ID);

        jobScheduler.shutdown();
    }

    private void removeJobConfig(String jobConfigId) {
        boolean deleted = configurationService.delete(jobConfigId).await(TIMEOUT, TimeUnit.SECONDS);
        assertTrue(deleted);
    }

    private void saveJobConfigJobId(String jobConfigId, String jobId) {
        boolean saved = configurationService.save(jobConfigId, "jobId", jobId)
                .toCompletable()
                .await(TIMEOUT, TimeUnit.SECONDS);
        assertTrue(saved);
    }

    @BeforeMethod
    public void initTest(Method method) throws Exception {
        logger.debug("Starting [" + method.getName() + "]");

        jobScheduler = new TestScheduler(rxSession);
        jobScheduler.truncateTables(getKeyspace());

        // We use the hack here again of creating the job configuration as a means of preventing the job from getting
        // scheduled. We want to avoid scheduling the TempTableCreator job so that we can better test the compression
        // job in isolation.
        saveJobConfigJobId(TempTableCreator.CONFIG_ID, UUID.randomUUID().toString());

        List<JobDetails> jobDetails = jobsManager.installJobs();

        jobsService = new JobsServiceImpl();
        jobsService.setSession(rxSession);
        jobsService.setScheduler(jobScheduler);
        jobsService.setMetricsService(metricsService);
        jobsService.setConfigurationService(configurationService);
        jobsService.start();

        compressionJob = jobDetails
                .stream()
                .filter(details -> details.getJobName().equals(JOB_NAME))
                .findFirst().orElse(null);
        assertNotNull(compressionJob);

        triggerTime = compressionJob.getTrigger().getTriggerTime();

        // We advance the scheduler's clock to triggerTime, and it is important to note that we cannot advance the clock
        // any later because test methods need  set up test data before the job runs. Each test method is then
        // responsible for advancing the clock to trigger the job.
        jobScheduler.advanceTimeTo(triggerTime);
        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onTimeSliceFinished(time -> {
            if (time.equals(new DateTime(triggerTime).minusMinutes(1))) {
                latch.countDown();
            }
        });
        assertTrue(latch.await(TIMEOUT, TimeUnit.SECONDS));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        // We need to once again remove the job configuration; otherwise, the job will not get rescheduled in the
        // initTest method, and it will fail.
        removeJobConfig(TempDataCompressor.CONFIG_ID);
        jobScheduler.shutdown();
    }

    @AfterClass(alwaysRun = true)
    public void shutdown() {
        dataAccess.shutdown();
    }

    @Test
    public void testCompressJob() throws Exception {
        long now = triggerTime;

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

        jobScheduler.advanceTimeBy(1);

        assertTrue(latch.await(TIMEOUT, TimeUnit.SECONDS));
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

        assertTrue(latch.await(TIMEOUT, TimeUnit.SECONDS));
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

    @Test
    public void testGaugeCompress() throws Exception {
        long now = triggerTime;

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

    @Test
    public void testCounterCompress() throws Exception {
        long now = triggerTime;

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

    @Test
    public void testAvailabilityCompress() throws Exception {
        long now = triggerTime;

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

    @Test
    public void testGaugeWithTags() throws Exception {
        long now = triggerTime;

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

    @Test
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
