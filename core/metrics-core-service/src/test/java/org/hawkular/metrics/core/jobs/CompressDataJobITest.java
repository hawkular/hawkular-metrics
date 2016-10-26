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
package org.hawkular.metrics.core.jobs;

import static java.util.Arrays.asList;

import static org.hawkular.metrics.core.jobs.CompressData.JOB_NAME;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hawkular.metrics.core.service.BaseITest;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.DataAccessImpl;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.core.service.Order;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
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

    private JobDetails compressionJob;

    @BeforeClass
    public void initClass() {
        dataAccess = new DataAccessImpl(session);

        resetConfig = session.prepare("DELETE FROM sys_config WHERE config_id = 'org.hawkular.metrics.jobs." +
                JOB_NAME + "'");

        configurationService = new ConfigurationService() ;
        configurationService.init(rxSession);

        metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(dataAccess);
        metricsService.setConfigurationService(configurationService);
        metricsService.startUp(session, getKeyspace(), true, new MetricRegistry());
    }

    @BeforeMethod
    public void initTest(Method method) {
        logger.debug("Starting [" + method.getName() + "]");

        session.execute(resetConfig.bind());

        jobScheduler = new TestScheduler(rxSession);
        long nextStart = LocalDateTime.now(ZoneOffset.UTC)
                .with(DateTimeService.startOfNextOddHour())
                .toInstant(ZoneOffset.UTC).toEpochMilli() - 60000;
        jobScheduler.advanceTimeTo(nextStart);
        jobScheduler.truncateTables(getKeyspace());

        jobsService = new JobsServiceImpl();
        jobsService.setSession(rxSession);
        jobsService.setScheduler(jobScheduler);
        jobsService.setMetricsService(metricsService);
        jobsService.setConfigurationService(configurationService);
        compressionJob = jobsService.start().stream().filter(details -> details.getJobName().equals(JOB_NAME))
                .findFirst().get();

        assertNotNull(compressionJob);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        jobsService.shutdown();
    }

    @Test
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
            latch.countDown();
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
    }

    @SuppressWarnings("unchecked")
    private <T> void testCompressResults(MetricType<T> type, Metric<T> metric, DateTime start) throws
            Exception {
        doAction(() -> metricsService.addDataPoints(type, Observable.just(metric)));

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(jobDetails -> {
            latch.countDown();
        });

//        for (JobDetails jobDetails : jobsService.getJobDetails().toBlocking().toIterable()) {
//            if(JOB_NAME.equals(jobDetails.getJobName())) {
//                jobScheduler.advanceTimeTo(jobDetails.getTrigger().getTriggerTime());
//                break;
//            }
//        }
        jobScheduler.advanceTimeTo(compressionJob.getTrigger().getTriggerTime());

        assertTrue(latch.await(25, TimeUnit.SECONDS));
        long startSlice = DateTimeService.getTimeSlice(start.getMillis(), Duration.standardHours(2));
        long endSlice = DateTimeService.getTimeSlice(jobScheduler.now(), Duration.standardHours(2));

        DataPointDecompressTransformer decompressor = new DataPointDecompressTransformer(type, Order.ASC, 0, start
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

    @Test
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

    @Test
    public void testAvailabilityCompress() throws Exception {
        long now = jobScheduler.now();

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

    private String nextTenantId() {
        return "T" + tenantCounter.getAndIncrement();
    }
}
