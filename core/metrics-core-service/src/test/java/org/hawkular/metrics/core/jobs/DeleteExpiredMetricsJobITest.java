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
package org.hawkular.metrics.core.jobs;

import static java.util.Arrays.asList;

import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hawkular.metrics.core.service.BaseITest;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.core.service.TestDataAccessFactory;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.impl.TestScheduler;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.PreparedStatement;
import com.google.common.collect.ImmutableMap;

import rx.Observable;

/**
 * Test the job that deletes expired metrics.
 *
 * @author Stefan Negrea
 */
public class DeleteExpiredMetricsJobITest extends BaseITest {

    private static Logger logger = Logger.getLogger(DeleteExpiredMetricsJobITest.class);

    private static AtomicInteger tenantCounter = new AtomicInteger();
    private MetricsServiceImpl metricsService;
    private DataAccess dataAccess;
    private JobsServiceImpl jobsService;
    private ConfigurationService configurationService;
    private TestScheduler jobScheduler;
    private PreparedStatement resetConfig;
    private JobDetails deleteExpiredMetricsJob;
    private String jobName;

    @BeforeClass
    public void initClass() {
        dataAccess = TestDataAccessFactory.newInstance(session);

        resetConfig = session.prepare("DELETE FROM sys_config WHERE config_id = 'org.hawkular.metrics.jobs." +
                DeleteExpiredMetrics.JOB_NAME + "'");

        configurationService = new ConfigurationService() ;
        configurationService.init(rxSession);

        metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(dataAccess);
        metricsService.setConfigurationService(configurationService);
        metricsService.startUp(session, getKeyspace(), true, metricRegistry);
    }

    @BeforeMethod
    public void initTest(Method method) {
        logger.debug("Starting [" + method.getName() + "]");

        session.execute(resetConfig.bind());

        jobName = method.getName();

        jobScheduler = new TestScheduler(rxSession);
        jobScheduler.truncateTables(getKeyspace());

        jobsService = new JobsServiceImpl(2, 7, true);
        jobsService.setSession(rxSession);
        jobsService.setScheduler(jobScheduler);
        jobsService.setMetricsService(metricsService);
        jobsService.setConfigurationService(configurationService);

        deleteExpiredMetricsJob = jobsService.start().stream()
                .filter(details -> details.getJobName().equals(DeleteExpiredMetrics.JOB_NAME))
                .findFirst().get();
        assertNotNull(deleteExpiredMetricsJob);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        jobsService.shutdown();
    }

    @AfterClass(alwaysRun = true)
    public void shutdown() {
        dataAccess.shutdown();
    }

    @Test
    public void testOnDemandDeleteExpiredMetricsJobCompressionEnabled() throws Exception {
        configurationService.save(CompressData.CONFIG_ID, "enabled", Boolean.TRUE.toString()).toBlocking();

        String tenantId = nextTenantId();
        DateTime start = new DateTime(jobScheduler.now());

        Metric<Double> g1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G1"),
                ImmutableMap.of("x", "1", "y", "2"), 10, asList(
                        new DataPoint<>(start.getMillis(), 1.1),
                        new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));
        Metric<Double> g2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G2"),
                ImmutableMap.of("x", "2", "y", "3"), 20, asList(
                        new DataPoint<>(start.getMillis(), 3.3),
                        new DataPoint<>(start.plusMinutes(2).getMillis(), 4.4)));
        Metric<Double> g3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G3"),
                ImmutableMap.of("x", "3", "y", "4"), 30, asList(
                        new DataPoint<>(start.getMillis(), 5.5),
                        new DataPoint<>(start.plusMinutes(2).getMillis(), 5.5)));

        doAction(() -> metricsService.createMetric(g1, true));
        doAction(() -> metricsService.createMetric(g2, true));
        doAction(() -> metricsService.createMetric(g3, true));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(g1, g2, g3)));

        List<Metric<Double>> metrics = getOnNextEvents(() -> metricsService.findMetrics(tenantId, GAUGE));
        assertEquals(metrics.size(), 3);

        long expiration = 1;
        runOnDemandDeleteExpiredMetricsJob(expiration);
        metrics = getOnNextEvents(() -> metricsService.findMetrics(tenantId, GAUGE));
        assertEquals(metrics.size(), 3);

        //verify that the expiration delay works by attempting to call the job repeatedly until close
        //to the boundary, the metric expires in 20 days, but there is 2 day delay,
        //When running the purge at 22 days - 2 hours, G2 should still be present
        for (int i = 18; i < 23; i++) {
            expiration = DateTimeService.now.get().getMillis() + (i * 24 - 2) * 3600 * 1000L;
            runOnDemandDeleteExpiredMetricsJob(expiration);
            metrics = getOnNextEvents(() -> metricsService.findMetrics(tenantId, GAUGE));
            assertEquals(metrics.size(), 2);
            for (Metric<?> metric : metrics) {
                assertNotEquals(metric.getId(), "G1");
            }
        }

        expiration = DateTimeService.now.get().getMillis() + 28 * 24 * 3600 * 1000L;
        runOnDemandDeleteExpiredMetricsJob(expiration);
        metrics = getOnNextEvents(() -> metricsService.findMetrics(tenantId, GAUGE));
        assertEquals(metrics.size(), 1);
        assertEquals(metrics.get(0).getId(), "G3");

        expiration = DateTimeService.now.get().getMillis() + 32 * 24 * 3600 * 1000L;
        runOnDemandDeleteExpiredMetricsJob(expiration);
        metrics = getOnNextEvents(() -> metricsService.findMetrics(tenantId, GAUGE));
        assertEquals(metrics.size(), 0);
    }

    @Test
    public void testOnDemandDeleteExpiredMetricsJobCompressionDisabled() throws Exception {
        configurationService.save(CompressData.CONFIG_ID, "enabled", Boolean.FALSE.toString()).toBlocking();

        String tenantId = nextTenantId();
        DateTime start = new DateTime(jobScheduler.now());

        Metric<Double> g1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G1"),
                ImmutableMap.of("x", "1", "y", "2"), 1);
        Metric<Double> g2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G2"),
                ImmutableMap.of("x", "2", "y", "3"), 2, asList(
                        new DataPoint<>(start.getMillis(), 3.3),
                        new DataPoint<>(start.plusMinutes(2).getMillis(), 4.4)));

        doAction(() -> metricsService.createMetric(g1, true));
        doAction(() -> metricsService.createMetric(g2, true));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(g2)));

        List<Metric<Double>> metrics = getOnNextEvents(() -> metricsService.findMetrics(tenantId, GAUGE));
        assertEquals(metrics.size(), 2);

        long expiration = DateTimeService.now.get().getMillis() + 10 * 24 * 3600 * 1000L;
        runOnDemandDeleteExpiredMetricsJob(expiration);
        metrics = getOnNextEvents(() -> metricsService.findMetrics(tenantId, GAUGE));
        assertEquals(metrics.size(), 1);
        assertEquals(metrics.get(0).getId(), "G2");
    }

    private void runOnDemandDeleteExpiredMetricsJob(long time) throws InterruptedException {
        JobDetails runJobDetails = jobsService.submitDeleteExpiredMetricsJob(time, jobName).toBlocking().value();
        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(jobDetails -> {
            logger.debug("Finished " + runJobDetails);
            latch.countDown();
        });
        jobScheduler.advanceTimeTo(runJobDetails.getTrigger().getTriggerTime());

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testScheduleDeleteExpiredMetricsJob() throws Exception {
        configurationService.save(CompressData.CONFIG_ID, "enabled", Boolean.TRUE.toString()).toBlocking();
        String tenantId = nextTenantId();

        Metric<Long> c1 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"),
                ImmutableMap.of("x", "1", "y", "2"), 1);
        Metric<Long> c2 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C2"),
                ImmutableMap.of("x", "2", "y", "3"), 12);

        doAction(() -> metricsService.createMetric(c1, true));
        dataAccess.updateMetricExpirationIndex(c1.getMetricId(),
                DateTimeService.now.get().getMillis() - 3 * 24 * 3600 * 1000L).toBlocking();
        doAction(() -> metricsService.createMetric(c2, true));

        List<Metric<Long>> metrics = getOnNextEvents(() -> metricsService.findMetrics(tenantId, COUNTER));
        assertEquals(metrics.size(), 2);

        waitForScheduledDeleteExpiredMetricsJob();
        metrics = getOnNextEvents(() -> metricsService.findMetrics(tenantId, COUNTER));
        assertEquals(metrics.size(), 1);
        assertEquals(metrics.get(0).getId(), "C2");
    }

    @Test
    public void testScheduleDeleteExpiredMetricsJobDisabled() throws Exception {
        int[] jobFrequency = { -1, 0, 1, -100 };
        boolean[] jobEnabled = { true, true, false, false };

        for (int i = 0; i < jobFrequency.length; i++) {
            jobsService = new JobsServiceImpl(2, jobFrequency[i], jobEnabled[i]);
            jobsService.setSession(rxSession);
            jobsService.setScheduler(jobScheduler);
            jobsService.setMetricsService(metricsService);
            jobsService.setConfigurationService(configurationService);

            assertEquals(jobsService.start().stream()
                    .filter(details -> details.getJobName().equals(DeleteExpiredMetrics.JOB_NAME))
                    .count(), 0);
        }
    }

    private void waitForScheduledDeleteExpiredMetricsJob() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(jobDetails -> {
            latch.countDown();
        });

        jobScheduler.advanceTimeTo(deleteExpiredMetricsJob.getTrigger().getTriggerTime());
        jobScheduler.advanceTimeBy(2);
        assertTrue(latch.await(25, TimeUnit.SECONDS));
    }

    private String nextTenantId() {
        return "T" + tenantCounter.getAndIncrement();
    }
}
