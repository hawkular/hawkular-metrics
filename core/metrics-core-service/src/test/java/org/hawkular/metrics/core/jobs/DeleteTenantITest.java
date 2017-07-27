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

import static org.hawkular.metrics.core.service.Order.ASC;
import static org.hawkular.metrics.model.AvailabilityType.DOWN;
import static org.hawkular.metrics.model.AvailabilityType.UP;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.hawkular.metrics.model.MetricType.STRING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hawkular.metrics.core.service.BaseITest;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.core.service.TestDataAccessFactory;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableMap;

import rx.Observable;

/**
 * @author jsanda
 */
public class DeleteTenantITest extends BaseITest {

    private static Logger logger = Logger.getLogger(DeleteTenantITest.class);

    private DataAccess dataAccess;
    private MetricsServiceImpl metricsService;

    private ConfigurationService configurationService;

    private TestScheduler jobScheduler;

    private JobsServiceImpl jobsService;

    private static AtomicInteger tenantCounter;

    private PreparedStatement getTags;

    private PreparedStatement getRetentions;

    private String jobName;

    @BeforeClass
    public void initClass() {
        tenantCounter = new AtomicInteger();

        getTags = session.prepare(
                "SELECT tvalue, type, metric FROM metrics_tags_idx WHERE tenant_id = ? AND tname = ?");
        getRetentions = session.prepare("SELECT metric FROM retentions_idx WHERE tenant_id = ? AND type = ?");

        dataAccess = TestDataAccessFactory.newInstance(session);

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

        jobName = method.getName();

        jobScheduler = new TestScheduler(rxSession);
        jobScheduler.truncateTables(getKeyspace());

        jobsService = new JobsServiceImpl();
        jobsService.setSession(rxSession);
        jobsService.setScheduler(jobScheduler);
        jobsService.setMetricsService(metricsService);
        jobsService.setConfigurationService(configurationService);
        jobsService.start();
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
    public void deleteTenantHavingGaugesAndNoMetricTags() throws Exception {
        String tenantId = nextTenantId();
        DateTime start = new DateTime(jobScheduler.now());

        Metric<Double> g1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G1"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));
        Metric<Double> g2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G2"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));

        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(g1, g2)));

        JobDetails details = jobsService.submitDeleteTenantJob(tenantId, jobName).toBlocking().value();

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(jobDetails -> {
            logger.debug("Finished " + details);
            latch.countDown();
        });

        jobScheduler.advanceTimeTo(details.getTrigger().getTriggerTime());

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        assertDataEmpty(g1, start, start.plusMinutes(3));
        assertDataEmpty(g2, start, start.plusMinutes(3));

        List<Metric<Double>> metrics = getOnNextEvents(() -> metricsService.findMetrics(tenantId, GAUGE));
        assertTrue(metrics.isEmpty());
    }

    @Test
    public void deleteTenantHavingGaugesWithMetricTagsAndDataRetention() throws Exception {
        String tenantId = nextTenantId();
        DateTime start = new DateTime(jobScheduler.now());

        Metric<Double> g1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G1"),
                ImmutableMap.of("x", "1", "y", "2"), 10, asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));
        Metric<Double> g2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G2"),
                ImmutableMap.of("x", "2", "y", "3"), 20, asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));

        doAction(() -> metricsService.createMetric(g1, true));
        doAction(() -> metricsService.createMetric(g2, true));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(g1, g2)));

        JobDetails details = jobsService.submitDeleteTenantJob(tenantId, jobName).toBlocking().value();

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(jobDetails -> {
            logger.debug("Finished " + details);
            latch.countDown();
        });

        jobScheduler.advanceTimeTo(details.getTrigger().getTriggerTime());

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        assertDataEmpty(g1, start, start.plusMinutes(3));
        assertDataEmpty(g2, start, start.plusMinutes(3));

        assertMetricTagIndexEmpty(tenantId, "x");
        assertMetricTagIndexEmpty(tenantId, "y");
        assertRetentionsIndexEmpty(tenantId, GAUGE);

        List<Metric<Double>> metrics = getOnNextEvents(() -> metricsService.findMetrics(tenantId, GAUGE));
        assertTrue(metrics.isEmpty());
    }

    @Test
    public void deleteTenantWithSettings() throws Exception {
        Tenant tenant = new Tenant(nextTenantId(), ImmutableMap.of(GAUGE, 10, COUNTER, 15, STRING, 20));
        doAction(() -> metricsService.createTenant(tenant, true));

        JobDetails details = jobsService.submitDeleteTenantJob(tenant.getId(), jobName).toBlocking().value();

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(jobDetails -> {
            logger.debug("Finished " + details);
            latch.countDown();
        });

        jobScheduler.advanceTimeTo(details.getTrigger().getTriggerTime());

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        assertRetentionsIndexEmpty(tenant.getId(), GAUGE);
        assertRetentionsIndexEmpty(tenant.getId(), COUNTER);
        assertRetentionsIndexEmpty(tenant.getId(), STRING);

        List<Tenant> tenants = getOnNextEvents(() -> metricsService.getTenants()
                .filter(t -> t.getId().equals(tenant.getId())));
        assertEquals(tenants.size(), 0, "Expected " + tenant + " to be deleted from tenants table");
    }

    @Test
    public void deleteTenantHavingAllMetricTypes() throws Exception {
        String tenantId = nextTenantId();
        DateTime start = new DateTime(jobScheduler.now());

        Metric<Double> g1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G1"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));
        Metric<Double> g2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G2"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));

        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(g1, g2)));

        Metric<Long> c1 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>(start.getMillis(), 10L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 20L)));
        Metric<Long> c2 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C2"), asList(
                new DataPoint<>(start.getMillis(), 10L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 20L)));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(c1, c2)));

        Metric<AvailabilityType> a1 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "A1"), asList(
                new DataPoint<>(start.getMillis(), UP),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN)));
        Metric<AvailabilityType> a2 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "A2"), asList(
                new DataPoint<>(start.getMillis(), UP),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN)));

        doAction(() -> metricsService.addDataPoints(AVAILABILITY, Observable.just(a1, a2)));

        Metric<String> s1 = new Metric<>(new MetricId<>(tenantId, STRING, "S1"), asList(
                new DataPoint<>(start.getMillis(), "starting"),
                new DataPoint<>(start.plusMinutes(2).getMillis(), "stopping")));
        Metric<String> s2 = new Metric<>(new MetricId<>(tenantId, STRING, "S2"), asList(
                new DataPoint<>(start.getMillis(), "starting"),
                new DataPoint<>(start.plusMinutes(2).getMillis(), "stopping")));

        doAction(() -> metricsService.addDataPoints(STRING, Observable.just(s1, s2)));

        JobDetails details = jobsService.submitDeleteTenantJob(tenantId, jobName).toBlocking().value();

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(jobDetails -> {
            logger.debug("Finished " + details);
            latch.countDown();
        });

        jobScheduler.advanceTimeTo(details.getTrigger().getTriggerTime());

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        assertDataEmpty(s1, start, start.plusMinutes(3));
        assertDataEmpty(s2, start, start.plusMinutes(3));
    }

    @Test
    public void deleteTenantTwiceConcurrently() throws Exception {
        String tenantId = nextTenantId();
        DateTime start = new DateTime(jobScheduler.now());

        Metric<Double> g1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G1"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));
        Metric<Double> g2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "G2"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2)));

        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(g1, g2)));

        Metric<Long> c1 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>(start.getMillis(), 10L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 20L)));
        Metric<Long> c2 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C2"), asList(
                new DataPoint<>(start.getMillis(), 10L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 20L)));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(c1, c2)));

        Metric<AvailabilityType> a1 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "A1"), asList(
                new DataPoint<>(start.getMillis(), UP),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN)));
        Metric<AvailabilityType> a2 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "A2"), asList(
                new DataPoint<>(start.getMillis(), UP),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN)));

        doAction(() -> metricsService.addDataPoints(AVAILABILITY, Observable.just(a1, a2)));

        Metric<String> s1 = new Metric<>(new MetricId<>(tenantId, STRING, "S1"), asList(
                new DataPoint<>(start.getMillis(), "starting"),
                new DataPoint<>(start.plusMinutes(2).getMillis(), "stopping")));
        Metric<String> s2 = new Metric<>(new MetricId<>(tenantId, STRING, "S2"), asList(
                new DataPoint<>(start.getMillis(), "starting"),
                new DataPoint<>(start.plusMinutes(2).getMillis(), "stopping")));

        doAction(() -> metricsService.addDataPoints(STRING, Observable.just(s1, s2)));

        JobDetails details1 = jobsService.submitDeleteTenantJob(tenantId, jobName).toBlocking().value();
        JobDetails details2 = jobsService.submitDeleteTenantJob(tenantId, jobName).toBlocking().value();

        assertEquals(details1.getTrigger().getTriggerTime(), details2.getTrigger().getTriggerTime(),
                "The jobs should be scheduled to execute at the same time");

        CountDownLatch latch = new CountDownLatch(2);
        jobScheduler.onJobFinished(jobDetails -> {
            logger.debug("Finished " + jobDetails);
            latch.countDown();
        });

        jobScheduler.advanceTimeTo(details1.getTrigger().getTriggerTime());

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        assertDataEmpty(s1, start, start.plusMinutes(3));
        assertDataEmpty(s2, start, start.plusMinutes(3));
    }

    @Test
    public void deleteNonexistentTenant() throws Exception {
        String tenantId = nextTenantId();

        JobDetails details = jobsService.submitDeleteTenantJob(tenantId, jobName).toBlocking().value();

        CountDownLatch latch = new CountDownLatch(1);
        jobScheduler.onJobFinished(jobDetails -> {
            logger.debug("Finished " + details);
            latch.countDown();
        });

        jobScheduler.advanceTimeTo(details.getTrigger().getTriggerTime());

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private String nextTenantId() {
        return "T" + tenantCounter.getAndIncrement();
    }

    private <T> void assertDataEmpty(Metric<T> metric, DateTime start, DateTime end) {
        List<DataPoint<T>> data = getOnNextEvents(() -> metricsService.findDataPoints(metric.getMetricId(),
                start.getMillis(), end.getMillis(), 0, ASC));
        assertTrue(data.isEmpty(), "Did not expect to find data for " + metric.getId());
    }

    private void assertMetricTagIndexEmpty(String tenantId, String tagName) {
        ResultSet resultSet = session.execute(getTags.bind(tenantId, tagName));
        assertEquals(resultSet.all().size(), 0, "Expected metric tag index to be empty for tag [" + tagName + "]");
    }

    private <T> void assertRetentionsIndexEmpty(String tenantId, MetricType<T> type) {
        ResultSet resultSet = session.execute(getRetentions.bind(tenantId, type.getCode()));
        assertEquals(resultSet.all().size(), 0, "Expected retentions index to be empty for " + type.getText() +
                " metrics");
    }

}
