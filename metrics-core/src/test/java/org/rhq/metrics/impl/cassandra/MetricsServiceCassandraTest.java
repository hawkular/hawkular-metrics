/*
 * Copyright 2014 Red Hat, Inc.
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

package org.rhq.metrics.impl.cassandra;

import static java.util.Arrays.asList;
import static org.joda.time.DateTime.now;
import static org.joda.time.Days.days;
import static org.joda.time.Hours.hours;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.rhq.metrics.core.AvailabilityType.DOWN;
import static org.rhq.metrics.core.AvailabilityType.UP;
import static org.rhq.metrics.core.Metric.DPART;
import static org.rhq.metrics.core.MetricType.AVAILABILITY;
import static org.rhq.metrics.core.MetricType.NUMERIC;
import static org.rhq.metrics.impl.cassandra.MetricsServiceCassandra.DEFAULT_TTL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.rhq.metrics.core.Availability;
import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricAlreadyExistsException;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric;
import org.rhq.metrics.core.Retention;
import org.rhq.metrics.core.Tag;
import org.rhq.metrics.core.Tenant;
import org.rhq.metrics.test.MetricsTestUtils;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author John Sanda
 */
public class MetricsServiceCassandraTest {

    private static class Context extends ExternalResource {
        private MetricsServiceCassandra metricsService;
        private DataAccess dataAccess;
        private Session session;
        @Override
        protected void before() throws Throwable {
            session = MetricsTestUtils.getSession();
            metricsService = new MetricsServiceCassandra();
            metricsService.startUp(session);
            dataAccess = metricsService.getDataAccess();
        }

        @Override
        protected void after() {
            metricsService.shutdown();
        }

        public void resetDB() {
            session.execute("TRUNCATE tenants");
            session.execute("TRUNCATE data");
            session.execute("TRUNCATE tags");
            session.execute("TRUNCATE metrics_idx");
            session.execute("TRUNCATE retentions_idx");
            metricsService.setDataAccess(dataAccess);
        }

        public DataAccess getDataAccess() {
            return dataAccess;
        }

        public Session getSession() {
            return session;
        }

        public MetricsServiceCassandra getMetricsService() {
            return metricsService;
        }

    }

    @ClassRule
    public static Context context = new Context();

    @Before
    public void initMethod() {
        context.resetDB();
    }

    @Test
    public void createTenants() throws Exception {
        Tenant t1 = new Tenant().setId("t1").setRetention(NUMERIC, 24).setRetention(AVAILABILITY, 24);
        Tenant t2 = new Tenant().setId("t2").setRetention(NUMERIC, 72);
        Tenant t3 = new Tenant().setId("t3").setRetention(AVAILABILITY, 48);
        Tenant t4 = new Tenant().setId("t4");

        List<ListenableFuture<Void>> insertFutures = new ArrayList<>();
        insertFutures.add(context.getMetricsService().createTenant(t1));
        insertFutures.add(context.getMetricsService().createTenant(t2));
        insertFutures.add(context.getMetricsService().createTenant(t3));
        insertFutures.add(context.getMetricsService().createTenant(t4));
        ListenableFuture<List<Void>> insertsFuture = Futures.allAsList(insertFutures);
        MetricsTestUtils.getUninterruptibly(insertsFuture);

        Collection<Tenant> tenants = MetricsTestUtils.getUninterruptibly(context.getMetricsService().getTenants());
        Set<Tenant> actualTenants = ImmutableSet.copyOf(tenants);
        Set<Tenant> expectedTenants = ImmutableSet.of(t1, t2, t3, t4);

        for (Tenant expected : expectedTenants) {
            Tenant actual = null;
            for (Tenant t : actualTenants) {
                if (t.getId().equals(expected.getId())) {
                    actual = t;
                }
            }
            assertNotNull("Expected to find a tenant with id [" + expected.getId() + "]", actual);
            assertEquals("The tenant does not match", expected, actual);
        }

        assertDataRetentionsIndexMatches(t1.getId(), NUMERIC, ImmutableSet.of(new Retention(
            new MetricId("[" + NUMERIC.getText() + "]"), hours(24).toStandardSeconds().getSeconds())));
        assertDataRetentionsIndexMatches(t1.getId(), AVAILABILITY, ImmutableSet.of(new Retention(
            new MetricId("[" + AVAILABILITY.getText() + "]"), hours(24).toStandardSeconds().getSeconds())));
    }

    @Test
    public void createAndFindMetrics() throws Exception {
        NumericMetric m1 = new NumericMetric("t1", new MetricId("m1"), ImmutableMap.of("a1", "1", "a2", "2"), 24);
        ListenableFuture<Void> insertFuture = context.getMetricsService().createMetric(m1);
        MetricsTestUtils.getUninterruptibly(insertFuture);

        ListenableFuture<Metric> queryFuture = context.getMetricsService().findMetric(m1.getTenantId(), m1.getType(),
                m1.getId());
        Metric actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        assertEquals("The metric does not match the expected value", m1, actual);

        AvailabilityMetric m2 = new AvailabilityMetric("t1", new MetricId("m2"), ImmutableMap.of("a3", "3", "a4", "4"));
        insertFuture = context.getMetricsService().createMetric(m2);
        MetricsTestUtils.getUninterruptibly(insertFuture);

        queryFuture = context.getMetricsService().findMetric(m2.getTenantId(), m2.getType(), m2.getId());
        actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        assertEquals("The metric does not match the expected value", m2, actual);

        insertFuture = context.getMetricsService().createMetric(m1);
        Throwable exception = null;
        try {
            MetricsTestUtils.getUninterruptibly(insertFuture);
        } catch (Exception e) {
            exception = e.getCause();
        }
        assertTrue("Expected a " + MetricAlreadyExistsException.class.getSimpleName() + " to be thrown",
            exception != null && exception instanceof MetricAlreadyExistsException);

        NumericMetric m3 = new NumericMetric("t1", new MetricId("m3"));
        m3.setDataRetention(24);
        insertFuture = context.getMetricsService().createMetric(m3);
        MetricsTestUtils.getUninterruptibly(insertFuture);

        assertMetricIndexMatches("t1", NUMERIC, asList(m1, m3));
        assertMetricIndexMatches("t1", AVAILABILITY, asList(m2));

        assertDataRetentionsIndexMatches("t1", NUMERIC, ImmutableSet.of(new Retention(m3.getId(), 24),
            new Retention(m1.getId(), 24)));
    }

    @Test
    public void updateMetadata() throws Exception {
        NumericMetric metric = new NumericMetric("t1", new MetricId("m1"), ImmutableMap.of("a1", "1", "a2", "2"));
        ListenableFuture<Void> insertFuture = context.getMetricsService().createMetric(metric);
        MetricsTestUtils.getUninterruptibly(insertFuture);

        Map<String, String> additions = ImmutableMap.of("a2", "two", "a3", "3");
        Set<String> deletions = ImmutableSet.of("a1");
        insertFuture = context.getMetricsService().updateMetadata(metric, additions, deletions);
        MetricsTestUtils.getUninterruptibly(insertFuture);

        ListenableFuture<Metric> queryFuture = context.getMetricsService().findMetric(metric.getTenantId(), NUMERIC,
            metric.getId());
        Metric updatedMetric = MetricsTestUtils.getUninterruptibly(queryFuture);

        assertEquals("The updated meta data does not match the expected values",
                ImmutableMap.of("a2", "two", "a3", "3"), updatedMetric.getMetadata());

        assertMetricIndexMatches(metric.getTenantId(), NUMERIC, asList(updatedMetric));
    }

    @Test
    public void addAndFetchNumericData() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId("t1")));

        NumericMetric m1 = new NumericMetric("t1", new MetricId("m1"));
        m1.addData(start.getMillis(), 1.1);
        m1.addData(start.plusMinutes(2).getMillis(), 2.2);
        m1.addData(start.plusMinutes(4).getMillis(), 3.3);
        m1.addData(end.getMillis(), 4.4);

        ListenableFuture<Void> insertFuture = context.getMetricsService().addNumericData(asList(m1));
        MetricsTestUtils.getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> queryFuture = context.getMetricsService().findData(m1, start.getMillis(),
            end.getMillis());
        List<NumericData> actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        List<NumericData> expected = asList(
            new NumericData(m1, start.plusMinutes(4).getMillis(), 3.3),
            new NumericData(m1, start.plusMinutes(2).getMillis(), 2.2),
            new NumericData(m1, start.getMillis(), 1.1)
        );

        assertEquals("The data does not match the expected values", expected, actual);
        assertMetricIndexMatches("t1", NUMERIC, asList(m1));
    }

    @Test
    public void verifyTTLsSetOnNumericData() throws Exception {
        DateTime start = now().minusMinutes(10);

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId("t1")));
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId("t2")
            .setRetention(NUMERIC, days(14).toStandardHours().getHours())));

        VerifyTTLDataAccess verifyTTLDataAccess = new VerifyTTLDataAccess(context.getDataAccess());

        context.getMetricsService().unloadDataRetentions();
        context.getMetricsService().loadDataRetentions();
        context.getMetricsService().setDataAccess(verifyTTLDataAccess);

        NumericMetric m1 = new NumericMetric("t1", new MetricId("m1"));
        m1.addData(start.getMillis(), 1.01);
        m1.addData(start.plusMinutes(1).getMillis(), 1.02);
        m1.addData(start.plusMinutes(2).getMillis(), 1.03);

        addDataInThePast(m1, days(2).toStandardDuration());

        Set<String> tags = ImmutableSet.of("tag1");

        verifyTTLDataAccess.numericTagTTLLessThanEqualTo(DEFAULT_TTL - days(2).toStandardSeconds().getSeconds());
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().tagNumericData(m1, tags, start.getMillis(),
            start.plusMinutes(2).getMillis()));

        verifyTTLDataAccess.setNumericTTL(days(14).toStandardSeconds().getSeconds());
        NumericMetric m2 = new NumericMetric("t2", new MetricId("m2"));
        m2.addData(start.plusMinutes(5).getMillis(), 2.02);
        addDataInThePast(m2, days(3).toStandardDuration());

        verifyTTLDataAccess.numericTagTTLLessThanEqualTo(days(14).minus(3).toStandardSeconds().getSeconds());
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().tagNumericData(m2, tags,
                start.plusMinutes(5).getMillis()));

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId("t3")
            .setRetention(NUMERIC, 24)));
        verifyTTLDataAccess.setNumericTTL(hours(24).toStandardSeconds().getSeconds());
        NumericMetric m3 = new NumericMetric("t3", new MetricId("m3"));
        m3.addData(start.getMillis(), 3.03);
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().addNumericData(asList(m3)));

        NumericMetric m4 = new NumericMetric("t2", new MetricId("m4"), Collections.emptyMap(), 28);
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createMetric(m4));

        verifyTTLDataAccess.setNumericTTL(28);
        m4.addData(start.plusMinutes(3).getMillis(), 4.1);
        m4.addData(start.plusMinutes(4).getMillis(), 4.2);
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().addNumericData(asList(m4)));
    }

    @Test
    public void verifyTTLsSetOnAvailabilityData() throws Exception {
        DateTime start = now().minusMinutes(10);

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId("t1")));
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId("t2")
            .setRetention(AVAILABILITY, days(14).toStandardHours().getHours())));

        VerifyTTLDataAccess verifyTTLDataAccess = new VerifyTTLDataAccess(context.getDataAccess());

        context.getMetricsService().unloadDataRetentions();
        context.getMetricsService().loadDataRetentions();
        context.getMetricsService().setDataAccess(verifyTTLDataAccess);
        context.getMetricsService().setDataAccess(verifyTTLDataAccess);

        AvailabilityMetric m1 = new AvailabilityMetric("t1", new MetricId("m1"));
        m1.addData(new Availability(start.getMillis(), UP));
        m1.addData(new Availability(start.plusMinutes(1).getMillis(), DOWN));
        m1.addData(new Availability(start.plusMinutes(2).getMillis(), DOWN));
        addDataInThePast(m1, days(2).toStandardDuration());

        Set<String> tags = ImmutableSet.of("tag1");

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(DEFAULT_TTL - days(2).toStandardSeconds().getSeconds());
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().tagAvailabilityData(m1, tags, start.getMillis(),
            start.plusMinutes(2).getMillis()));

        verifyTTLDataAccess.setAvailabilityTTL(days(14).toStandardSeconds().getSeconds());
        AvailabilityMetric m2 = new AvailabilityMetric("t2", new MetricId("m2"));
        m2.addData(new Availability(start.plusMinutes(5).getMillis(), UP));
        addDataInThePast(m2, days(5).toStandardDuration());

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(days(14).minus(5).toStandardSeconds().getSeconds());
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().tagAvailabilityData(m2, tags,
                start.plusMinutes(5).getMillis()));

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId("t3")
            .setRetention(AVAILABILITY, 24)));
        verifyTTLDataAccess.setAvailabilityTTL(hours(24).toStandardSeconds().getSeconds());
        AvailabilityMetric m3 = new AvailabilityMetric("t3", new MetricId("m3"));
        m3.addData(new Availability(start.getMillis(), UP));
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().addAvailabilityData(asList(m3)));
    }

    private void addDataInThePast(NumericMetric metric, final Duration duration) throws Exception {
        DataAccess originalDataAccess = context.getMetricsService().getDataAccess();
        final PreparedStatement insertNumericDataWithTimestamp = context.getSession().prepare(
                "INSERT INTO data (tenant_id, type, metric, interval, dpart, time, n_value) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?) " + "USING TTL ? AND TIMESTAMP ?");

        try {
            context.getMetricsService().setDataAccess(new DelegatingDataAccess(context.getDataAccess()) {
                @Override
                public ResultSetFuture insertData(NumericMetric m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (NumericData d : m.getData()) {
                        batchStatement.add(insertNumericDataWithTimestamp.bind(m.getTenantId(), NUMERIC.getCode(),
                                m.getId().getName(), m.getId().getInterval().toString(), DPART, d.getTimeUUID(),
                                d.getValue(), actualTTL, writeTime));
                    }
                    return context.getSession().executeAsync(batchStatement);
                }
            });
            context.getMetricsService().addNumericData(asList(metric));
        } finally {
            context.getMetricsService().setDataAccess(originalDataAccess);
        }
    }

    private void addDataInThePast(AvailabilityMetric metric, final Duration duration) throws Exception {
        DataAccess originalDataAccess = context.getMetricsService().getDataAccess();
        final PreparedStatement insertAvailabilityDateWithTimestamp = context
                .getSession().prepare("INSERT INTO data (tenant_id, type, metric, interval, dpart, time, availability) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?) " + "USING TTL ? AND TIMESTAMP ?");
        try {
            context.getMetricsService().setDataAccess(new DelegatingDataAccess(context.getDataAccess()) {
                @Override
                public ResultSetFuture insertData(AvailabilityMetric m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (Availability a : m.getData()) {
                        batchStatement.add(insertAvailabilityDateWithTimestamp.bind(m.getTenantId(),
                            AVAILABILITY.getCode(), m.getId().getName(), m.getId().getInterval().toString(), DPART,
                            a.getTimeUUID(), a.getBytes(), actualTTL, writeTime));
                    }
                    return context.getSession().executeAsync(batchStatement);
                }
            });
            context.getMetricsService().addAvailabilityData(asList(metric));
        } finally {
            context.getMetricsService().setDataAccess(originalDataAccess);
        }
    }

    @Test
    public void fetchNumericDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId("tenant1")));

        NumericMetric metric = new NumericMetric("tenant1", new MetricId("m1"));
        metric.addData(start.getMillis(), 100.0);
        metric.addData(start.plusMinutes(1).getMillis(), 101.1);
        metric.addData(start.plusMinutes(2).getMillis(), 102.2);
        metric.addData(start.plusMinutes(3).getMillis(), 103.3);
        metric.addData(start.plusMinutes(4).getMillis(), 104.4);
        metric.addData(start.plusMinutes(5).getMillis(), 105.5);
        metric.addData(start.plusMinutes(6).getMillis(), 106.6);

        ListenableFuture<Void> insertFuture = context.getMetricsService().addNumericData(asList(metric));
        MetricsTestUtils.getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> tagFuture = context.getMetricsService().tagNumericData(metric,
            ImmutableSet.of("t1", "t2"), start.plusMinutes(2).getMillis());
        MetricsTestUtils.getUninterruptibly(tagFuture);

        tagFuture = context.getMetricsService().tagNumericData(metric, ImmutableSet.of("t3", "t4"),
                start.plusMinutes(3).getMillis(), start.plusMinutes(5).getMillis());
        MetricsTestUtils.getUninterruptibly(tagFuture);

        ListenableFuture<List<NumericData>> queryFuture = context.getMetricsService().findData(metric,
                start.getMillis(), end.getMillis());
        List<NumericData> actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        List<NumericData> expected = asList(
            new NumericData(metric, start.plusMinutes(6).getMillis(), 106.6),
            new NumericData(metric, start.plusMinutes(5).getMillis(), 105.5),
            new NumericData(metric, start.plusMinutes(4).getMillis(), 104.4),
            new NumericData(metric, start.plusMinutes(3).getMillis(), 103.3),
            new NumericData(metric, start.plusMinutes(2).getMillis(), 102.2),
            new NumericData(metric, start.plusMinutes(1).getMillis(), 101.1),
            new NumericData(metric, start.getMillis(), 100.0)
        );

        assertEquals("The data does not match the expected values", expected, actual);
        assertEquals("The tags do not match", ImmutableSet.of(new Tag("t3"), new Tag("t4")), actual.get(3).getTags());
        assertEquals("The tags do not match", ImmutableSet.of(new Tag("t3"), new Tag("t4")), actual.get(2).getTags());
        assertEquals("The tags do not match", ImmutableSet.of(new Tag("t3"), new Tag("t4")), actual.get(2).getTags());
        assertEquals("The tags do not match", ImmutableSet.of(new Tag("t1"), new Tag("t2")), actual.get(4).getTags());
    }

    @Test
    public void addNumericDataForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId(tenantId)));

        NumericMetric m1 = new NumericMetric(tenantId, new MetricId("m1"));
        m1.addData(start.plusSeconds(30).getMillis(), 11.2);
        m1.addData(start.getMillis(), 11.1);

        NumericMetric m2 = new NumericMetric(tenantId, new MetricId("m2"));
        m2.addData(start.plusSeconds(30).getMillis(), 12.2);
        m2.addData(start.getMillis(), 12.1);

        NumericMetric m3 = new NumericMetric(tenantId, new MetricId("m3"));

        NumericMetric m4 = new NumericMetric(tenantId, new MetricId("m4"), Collections.emptyMap(), 24);
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createMetric(m4));
        m4.addData(start.plusSeconds(30).getMillis(), 55.5);
        m4.addData(end.getMillis(), 66.6);

        ListenableFuture<Void> insertFuture = context.getMetricsService().addNumericData(asList(m1, m2, m3, m4));
        MetricsTestUtils.getUninterruptibly(insertFuture);

        ListenableFuture<NumericMetric> queryFuture = context.getMetricsService().findNumericData(m1,
                start.getMillis(), end.getMillis());
        NumericMetric actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        assertMetricEquals(m1, actual);

        queryFuture = context.getMetricsService().findNumericData(m2, start.getMillis(), end.getMillis());
        actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        assertMetricEquals(m2, actual);

        queryFuture = context.getMetricsService().findNumericData(m3, start.getMillis(), end.getMillis());
        actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        assertNull("Did not expect to get back results since there is no data for " + m3, actual);

        queryFuture = context.getMetricsService().findNumericData(m4, start.getMillis(), end.getMillis());
        actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        NumericMetric expected = new NumericMetric(tenantId, new MetricId("m4"));
        expected.setDataRetention(24);
        expected.addData(start.plusSeconds(30).getMillis(), 55.5);
        assertMetricEquals(expected, actual);

        assertMetricIndexMatches(tenantId, NUMERIC, asList(m1, m2, m3, m4));
    }

    @Test
    public void addAvailabilityForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId(tenantId)));

        AvailabilityMetric m1 = new AvailabilityMetric(tenantId, new MetricId("m1"));
        m1.addData(new Availability(m1, start.plusSeconds(20).getMillis(), "down"));
        m1.addData(new Availability(m1, start.plusSeconds(10).getMillis(), "up"));

        AvailabilityMetric m2 = new AvailabilityMetric(tenantId, new MetricId("m2"));
        m2.addData(new Availability(m2, start.plusSeconds(30).getMillis(), "up"));
        m2.addData(new Availability(m2, start.plusSeconds(15).getMillis(), "down"));

        AvailabilityMetric m3 = new AvailabilityMetric(tenantId, new MetricId("m3"));

        ListenableFuture<Void> insertFuture = context.getMetricsService().addAvailabilityData(asList(m1, m2, m3));
        MetricsTestUtils.getUninterruptibly(insertFuture);

        ListenableFuture<AvailabilityMetric> queryFuture = context.getMetricsService().findAvailabilityData(m1,
                start.getMillis(), end.getMillis());
        AvailabilityMetric actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        assertMetricEquals(m1, actual);

        queryFuture = context.getMetricsService().findAvailabilityData(m2, start.getMillis(), end.getMillis());
        actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        assertMetricEquals(m2, actual);

        queryFuture = context.getMetricsService().findAvailabilityData(m3, start.getMillis(), end.getMillis());
        actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        assertNull("Did not expect to get back results since there is no data for " + m3, actual);

        AvailabilityMetric m4 = new AvailabilityMetric(tenantId, new MetricId("m4"), Collections.emptyMap(), 24);
        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createMetric(m4));
        m4.addData(new Availability(start.plusMinutes(2).getMillis(), UP));
        m4.addData(new Availability(end.plusMinutes(2).getMillis(), UP));

        insertFuture = context.getMetricsService().addAvailabilityData(asList(m4));
        MetricsTestUtils.getUninterruptibly(insertFuture);

        queryFuture = context.getMetricsService().findAvailabilityData(m4, start.getMillis(), end.getMillis());
        actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        AvailabilityMetric expected = new AvailabilityMetric(tenantId, m4.getId(), Collections.emptyMap(), 24);
        expected.addData(new Availability(start.plusMinutes(2).getMillis(), UP));
        assertMetricEquals(expected, actual);

        assertMetricIndexMatches(tenantId, AVAILABILITY, asList(m1, m2, m3, m4));
    }

    @Test
    public void fetchAvailabilityDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId("tenant1")));

        AvailabilityMetric metric = new AvailabilityMetric("tenant1", new MetricId("A1"));
        metric.addAvailability(start.getMillis(), UP);
        metric.addAvailability(start.plusMinutes(1).getMillis(), DOWN);
        metric.addAvailability(start.plusMinutes(2).getMillis(), DOWN);
        metric.addAvailability(start.plusMinutes(3).getMillis(), UP);
        metric.addAvailability(start.plusMinutes(4).getMillis(), DOWN);
        metric.addAvailability(start.plusMinutes(5).getMillis(), UP);
        metric.addAvailability(start.plusMinutes(6).getMillis(), UP);

        ListenableFuture<Void> insertFuture = context.getMetricsService().addAvailabilityData(asList(metric));
        MetricsTestUtils.getUninterruptibly(insertFuture);

        ListenableFuture<List<Availability>> tagFuture = context.getMetricsService().tagAvailabilityData(metric,
            ImmutableSet.of("t1", "t2"), start.plusMinutes(2).getMillis());
        MetricsTestUtils.getUninterruptibly(tagFuture);

        tagFuture = context.getMetricsService().tagAvailabilityData(metric, ImmutableSet.of("t3", "t4"),
            start.plusMinutes(3).getMillis(), start.plusMinutes(5).getMillis());
        MetricsTestUtils.getUninterruptibly(tagFuture);

        ListenableFuture<AvailabilityMetric> queryFuture = context.getMetricsService().findAvailabilityData(metric,
                start.getMillis(), end.getMillis());
        AvailabilityMetric actualMetric = MetricsTestUtils.getUninterruptibly(queryFuture);
        List<Availability> actual = actualMetric.getData();
        List<Availability> expected = asList(
            new Availability(metric, start.plusMinutes(6).getMillis(), UP),
            new Availability(metric, start.plusMinutes(5).getMillis(), UP),
            new Availability(metric, start.plusMinutes(4).getMillis(), DOWN),
            new Availability(metric, start.plusMinutes(3).getMillis(), UP),
            new Availability(metric, start.plusMinutes(2).getMillis(), DOWN),
            new Availability(metric, start.plusMinutes(1).getMillis(), DOWN),
            new Availability(metric, start.getMillis(), UP)
        );

        assertEquals("The data does not match the expected values", expected, actual);
        assertEquals("The tags do not match", ImmutableSet.of(new Tag("t3"), new Tag("t4")), actual.get(3).getTags());
        assertEquals("The tags do not match", ImmutableSet.of(new Tag("t3"), new Tag("t4")), actual.get(2).getTags());
        assertEquals("The tags do not match", ImmutableSet.of(new Tag("t3"), new Tag("t4")), actual.get(2).getTags());
        assertEquals("The tags do not match", ImmutableSet.of(new Tag("t1"), new Tag("t2")), actual.get(4).getTags());
    }

    @Test
    public void tagNumericDataByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId(tenant)));

        NumericData d1 = new NumericData(start.getMillis(), 101.1);
        NumericData d2 = new NumericData(start.plusMinutes(2).getMillis(), 101.2);
        NumericData d3 = new NumericData(start.plusMinutes(6).getMillis(), 102.2);
        NumericData d4 = new NumericData(start.plusMinutes(8).getMillis(), 102.3);
        NumericData d5 = new NumericData(start.plusMinutes(4).getMillis(), 102.1);
        NumericData d6 = new NumericData(start.plusMinutes(4).getMillis(), 101.4);
        NumericData d7 = new NumericData(start.plusMinutes(10).getMillis(), 102.4);
        NumericData d8 = new NumericData(start.plusMinutes(6).getMillis(), 103.1);
        NumericData d9 = new NumericData(start.plusMinutes(7).getMillis(), 103.1);

        NumericMetric m1 = new NumericMetric(tenant, new MetricId("m1"));
        m1.addData(d1);
        m1.addData(d2);
        m1.addData(d6);

        NumericMetric m2 = new NumericMetric(tenant, new MetricId("m2"));
        m2.addData(d3);
        m2.addData(d4);
        m2.addData(d5);
        m2.addData(d7);

        NumericMetric m3 = new NumericMetric(tenant, new MetricId("m3"));
        m3.addData(d8);
        m3.addData(d9);

        ListenableFuture<Void> insertFuture = context.getMetricsService().addNumericData(asList(m1, m2, m3));
        MetricsTestUtils.getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> tagFuture1 = context.getMetricsService().tagNumericData(m1,
                ImmutableSet.of("t1"), start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<NumericData>> tagFuture2 = context.getMetricsService().tagNumericData(m2,
                ImmutableSet.of("t1"), start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<NumericData>> tagFuture3 = context.getMetricsService().tagNumericData(m1,
                ImmutableSet.of("t2"), start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<NumericData>> tagFuture4 = context.getMetricsService().tagNumericData(m2,
                ImmutableSet.of("t2"), start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<NumericData>> tagFuture5 = context.getMetricsService().tagNumericData(m3,
                ImmutableSet.of("t2"), start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());

        MetricsTestUtils.getUninterruptibly(tagFuture1);
        MetricsTestUtils.getUninterruptibly(tagFuture2);
        MetricsTestUtils.getUninterruptibly(tagFuture3);
        MetricsTestUtils.getUninterruptibly(tagFuture4);
        MetricsTestUtils.getUninterruptibly(tagFuture5);

        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = context.getMetricsService()
                .findNumericDataByTags(tenant, ImmutableSet.of("t1", "t2"));
        Map<MetricId, Set<NumericData>> actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<NumericData>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(d1, d2, d6),
            new MetricId("m2"), ImmutableSet.of(d5, d3)
        );

        assertEquals("The tagged data does not match", expected, actual);
    }

    @Test
    public void tagAvailabilityByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId(tenant)));

        AvailabilityMetric m1 = new AvailabilityMetric(tenant, new MetricId("m1"));
        AvailabilityMetric m2 = new AvailabilityMetric(tenant, new MetricId("m2"));
        AvailabilityMetric m3 = new AvailabilityMetric(tenant, new MetricId("m3"));

        Availability a1 = new Availability(m1, start.getMillis(), UP);
        Availability a2 = new Availability(m1, start.plusMinutes(2).getMillis(), UP);
        Availability a3 = new Availability(m2, start.plusMinutes(6).getMillis(), DOWN);
        Availability a4 = new Availability(m2, start.plusMinutes(8).getMillis(), DOWN);
        Availability a5 = new Availability(m2, start.plusMinutes(4).getMillis(), UP);
        Availability a6 = new Availability(m1, start.plusMinutes(4).getMillis(), DOWN);
        Availability a7 = new Availability(m2, start.plusMinutes(10).getMillis(), UP);
        Availability a8 = new Availability(m3, start.plusMinutes(6).getMillis(), DOWN);
        Availability a9 = new Availability(m3, start.plusMinutes(7).getMillis(), UP);

        m1.addData(a1);
        m1.addData(a2);
        m1.addData(a6);

        m2.addData(a3);
        m2.addData(a4);
        m2.addData(a5);
        m2.addData(a7);

        m3.addData(a8);
        m3.addData(a9);

        ListenableFuture<Void> insertFuture = context.getMetricsService().addAvailabilityData(asList(m1, m2, m3));
        MetricsTestUtils.getUninterruptibly(insertFuture);

        ListenableFuture<List<Availability>> tagFuture1 = context.getMetricsService().tagAvailabilityData(m1,
                ImmutableSet.of("t1"), start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<Availability>> tagFuture2 = context.getMetricsService().tagAvailabilityData(m2,
                ImmutableSet.of("t1"), start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<Availability>> tagFuture3 = context.getMetricsService().tagAvailabilityData(m1,
                ImmutableSet.of("t2"), start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<Availability>> tagFuture4 = context.getMetricsService().tagAvailabilityData(m2,
                ImmutableSet.of("t2"), start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<Availability>> tagFuture5 = context.getMetricsService().tagAvailabilityData(m3,
                ImmutableSet.of("t2"), start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());

        MetricsTestUtils.getUninterruptibly(tagFuture1);
        MetricsTestUtils.getUninterruptibly(tagFuture2);
        MetricsTestUtils.getUninterruptibly(tagFuture3);
        MetricsTestUtils.getUninterruptibly(tagFuture4);
        MetricsTestUtils.getUninterruptibly(tagFuture5);

        ListenableFuture<Map<MetricId, Set<Availability>>> queryFuture = context.getMetricsService()
                .findAvailabilityByTags(tenant, ImmutableSet.of("t1", "t2"));
        Map<MetricId, Set<Availability>> actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<Availability>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(a1, a2, a6),
            new MetricId("m2"), ImmutableSet.of(a5, a3)
        );

        assertEquals("The tagged data does not match", expected, actual);
    }

    @Test
    public void tagIndividualNumericDataPoints() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId(tenant)));

        NumericData d1 = new NumericData(start.getMillis(), 101.1);
        NumericData d2 = new NumericData(start.plusMinutes(2).getMillis(), 101.2);
        NumericData d3 = new NumericData(start.plusMinutes(6).getMillis(), 102.2);
        NumericData d4 = new NumericData(start.plusMinutes(8).getMillis(), 102.3);
        NumericData d5 = new NumericData(start.plusMinutes(4).getMillis(), 102.1);
        NumericData d6 = new NumericData(start.plusMinutes(4).getMillis(), 101.4);
        NumericData d7 = new NumericData(start.plusMinutes(10).getMillis(), 102.4);
        NumericData d8 = new NumericData(start.plusMinutes(6).getMillis(), 103.1);
        NumericData d9 = new NumericData(start.plusMinutes(7).getMillis(), 103.1);

        NumericMetric m1 = new NumericMetric(tenant, new MetricId("m1"));
        m1.addData(d1);
        m1.addData(d2);
        m1.addData(d6);

        NumericMetric m2 = new NumericMetric(tenant, new MetricId("m2"));
        m2.addData(d3);
        m2.addData(d4);
        m2.addData(d5);
        m2.addData(d7);

        NumericMetric m3 = new NumericMetric(tenant, new MetricId("m3"));
        m3.addData(d8);
        m3.addData(d9);


        ListenableFuture<Void> insertFuture = context.getMetricsService().addNumericData(asList(m1, m2, m3));
        MetricsTestUtils.getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> tagFuture = context.getMetricsService().tagNumericData(m1,
                ImmutableSet.of("t1"), d1.getTimestamp());
        assertEquals("Tagging " + d1 + " returned unexpected results", asList(d1),
                MetricsTestUtils.getUninterruptibly(tagFuture));

        tagFuture = context.getMetricsService()
                .tagNumericData(m1, ImmutableSet.of("t1", "t2", "t3"), d2.getTimestamp());
        assertEquals("Tagging " + d2 + " returned unexpected results", asList(d2),
                MetricsTestUtils.getUninterruptibly(tagFuture));

        tagFuture = context.getMetricsService().tagNumericData(m1, ImmutableSet.of("t1"),
                start.minusMinutes(10).getMillis());
        assertEquals("No data should be returned since there is no data for this time", Collections.emptyList(),
                MetricsTestUtils.getUninterruptibly(tagFuture));

        tagFuture = context.getMetricsService().tagNumericData(m2, ImmutableSet.of("t2", "t3"), d3.getTimestamp());
        assertEquals("Tagging " + d3 + " returned unexpected results", asList(d3),
                MetricsTestUtils.getUninterruptibly(tagFuture));

        tagFuture = context.getMetricsService().tagNumericData(m2, ImmutableSet.of("t3", "t4"), d4.getTimestamp());
        assertEquals("Tagging " + d4 + " returned unexpected results", asList(d4),
                MetricsTestUtils.getUninterruptibly(tagFuture));

        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = context.getMetricsService()
                .findNumericDataByTags(tenant, ImmutableSet.of("t2", "t3"));
        Map<MetricId, Set<NumericData>> actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<NumericData>> expected = ImmutableMap.of(new MetricId("m1"),
                ImmutableSet.of(d2), new MetricId("m2"), ImmutableSet.of(d3, d4));

        assertEquals("The tagged data does not match", expected, actual);
    }

    @Test
    public void tagIndividualAvailabilityDataPoints() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        MetricsTestUtils.getUninterruptibly(context.getMetricsService().createTenant(new Tenant().setId(tenant)));

        AvailabilityMetric m1 = new AvailabilityMetric(tenant, new MetricId("m1"));
        AvailabilityMetric m2 = new AvailabilityMetric(tenant, new MetricId("m2"));
        AvailabilityMetric m3 = new AvailabilityMetric(tenant, new MetricId("m3"));

        Availability a1 = new Availability(m1, start.getMillis(), UP);
        Availability a2 = new Availability(m1, start.plusMinutes(2).getMillis(), UP);
        Availability a3 = new Availability(m2, start.plusMinutes(6).getMillis(), DOWN);
        Availability a4 = new Availability(m2, start.plusMinutes(8).getMillis(), DOWN);
        Availability a5 = new Availability(m2, start.plusMinutes(4).getMillis(), UP);
        Availability a6 = new Availability(m1, start.plusMinutes(4).getMillis(), DOWN);
        Availability a7 = new Availability(m2, start.plusMinutes(10).getMillis(), UP);
        Availability a8 = new Availability(m3, start.plusMinutes(6).getMillis(), DOWN);
        Availability a9 = new Availability(m3, start.plusMinutes(7).getMillis(), UP);

        m1.addData(a1);
        m1.addData(a2);
        m1.addData(a6);

        m2.addData(a3);
        m2.addData(a4);
        m2.addData(a5);
        m2.addData(a7);

        m3.addData(a8);
        m3.addData(a9);

        ListenableFuture<Void> insertFuture = context.getMetricsService().addAvailabilityData(asList(m1, m2, m3));
        MetricsTestUtils.getUninterruptibly(insertFuture);

        ListenableFuture<List<Availability>> tagFuture = context.getMetricsService().tagAvailabilityData(m1,
                ImmutableSet.of("t1"), a1.getTimestamp());
        assertEquals("Tagging " + a1 + " returned unexpected results", asList(a1),
                MetricsTestUtils.getUninterruptibly(tagFuture));

        tagFuture = context.getMetricsService().tagAvailabilityData(m1, ImmutableSet.of("t1", "t2", "t3"),
                a2.getTimestamp());
        assertEquals("Tagging " + a2 + " returned unexpected results", asList(a2),
                MetricsTestUtils.getUninterruptibly(tagFuture));

        tagFuture = context.getMetricsService().tagAvailabilityData(m1, ImmutableSet.of("t1"),
                start.minusMinutes(10).getMillis());
        assertEquals("No data should be returned since there is no data for this time", Collections.emptyList(),
                MetricsTestUtils.getUninterruptibly(tagFuture));

        tagFuture = context.getMetricsService().tagAvailabilityData(m2, ImmutableSet.of("t2", "t3"), a3.getTimestamp());
        assertEquals("Tagging " + a3 + " returned unexpected results", asList(a3),
                MetricsTestUtils.getUninterruptibly(tagFuture));

        tagFuture = context.getMetricsService().tagAvailabilityData(m2, ImmutableSet.of("t3", "t4"), a4.getTimestamp());
        assertEquals("Tagging " + a4 + " returned unexpected results", asList(a4),
                MetricsTestUtils.getUninterruptibly(tagFuture));

        ListenableFuture<Map<MetricId, Set<Availability>>> queryFuture = context.getMetricsService()
                .findAvailabilityByTags(tenant, ImmutableSet.of("t2", "t3"));
        Map<MetricId, Set<Availability>> actual = MetricsTestUtils.getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<Availability>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(a2),
            new MetricId("m2"), ImmutableSet.of(a3, a4)
        );

        assertEquals("The tagged data does not match", expected, actual);
    }

    private void assertMetricEquals(Metric expected, Metric actual) {
        assertEquals("The metric doe not match the expected value", expected, actual);
        assertEquals("The data does not match the expected values", expected.getData(), actual.getData());
    }

    private void assertMetricIndexMatches(String tenantId, MetricType type, List<? extends Metric> expected)
        throws Exception {
        ListenableFuture<List<Metric>> metricsFuture = context.getMetricsService().findMetrics(tenantId, type);
        List<Metric> actualIndex = MetricsTestUtils.getUninterruptibly(metricsFuture);

        assertEquals("The metrics index results do not match", expected, actualIndex);
    }

    private void assertDataRetentionsIndexMatches(String tenantId, MetricType type, Set<Retention> expected)
        throws Exception {
        ResultSetFuture queryFuture = context.getDataAccess().findDataRetentions(tenantId, type);
        ListenableFuture<Set<Retention>> retentionsFuture = Futures.transform(queryFuture, new DataRetentionsMapper());
        Set<Retention> actual = MetricsTestUtils.getUninterruptibly(retentionsFuture);

        assertEquals("The data retentions are wrong", expected, actual);
    }

    private static class VerifyTTLDataAccess extends DelegatingDataAccess {

        private int numericTTL;

        private int numericTagTTL;

        private int availabilityTTL;

        private int availabilityTagTTL;

        public VerifyTTLDataAccess(DataAccess instance) {
            super(instance);
            numericTTL = DEFAULT_TTL;
            numericTagTTL = DEFAULT_TTL;
            availabilityTTL = DEFAULT_TTL;
            availabilityTagTTL = DEFAULT_TTL;
        }

        public void setNumericTTL(int expectedTTL) {
            this.numericTTL = expectedTTL;
        }

        public void numericTagTTLLessThanEqualTo(int numericTagTTL) {
            this.numericTagTTL = numericTagTTL;
        }

        public void setAvailabilityTTL(int availabilityTTL) {
            this.availabilityTTL = availabilityTTL;
        }

        public void availabilityTagTLLLessThanEqualTo(int availabilityTagTTL) {
            this.availabilityTagTTL = availabilityTagTTL;
        }

        @Override
        public ResultSetFuture insertData(NumericMetric metric, int ttl) {
            assertEquals("The numeric data TTL does not match the expected value when " +
                "inserting data", numericTTL, ttl);
            return super.insertData(metric, ttl);
        }

        @Override
        public ResultSetFuture insertData(AvailabilityMetric metric, int ttl) {
            assertEquals("The availability data TTL does not match the expected value when " +
                "inserting data", availabilityTTL, ttl);
            return super.insertData(metric, ttl);
        }

        @Override
        public ResultSetFuture insertNumericTag(String tag, List<NumericData> data) {
            for (NumericData d : data) {
                assertTrue("Expected the TTL to be <= " + numericTagTTL +
                    " but it was " + d.getTTL(), d.getTTL() <= numericTagTTL);
            }
            return super.insertNumericTag(tag, data);
        }

        @Override
        public ResultSetFuture insertAvailabilityTag(String tag, List<Availability> data) {
            for (Availability a : data) {
                assertTrue("Expected the TTL to be <= " + availabilityTagTTL +
                    " but it was " + a.getTTL(), a.getTTL() <= availabilityTagTTL);
            }
            return super.insertAvailabilityTag(tag, data);
        }
    }

}
