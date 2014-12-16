package org.rhq.metrics.impl.cassandra;

import static java.util.Arrays.asList;
import static org.joda.time.DateTime.now;
import static org.joda.time.Days.days;
import static org.joda.time.Hours.hours;
import static org.rhq.metrics.core.AvailabilityType.DOWN;
import static org.rhq.metrics.core.AvailabilityType.UP;
import static org.rhq.metrics.core.Metric.DPART;
import static org.rhq.metrics.core.MetricType.AVAILABILITY;
import static org.rhq.metrics.core.MetricType.NUMERIC;
import static org.rhq.metrics.impl.cassandra.MetricsServiceCassandra.DEFAULT_TTL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.metrics.core.Availability;
import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricAlreadyExistsException;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric2;
import org.rhq.metrics.core.Retention;
import org.rhq.metrics.core.Tag;
import org.rhq.metrics.core.Tenant;
import org.rhq.metrics.test.MetricsTest;

/**
 * @author John Sanda
 */
public class MetricsServiceCassandraTest extends MetricsTest {

    private MetricsServiceCassandra metricsService;

    private DataAccess dataAccess;

    private PreparedStatement insertNumericDataWithTimestamp;

    private PreparedStatement insertAvailabilityDateWithTimestamp;

    @BeforeClass
    public void initClass() {
        initSession();
        metricsService = new MetricsServiceCassandra();
        metricsService.startUp(session);
        dataAccess = metricsService.getDataAccess();

        insertNumericDataWithTimestamp = session.prepare(
            "INSERT INTO data (tenant_id, type, metric, interval, dpart, time, n_value) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?) " +
            "USING TTL ? AND TIMESTAMP ?");

        insertAvailabilityDateWithTimestamp = session.prepare(
            "INSERT INTO data (tenant_id, type, metric, interval, dpart, time, availability) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?) " +
            "USING TTL ? AND TIMESTAMP ?"
        );
    }

    @BeforeMethod
    public void initMethod() {
        session.execute("TRUNCATE tenants");
        session.execute("TRUNCATE data");
        session.execute("TRUNCATE tags");
        session.execute("TRUNCATE metrics_idx");
        session.execute("TRUNCATE retentions_idx");
        metricsService.setDataAccess(dataAccess);
    }

    @Test
    public void createTenants() throws Exception {
        Tenant t1 = new Tenant().setId("t1").setRetention(NUMERIC, 24).setRetention(AVAILABILITY, 24);
        Tenant t2 = new Tenant().setId("t2").setRetention(NUMERIC, 72);
        Tenant t3 = new Tenant().setId("t3").setRetention(AVAILABILITY, 48);
        Tenant t4 = new Tenant().setId("t4");

        List<ListenableFuture<Void>> insertFutures = new ArrayList<>();
        insertFutures.add(metricsService.createTenant(t1));
        insertFutures.add(metricsService.createTenant(t2));
        insertFutures.add(metricsService.createTenant(t3));
        insertFutures.add(metricsService.createTenant(t4));
        ListenableFuture<List<Void>> insertsFuture = Futures.allAsList(insertFutures);
        getUninterruptibly(insertsFuture);

        Collection<Tenant> tenants = getUninterruptibly(metricsService.getTenants());
        Set<Tenant> actualTenants = ImmutableSet.copyOf(tenants);
        Set<Tenant> expectedTenants = ImmutableSet.of(t1, t2, t3, t4);

        for (Tenant expected : expectedTenants) {
            Tenant actual = null;
            for (Tenant t : actualTenants) {
                if (t.getId().equals(expected.getId())) {
                    actual = t;
                }
            }
            assertNotNull(actual, "Expected to find a tenant with id [" + expected.getId() + "]");
            assertEquals(actual, expected, "The tenant does not match");
        }

        assertDataRetentionsIndexMatches(t1.getId(), NUMERIC, ImmutableSet.of(new Retention(
            new MetricId("[" + NUMERIC.getText() + "]"), hours(24).toStandardSeconds().getSeconds())));
        assertDataRetentionsIndexMatches(t1.getId(), AVAILABILITY, ImmutableSet.of(new Retention(
            new MetricId("[" + AVAILABILITY.getText() + "]"), hours(24).toStandardSeconds().getSeconds())));
    }

    @Test
    public void createAndFindMetrics() throws Exception {
        NumericMetric2 m1 = new NumericMetric2("t1", new MetricId("m1"), ImmutableMap.of("a1", "1", "a2", "2"), 24);
        ListenableFuture<Void> insertFuture = metricsService.createMetric(m1);
        getUninterruptibly(insertFuture);

        ListenableFuture<Metric> queryFuture = metricsService.findMetric(m1.getTenantId(), m1.getType(), m1.getId());
        Metric actual = getUninterruptibly(queryFuture);
        assertEquals(actual, m1, "The metric does not match the expected value");

        AvailabilityMetric m2 = new AvailabilityMetric("t1", new MetricId("m2"), ImmutableMap.of("a3", "3", "a4", "4"));
        insertFuture = metricsService.createMetric(m2);
        getUninterruptibly(insertFuture);

        queryFuture = metricsService.findMetric(m2.getTenantId(), m2.getType(), m2.getId());
        actual = getUninterruptibly(queryFuture);
        assertEquals(actual, m2, "The metric does not match the expected value");

        insertFuture = metricsService.createMetric(m1);
        Throwable exception = null;
        try {
            getUninterruptibly(insertFuture);
        } catch (Exception e) {
            exception = e.getCause();
        }
        assertTrue(exception != null && exception instanceof MetricAlreadyExistsException,
            "Expected a " + MetricAlreadyExistsException.class.getSimpleName() + " to be thrown");

        NumericMetric2 m3 = new NumericMetric2("t1", new MetricId("m3"));
        m3.setDataRetention(24);
        insertFuture = metricsService.createMetric(m3);
        getUninterruptibly(insertFuture);

        assertMetricIndexMatches("t1", NUMERIC, asList(m1, m3));
        assertMetricIndexMatches("t1", AVAILABILITY, asList(m2));

        assertDataRetentionsIndexMatches("t1", NUMERIC, ImmutableSet.of(new Retention(m3.getId(), 24),
            new Retention(m1.getId(), 24)));
    }

    @Test
    public void updateMetadata() throws Exception {
        NumericMetric2 metric = new NumericMetric2("t1", new MetricId("m1"), ImmutableMap.of("a1", "1", "a2", "2"));
        ListenableFuture<Void> insertFuture = metricsService.createMetric(metric);
        getUninterruptibly(insertFuture);

        Map<String, String> additions = ImmutableMap.of("a2", "two", "a3", "3");
        Set<String> deletions = ImmutableSet.of("a1");
        insertFuture = metricsService.updateMetadata(metric, additions, deletions);
        getUninterruptibly(insertFuture);

        ListenableFuture<Metric> queryFuture = metricsService.findMetric(metric.getTenantId(), NUMERIC,
            metric.getId());
        Metric updatedMetric = getUninterruptibly(queryFuture);

        assertEquals(updatedMetric.getMetadata(), ImmutableMap.of("a2", "two", "a3", "3"),
            "The updated meta data does not match the expected values");

        assertMetricIndexMatches(metric.getTenantId(), NUMERIC, asList(updatedMetric));
    }

    @Test
    public void addAndFetchNumericData() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t1")));

        NumericMetric2 m1 = new NumericMetric2("t1", new MetricId("m1"));
        m1.addData(start.getMillis(), 1.1);
        m1.addData(start.plusMinutes(2).getMillis(), 2.2);
        m1.addData(start.plusMinutes(4).getMillis(), 3.3);
        m1.addData(end.getMillis(), 4.4);

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(m1));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> queryFuture = metricsService.findData(m1, start.getMillis(),
            end.getMillis());
        List<NumericData> actual = getUninterruptibly(queryFuture);
        List<NumericData> expected = asList(
            new NumericData(m1, start.plusMinutes(4).getMillis(), 3.3),
            new NumericData(m1, start.plusMinutes(2).getMillis(), 2.2),
            new NumericData(m1, start.getMillis(), 1.1)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
        assertMetricIndexMatches("t1", NUMERIC, asList(m1));
    }

    @Test
    public void verifyTTLsSetOnNumericData() throws Exception {
        DateTime start = now().minusMinutes(10);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t1")));
        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t2")
            .setRetention(NUMERIC, days(14).toStandardHours().getHours())));

        VerifyTTLDataAccess verifyTTLDataAccess = new VerifyTTLDataAccess(dataAccess);

        metricsService.unloadDataRetentions();
        metricsService.loadDataRetentions();
        metricsService.setDataAccess(verifyTTLDataAccess);

        NumericMetric2 m1 = new NumericMetric2("t1", new MetricId("m1"));
        m1.addData(start.getMillis(), 1.01);
        m1.addData(start.plusMinutes(1).getMillis(), 1.02);
        m1.addData(start.plusMinutes(2).getMillis(), 1.03);

        addDataInThePast(m1, days(2).toStandardDuration());

        Set<String> tags = ImmutableSet.of("tag1");

        verifyTTLDataAccess.numericTagTTLLessThanEqualTo(DEFAULT_TTL - days(2).toStandardSeconds().getSeconds());
        getUninterruptibly(metricsService.tagNumericData(m1, tags, start.getMillis(),
            start.plusMinutes(2).getMillis()));

        verifyTTLDataAccess.setNumericTTL(days(14).toStandardSeconds().getSeconds());
        NumericMetric2 m2 = new NumericMetric2("t2", new MetricId("m2"));
        m2.addData(start.plusMinutes(5).getMillis(), 2.02);
        addDataInThePast(m2, days(3).toStandardDuration());

        verifyTTLDataAccess.numericTagTTLLessThanEqualTo(days(14).minus(3).toStandardSeconds().getSeconds());
        getUninterruptibly(metricsService.tagNumericData(m2, tags, start.plusMinutes(5).getMillis()));

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t3")
            .setRetention(NUMERIC, 24)));
        verifyTTLDataAccess.setNumericTTL(hours(24).toStandardSeconds().getSeconds());
        NumericMetric2 m3 = new NumericMetric2("t3", new MetricId("m3"));
        m3.addData(start.getMillis(), 3.03);
        getUninterruptibly(metricsService.addNumericData(asList(m3)));

        NumericMetric2 m4 = new NumericMetric2("t2", new MetricId("m4"), Collections.EMPTY_MAP, 28);
        getUninterruptibly(metricsService.createMetric(m4));

        verifyTTLDataAccess.setNumericTTL(28);
        m4.addData(start.plusMinutes(3).getMillis(), 4.1);
        m4.addData(start.plusMinutes(4).getMillis(), 4.2);
        getUninterruptibly(metricsService.addNumericData(asList(m4)));
    }

    @Test
    public void verifyTTLsSetOnAvailabilityData() throws Exception {
        DateTime start = now().minusMinutes(10);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t1")));
        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t2")
            .setRetention(AVAILABILITY, days(14).toStandardHours().getHours())));

        VerifyTTLDataAccess verifyTTLDataAccess = new VerifyTTLDataAccess(dataAccess);

        metricsService.unloadDataRetentions();
        metricsService.loadDataRetentions();
        metricsService.setDataAccess(verifyTTLDataAccess);
        metricsService.setDataAccess(verifyTTLDataAccess);

        AvailabilityMetric m1 = new AvailabilityMetric("t1", new MetricId("m1"));
        m1.addData(new Availability(start.getMillis(), UP));
        m1.addData(new Availability(start.plusMinutes(1).getMillis(), DOWN));
        m1.addData(new Availability(start.plusMinutes(2).getMillis(), DOWN));
        addDataInThePast(m1, days(2).toStandardDuration());

        Set<String> tags = ImmutableSet.of("tag1");

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(DEFAULT_TTL - days(2).toStandardSeconds().getSeconds());
        getUninterruptibly(metricsService.tagAvailabilityData(m1, tags, start.getMillis(),
            start.plusMinutes(2).getMillis()));

        verifyTTLDataAccess.setAvailabilityTTL(days(14).toStandardSeconds().getSeconds());
        AvailabilityMetric m2 = new AvailabilityMetric("t2", new MetricId("m2"));
        m2.addData(new Availability(start.plusMinutes(5).getMillis(), UP));
        addDataInThePast(m2, days(5).toStandardDuration());

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(days(14).minus(5).toStandardSeconds().getSeconds());
        getUninterruptibly(metricsService.tagAvailabilityData(m2, tags, start.plusMinutes(5).getMillis()));

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t3")
            .setRetention(AVAILABILITY, 24)));
        verifyTTLDataAccess.setAvailabilityTTL(hours(24).toStandardSeconds().getSeconds());
        AvailabilityMetric m3 = new AvailabilityMetric("t3", new MetricId("m3"));
        m3.addData(new Availability(start.getMillis(), UP));
        getUninterruptibly(metricsService.addAvailabilityData(asList(m3)));
    }

    private void addDataInThePast(NumericMetric2 metric, final Duration duration) throws Exception {
        DataAccess originalDataAccess = metricsService.getDataAccess();
        try {
            metricsService.setDataAccess(new DelegatingDataAccess(dataAccess) {
                @Override
                public ResultSetFuture insertData(NumericMetric2 m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (NumericData d : m.getData()) {
                        batchStatement.add(insertNumericDataWithTimestamp.bind(m.getTenantId(), NUMERIC.getCode(),
                                m.getId().getName(), m.getId().getInterval().toString(), DPART, d.getTimeUUID(),
                                d.getValue(), actualTTL, writeTime));
                    }
                    return session.executeAsync(batchStatement);
                }
            });
            metricsService.addNumericData(asList(metric));
        } finally {
            metricsService.setDataAccess(originalDataAccess);
        }
    }

    private void addDataInThePast(AvailabilityMetric metric, final Duration duration) throws Exception {
        DataAccess originalDataAccess = metricsService.getDataAccess();
        try {
            metricsService.setDataAccess(new DelegatingDataAccess(dataAccess) {
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
                    return session.executeAsync(batchStatement);
                }
            });
            metricsService.addAvailabilityData(asList(metric));
        } finally {
            metricsService.setDataAccess(originalDataAccess);
        }
    }

    @Test
    public void fetchNumericDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("tenant1")));

        NumericMetric2 metric = new NumericMetric2("tenant1", new MetricId("m1"));
        metric.addData(start.getMillis(), 100.0);
        metric.addData(start.plusMinutes(1).getMillis(), 101.1);
        metric.addData(start.plusMinutes(2).getMillis(), 102.2);
        metric.addData(start.plusMinutes(3).getMillis(), 103.3);
        metric.addData(start.plusMinutes(4).getMillis(), 104.4);
        metric.addData(start.plusMinutes(5).getMillis(), 105.5);
        metric.addData(start.plusMinutes(6).getMillis(), 106.6);

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(metric));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> tagFuture = metricsService.tagNumericData(metric,
            ImmutableSet.of("t1", "t2"), start.plusMinutes(2).getMillis());
        getUninterruptibly(tagFuture);

        tagFuture = metricsService.tagNumericData(metric, ImmutableSet.of("t3", "t4"), start.plusMinutes(3).getMillis(),
            start.plusMinutes(5).getMillis());
        getUninterruptibly(tagFuture);

        ListenableFuture<List<NumericData>> queryFuture = metricsService.findData(metric, start.getMillis(),
            end.getMillis());
        List<NumericData> actual = getUninterruptibly(queryFuture);
        List<NumericData> expected = asList(
            new NumericData(metric, start.plusMinutes(6).getMillis(), 106.6),
            new NumericData(metric, start.plusMinutes(5).getMillis(), 105.5),
            new NumericData(metric, start.plusMinutes(4).getMillis(), 104.4),
            new NumericData(metric, start.plusMinutes(3).getMillis(), 103.3),
            new NumericData(metric, start.plusMinutes(2).getMillis(), 102.2),
            new NumericData(metric, start.plusMinutes(1).getMillis(), 101.1),
            new NumericData(metric, start.getMillis(), 100.0)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
        assertEquals(actual.get(3).getTags(), ImmutableSet.of(new Tag("t3"), new Tag("t4")), "The tags do not match");
        assertEquals(actual.get(2).getTags(), ImmutableSet.of(new Tag("t3"), new Tag("t4")), "The tags do not match");
        assertEquals(actual.get(2).getTags(), ImmutableSet.of(new Tag("t3"), new Tag("t4")), "The tags do not match");
        assertEquals(actual.get(4).getTags(), ImmutableSet.of(new Tag("t1"), new Tag("t2")), "The tags do not match");
    }

    @Test
    public void addNumericDataForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenantId)));

        NumericMetric2 m1 = new NumericMetric2(tenantId, new MetricId("m1"));
        m1.addData(start.plusSeconds(30).getMillis(), 11.2);
        m1.addData(start.getMillis(), 11.1);

        NumericMetric2 m2 = new NumericMetric2(tenantId, new MetricId("m2"));
        m2.addData(start.plusSeconds(30).getMillis(), 12.2);
        m2.addData(start.getMillis(), 12.1);

        NumericMetric2 m3 = new NumericMetric2(tenantId, new MetricId("m3"));

        NumericMetric2 m4 = new NumericMetric2(tenantId, new MetricId("m4"), Collections.EMPTY_MAP, 24);
        getUninterruptibly(metricsService.createMetric(m4));
        m4.addData(start.plusSeconds(30).getMillis(), 55.5);
        m4.addData(end.getMillis(), 66.6);

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(m1, m2, m3, m4));
        getUninterruptibly(insertFuture);

        ListenableFuture<NumericMetric2> queryFuture = metricsService.findNumericData(m1, start.getMillis(),
            end.getMillis());
        NumericMetric2 actual = getUninterruptibly(queryFuture);
        assertMetricEquals(actual, m1);

        queryFuture = metricsService.findNumericData(m2, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        assertMetricEquals(actual, m2);

        queryFuture = metricsService.findNumericData(m3, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        assertNull(actual, "Did not expect to get back results since there is no data for " + m3);

        queryFuture = metricsService.findNumericData(m4, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        NumericMetric2 expected = new NumericMetric2(tenantId, new MetricId("m4"));
        expected.setDataRetention(24);
        expected.addData(start.plusSeconds(30).getMillis(), 55.5);
        assertMetricEquals(actual, expected);

        assertMetricIndexMatches(tenantId, NUMERIC, asList(m1, m2, m3, m4));
    }

    @Test
    public void addAvailabilityForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenantId)));

        AvailabilityMetric m1 = new AvailabilityMetric(tenantId, new MetricId("m1"));
        m1.addData(new Availability(m1, start.plusSeconds(20).getMillis(), "down"));
        m1.addData(new Availability(m1, start.plusSeconds(10).getMillis(), "up"));

        AvailabilityMetric m2 = new AvailabilityMetric(tenantId, new MetricId("m2"));
        m2.addData(new Availability(m2, start.plusSeconds(30).getMillis(), "up"));
        m2.addData(new Availability(m2, start.plusSeconds(15).getMillis(), "down"));

        AvailabilityMetric m3 = new AvailabilityMetric(tenantId, new MetricId("m3"));

        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

        ListenableFuture<AvailabilityMetric> queryFuture = metricsService.findAvailabilityData(m1, start.getMillis(),
            end.getMillis());
        AvailabilityMetric actual = getUninterruptibly(queryFuture);
        assertMetricEquals(actual, m1);

        queryFuture = metricsService.findAvailabilityData(m2, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        assertMetricEquals(actual, m2);

        queryFuture = metricsService.findAvailabilityData(m3, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        assertNull(actual, "Did not expect to get back results since there is no data for " + m3);

        AvailabilityMetric m4 = new AvailabilityMetric(tenantId, new MetricId("m4"), Collections.EMPTY_MAP, 24);
        getUninterruptibly(metricsService.createMetric(m4));
        m4.addData(new Availability(start.plusMinutes(2).getMillis(), UP));
        m4.addData(new Availability(end.plusMinutes(2).getMillis(), UP));

        insertFuture = metricsService.addAvailabilityData(asList(m4));
        getUninterruptibly(insertFuture);

        queryFuture = metricsService.findAvailabilityData(m4, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        AvailabilityMetric expected = new AvailabilityMetric(tenantId, m4.getId(), Collections.EMPTY_MAP, 24);
        expected.addData(new Availability(start.plusMinutes(2).getMillis(), UP));
        assertMetricEquals(actual, expected);

        assertMetricIndexMatches(tenantId, AVAILABILITY, asList(m1, m2, m3, m4));
    }

    @Test
    public void fetchAvailabilityDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("tenant1")));

        AvailabilityMetric metric = new AvailabilityMetric("tenant1", new MetricId("A1"));
        metric.addAvailability(start.getMillis(), UP);
        metric.addAvailability(start.plusMinutes(1).getMillis(), DOWN);
        metric.addAvailability(start.plusMinutes(2).getMillis(), DOWN);
        metric.addAvailability(start.plusMinutes(3).getMillis(), UP);
        metric.addAvailability(start.plusMinutes(4).getMillis(), DOWN);
        metric.addAvailability(start.plusMinutes(5).getMillis(), UP);
        metric.addAvailability(start.plusMinutes(6).getMillis(), UP);

        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(asList(metric));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<Availability>> tagFuture = metricsService.tagAvailabilityData(metric,
            ImmutableSet.of("t1", "t2"), start.plusMinutes(2).getMillis());
        getUninterruptibly(tagFuture);

        tagFuture = metricsService.tagAvailabilityData(metric, ImmutableSet.of("t3", "t4"),
            start.plusMinutes(3).getMillis(), start.plusMinutes(5).getMillis());
        getUninterruptibly(tagFuture);

        ListenableFuture<AvailabilityMetric> queryFuture = metricsService.findAvailabilityData(metric,
                start.getMillis(), end.getMillis());
        AvailabilityMetric actualMetric = getUninterruptibly(queryFuture);
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

        assertEquals(actual, expected, "The data does not match the expected values");
        assertEquals(actual.get(3).getTags(), ImmutableSet.of(new Tag("t3"), new Tag("t4")), "The tags do not match");
        assertEquals(actual.get(2).getTags(), ImmutableSet.of(new Tag("t3"), new Tag("t4")), "The tags do not match");
        assertEquals(actual.get(2).getTags(), ImmutableSet.of(new Tag("t3"), new Tag("t4")), "The tags do not match");
        assertEquals(actual.get(4).getTags(), ImmutableSet.of(new Tag("t1"), new Tag("t2")), "The tags do not match");
    }

    @Test
    public void tagNumericDataByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenant)));

        NumericData d1 = new NumericData(start.getMillis(), 101.1);
        NumericData d2 = new NumericData(start.plusMinutes(2).getMillis(), 101.2);
        NumericData d3 = new NumericData(start.plusMinutes(6).getMillis(), 102.2);
        NumericData d4 = new NumericData(start.plusMinutes(8).getMillis(), 102.3);
        NumericData d5 = new NumericData(start.plusMinutes(4).getMillis(), 102.1);
        NumericData d6 = new NumericData(start.plusMinutes(4).getMillis(), 101.4);
        NumericData d7 = new NumericData(start.plusMinutes(10).getMillis(), 102.4);
        NumericData d8 = new NumericData(start.plusMinutes(6).getMillis(), 103.1);
        NumericData d9 = new NumericData(start.plusMinutes(7).getMillis(), 103.1);

        NumericMetric2 m1 = new NumericMetric2(tenant, new MetricId("m1"));
        m1.addData(d1);
        m1.addData(d2);
        m1.addData(d6);

        NumericMetric2 m2 = new NumericMetric2(tenant, new MetricId("m2"));
        m2.addData(d3);
        m2.addData(d4);
        m2.addData(d5);
        m2.addData(d7);

        NumericMetric2 m3 = new NumericMetric2(tenant, new MetricId("m3"));
        m3.addData(d8);
        m3.addData(d9);

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> tagFuture1 = metricsService.tagNumericData(m1, ImmutableSet.of("t1"),
            start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<NumericData>> tagFuture2 = metricsService.tagNumericData(m2, ImmutableSet.of("t1"),
            start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<NumericData>> tagFuture3 = metricsService.tagNumericData(m1, ImmutableSet.of("t2"),
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<NumericData>> tagFuture4 = metricsService.tagNumericData(m2, ImmutableSet.of("t2"),
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<NumericData>> tagFuture5 = metricsService.tagNumericData(m3, ImmutableSet.of("t2"),
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());

        getUninterruptibly(tagFuture1);
        getUninterruptibly(tagFuture2);
        getUninterruptibly(tagFuture3);
        getUninterruptibly(tagFuture4);
        getUninterruptibly(tagFuture5);

        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = metricsService.findNumericDataByTags(tenant,
            ImmutableSet.of("t1", "t2"));
        Map<MetricId, Set<NumericData>> actual = getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<NumericData>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(d1, d2, d6),
            new MetricId("m2"), ImmutableSet.of(d5, d3)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagAvailabilityByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenant)));

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

        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<Availability>> tagFuture1 = metricsService.tagAvailabilityData(m1, ImmutableSet.of("t1"),
            start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<Availability>> tagFuture2 = metricsService.tagAvailabilityData(m2, ImmutableSet.of("t1"),
            start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<Availability>> tagFuture3 = metricsService.tagAvailabilityData(m1, ImmutableSet.of("t2"),
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<Availability>> tagFuture4 = metricsService.tagAvailabilityData(m2, ImmutableSet.of("t2"),
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<Availability>> tagFuture5 = metricsService.tagAvailabilityData(m3, ImmutableSet.of("t2"),
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());

        getUninterruptibly(tagFuture1);
        getUninterruptibly(tagFuture2);
        getUninterruptibly(tagFuture3);
        getUninterruptibly(tagFuture4);
        getUninterruptibly(tagFuture5);

        ListenableFuture<Map<MetricId, Set<Availability>>> queryFuture = metricsService.findAvailabilityByTags(tenant,
            ImmutableSet.of("t1", "t2"));
        Map<MetricId, Set<Availability>> actual = getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<Availability>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(a1, a2, a6),
            new MetricId("m2"), ImmutableSet.of(a5, a3)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagIndividualNumericDataPoints() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenant)));

        NumericData d1 = new NumericData(start.getMillis(), 101.1);
        NumericData d2 = new NumericData(start.plusMinutes(2).getMillis(), 101.2);
        NumericData d3 = new NumericData(start.plusMinutes(6).getMillis(), 102.2);
        NumericData d4 = new NumericData(start.plusMinutes(8).getMillis(), 102.3);
        NumericData d5 = new NumericData(start.plusMinutes(4).getMillis(), 102.1);
        NumericData d6 = new NumericData(start.plusMinutes(4).getMillis(), 101.4);
        NumericData d7 = new NumericData(start.plusMinutes(10).getMillis(), 102.4);
        NumericData d8 = new NumericData(start.plusMinutes(6).getMillis(), 103.1);
        NumericData d9 = new NumericData(start.plusMinutes(7).getMillis(), 103.1);

        NumericMetric2 m1 = new NumericMetric2(tenant, new MetricId("m1"));
        m1.addData(d1);
        m1.addData(d2);
        m1.addData(d6);

        NumericMetric2 m2 = new NumericMetric2(tenant, new MetricId("m2"));
        m2.addData(d3);
        m2.addData(d4);
        m2.addData(d5);
        m2.addData(d7);

        NumericMetric2 m3 = new NumericMetric2(tenant, new MetricId("m3"));
        m3.addData(d8);
        m3.addData(d9);


        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> tagFuture = metricsService.tagNumericData(m1, ImmutableSet.of("t1"),
            d1.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d1), "Tagging " + d1 + " returned unexpected results");

        tagFuture = metricsService.tagNumericData(m1, ImmutableSet.of("t1", "t2", "t3"), d2.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d2), "Tagging " + d2 + " returned unexpected results");

        tagFuture = metricsService.tagNumericData(m1, ImmutableSet.of("t1"), start.minusMinutes(10).getMillis());
        assertEquals(getUninterruptibly(tagFuture), Collections.emptyList(),
            "No data should be returned since there is no data for this time");

        tagFuture = metricsService.tagNumericData(m2, ImmutableSet.of("t2", "t3"), d3.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d3), "Tagging " + d3 + " returned unexpected results");

        tagFuture = metricsService.tagNumericData(m2, ImmutableSet.of("t3", "t4"), d4.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d4), "Tagging " + d4 + " returned unexpected results");

        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = metricsService.findNumericDataByTags(tenant,
            ImmutableSet.of("t2", "t3"));
        Map<MetricId, Set<NumericData>> actual = getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<NumericData>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(d2),
            new MetricId("m2"), ImmutableSet.of(d3, d4)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagIndividualAvailabilityDataPoints() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenant)));

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

        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<Availability>> tagFuture = metricsService.tagAvailabilityData(m1, ImmutableSet.of("t1"),
            a1.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(a1), "Tagging " + a1 + " returned unexpected results");

        tagFuture = metricsService.tagAvailabilityData(m1, ImmutableSet.of("t1", "t2", "t3"), a2.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(a2), "Tagging " + a2 + " returned unexpected results");

        tagFuture = metricsService.tagAvailabilityData(m1, ImmutableSet.of("t1"), start.minusMinutes(10).getMillis());
        assertEquals(getUninterruptibly(tagFuture), Collections.emptyList(),
            "No data should be returned since there is no data for this time");

        tagFuture = metricsService.tagAvailabilityData(m2, ImmutableSet.of("t2", "t3"), a3.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(a3), "Tagging " + a3 + " returned unexpected results");

        tagFuture = metricsService.tagAvailabilityData(m2, ImmutableSet.of("t3", "t4"), a4.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(a4), "Tagging " + a4 + " returned unexpected results");

        ListenableFuture<Map<MetricId, Set<Availability>>> queryFuture = metricsService.findAvailabilityByTags(tenant,
            ImmutableSet.of("t2", "t3"));
        Map<MetricId, Set<Availability>> actual = getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<Availability>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(a2),
            new MetricId("m2"), ImmutableSet.of(a3, a4)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    private void assertMetricEquals(Metric actual, Metric expected) {
        assertEquals(actual, expected, "The metric doe not match the expected value");
        assertEquals(actual.getData(), expected.getData(), "The data does not match the expected values");
    }

    private void assertMetricIndexMatches(String tenantId, MetricType type, List<? extends Metric> expected)
        throws Exception {
        ListenableFuture<List<Metric>> metricsFuture = metricsService.findMetrics(tenantId, type);
        List<Metric> actualIndex = getUninterruptibly(metricsFuture);

        assertEquals(actualIndex, expected, "The metrics index results do not match");
    }

    private void assertDataRetentionsIndexMatches(String tenantId, MetricType type, Set<Retention> expected)
        throws Exception {
        ResultSetFuture queryFuture = dataAccess.findDataRetentions(tenantId, type);
        ListenableFuture<Set<Retention>> retentionsFuture = Futures.transform(queryFuture, new DataRetentionsMapper());
        Set<Retention> actual = getUninterruptibly(retentionsFuture);

        assertEquals(actual, expected, "The data retentions are wrong");
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
        public ResultSetFuture insertData(NumericMetric2 metric, int ttl) {
            assertEquals(ttl, numericTTL, "The numeric data TTL does not match the expected value when " +
                "inserting data");
            return super.insertData(metric, ttl);
        }

        @Override
        public ResultSetFuture insertData(AvailabilityMetric metric, int ttl) {
            assertEquals(ttl, availabilityTTL, "The availability data TTL does not match the expected value when " +
                "inserting data");
            return super.insertData(metric, ttl);
        }

        @Override
        public ResultSetFuture insertNumericTag(String tag, List<NumericData> data) {
            for (NumericData d : data) {
                assertTrue(d.getTTL() <= numericTagTTL, "Expected the TTL to be <= " + numericTagTTL +
                    " but it was " + d.getTTL());
            }
            return super.insertNumericTag(tag, data);
        }

        @Override
        public ResultSetFuture insertAvailabilityTag(String tag, List<Availability> data) {
            for (Availability a : data) {
                assertTrue(a.getTTL() <= availabilityTagTTL, "Expected the TTL to be <= " + availabilityTagTTL +
                    " but it was " + a.getTTL());
            }
            return super.insertAvailabilityTag(tag, data);
        }
    }

}
