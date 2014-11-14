package org.rhq.metrics.core;

import static java.util.Arrays.asList;
import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.metrics.impl.cassandra.MetricsServiceCassandra;
import org.rhq.metrics.test.MetricsTest;

/**
 * @author John Sanda
 */
public class MetricsServiceTest extends MetricsTest {

    private MetricsServiceCassandra metricsService;

    @BeforeClass
    public void initClass() {
        initSession();
        metricsService = new MetricsServiceCassandra();
        metricsService.startUp(session);
    }

    @BeforeMethod
    public void initMethod() {
        session.execute("TRUNCATE tenants");
        session.execute("TRUNCATE numeric_data");
        session.execute("TRUNCATE tags");
    }


    @Test
    public void addAndFetchRawData() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "t1";
        String metric = "m1";

        NumericData d1 = new NumericData().setTenantId(tenantId).setId(new MetricId("m1")).setTimestamp(start.getMillis())
            .setValue(1.1);
        NumericData d2 = new NumericData().setTenantId(tenantId).setId(new MetricId("m1"))
            .setTimestamp(start.plusMinutes(2).getMillis()).setValue(2.2);
        NumericData d3 = new NumericData().setTenantId(tenantId).setId(new MetricId("m1"))
            .setTimestamp(start.plusMinutes(4).getMillis()).setValue(3.3);
        NumericData d4 = new NumericData().setTenantId(tenantId).setId(new MetricId("m1")).setTimestamp(end.getMillis())
            .setValue(4.4);

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(ImmutableSet.of(d1, d2, d3, d4));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> queryFuture = metricsService.findData(tenantId, metric, start.getMillis(),
            end.getMillis());
        List<NumericData> actual = getUninterruptibly(queryFuture);
        List<NumericData> expected = asList(d3, d2, d1);

        assertEquals(actual, expected, "The numeric raw data does not match");
    }

    @Test
    public void tagNumericDataByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        NumericData d1 = new NumericData().setTenantId(tenant).setId(new MetricId("m1")).setTimestamp(start.getMillis())
            .setValue(101.1);
        NumericData d2 = new NumericData().setTenantId(tenant).setId(new MetricId("m1")).setTimestamp(
            start.plusMinutes(2).getMillis()).setValue(101.2);
        NumericData d6 = new NumericData().setTenantId(tenant).setId(new MetricId("m1")).setTimestamp(
            start.plusMinutes(4).getMillis()).setValue(101.4);

        NumericData d5 = new NumericData().setTenantId(tenant).setId(new MetricId("m2")).setTimestamp(
            start.plusMinutes(4).getMillis()).setValue(102.1);
        NumericData d3 = new NumericData().setTenantId(tenant).setId(new MetricId("m2")).setTimestamp(
            start.plusMinutes(6).getMillis()).setValue(102.2);
        NumericData d4 = new NumericData().setTenantId(tenant).setId(new MetricId("m2")).setTimestamp(
            start.plusMinutes(8).getMillis()).setValue(102.3);
        NumericData d7 = new NumericData().setTenantId(tenant).setId(new MetricId("m2")).setTimestamp(
            start.plusMinutes(10).getMillis()).setValue(102.4);

        NumericData d8 = new NumericData().setTenantId(tenant).setId(new MetricId("m3")).setTimestamp(
            start.plusMinutes(6).getMillis()).setValue(103.1);
        NumericData d9 = new NumericData().setTenantId(tenant).setId(new MetricId("m3")).setTimestamp(
            start.plusMinutes(7).getMillis()).setValue(103.1);

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(ImmutableSet.of(d1, d2, d3, d4, d5, d6, d7,
            d8, d9));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> tagFuture1 = metricsService.tagData(tenant, ImmutableSet.of("t1"), "m1",
            start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<NumericData>> tagFuture2 = metricsService.tagData(tenant, ImmutableSet.of("t1"), "m2",
            start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<NumericData>> tagFuture3 = metricsService.tagData(tenant, ImmutableSet.of("t2"), "m1",
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<NumericData>> tagFuture4 = metricsService.tagData(tenant, ImmutableSet.of("t2"), "m2",
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<NumericData>> tagFuture5 = metricsService.tagData(tenant, ImmutableSet.of("t2"), "m3",
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());

        getUninterruptibly(tagFuture1);
        getUninterruptibly(tagFuture2);
        getUninterruptibly(tagFuture3);
        getUninterruptibly(tagFuture4);
        getUninterruptibly(tagFuture5);

        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = metricsService.findDataByTags(tenant,
            ImmutableSet.of("t1", "t2"));
        Map<MetricId, Set<NumericData>> actual = getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<NumericData>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(d1, d2, d6),
            new MetricId("m2"), ImmutableSet.of(d5, d3)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagIndividualDataPoints() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        NumericData d1 = new NumericData().setTenantId(tenant).setId(new MetricId("m1")).setTimestamp(start.getMillis())
            .setValue(101.1);
        NumericData d2 = new NumericData().setTenantId(tenant).setId(new MetricId("m1")).setTimestamp(
            start.plusMinutes(2).getMillis()).setValue(101.2);
        NumericData d6 = new NumericData().setTenantId(tenant).setId(new MetricId("m1")).setTimestamp(
            start.plusMinutes(4).getMillis()).setValue(101.4);

        NumericData d5 = new NumericData().setTenantId(tenant).setId(new MetricId("m2")).setTimestamp(
            start.plusMinutes(4).getMillis()).setValue(102.1);
        NumericData d3 = new NumericData().setTenantId(tenant).setId(new MetricId("m2")).setTimestamp(
            start.plusMinutes(6).getMillis()).setValue(102.2);
        NumericData d4 = new NumericData().setTenantId(tenant).setId(new MetricId("m2")).setTimestamp(
            start.plusMinutes(8).getMillis()).setValue(102.3);
        NumericData d7 = new NumericData().setTenantId(tenant).setId(new MetricId("m2")).setTimestamp(
            start.plusMinutes(10).getMillis()).setValue(102.4);

        NumericData d8 = new NumericData().setTenantId(tenant).setId(new MetricId("m3")).setTimestamp(
            start.plusMinutes(6).getMillis()).setValue(103.1);
        NumericData d9 = new NumericData().setTenantId(tenant).setId(new MetricId("m3")).setTimestamp(
            start.plusMinutes(7).getMillis()).setValue(103.1);

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(ImmutableSet.of(d1, d2, d3, d4, d5, d6, d7,
            d8, d9));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> tagFuture = metricsService.tagData(tenant, ImmutableSet.of("t1"),
            "m1", d1.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d1), "Tagging " + d1 + " returned unexpected results");

        tagFuture = metricsService.tagData(tenant, ImmutableSet.of("t1", "t2", "t3"), "m1", d2.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d2), "Tagging " + d2 + " returned unexpected results");

        tagFuture = metricsService.tagData(tenant, ImmutableSet.of("t1"), "m1", start.minusMinutes(10).getMillis());
        assertEquals(getUninterruptibly(tagFuture), Collections.emptyList(),
            "No data should be returned since there is no data for this time");

        tagFuture = metricsService.tagData(tenant, ImmutableSet.of("t2", "t3"), "m2", d3.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d3), "Tagging " + d3 + " returned unexpected results");

        tagFuture = metricsService.tagData(tenant, ImmutableSet.of("t3", "t4"), "m2", d4.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d4), "Tagging " + d4 + " returned unexpected results");

        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = metricsService.findDataByTags(tenant,
            ImmutableSet.of("t2", "t3"));
        Map<MetricId, Set<NumericData>> actual = getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<NumericData>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(d2),
            new MetricId("m2"), ImmutableSet.of(d3, d4)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

}
