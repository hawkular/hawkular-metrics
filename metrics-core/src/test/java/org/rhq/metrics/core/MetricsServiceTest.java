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

        Metric metric = new Metric()
            .setTenantId("t1")
            .setId(new MetricId("m1"));

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(
            new NumericData(metric, start.getMillis(), 1.1),
            new NumericData(metric, start.plusMinutes(2).getMillis(), 2.2),
            new NumericData(metric, start.plusMinutes(4).getMillis(), 3.3),
            new NumericData(metric, end.getMillis(), 4.4)
        ));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> queryFuture = metricsService.findData(metric.getTenantId(),
            metric.getId().getName(), start.getMillis(), end.getMillis());
        List<NumericData> actual = getUninterruptibly(queryFuture);
        List<NumericData> expected = asList(
            new NumericData(metric, start.plusMinutes(4).getMillis(), 3.3),
            new NumericData(metric, start.plusMinutes(2).getMillis(), 2.2),
            new NumericData(metric, start.getMillis(), 1.1)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
    }

    @Test
    public void tagNumericDataByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        Metric m1 = new Metric().setTenantId(tenant).setId(new MetricId("m1"));
        Metric m2 = new Metric().setTenantId(tenant).setId(new MetricId("m2"));
        Metric m3 = new Metric().setTenantId(tenant).setId(new MetricId("m3"));

        NumericData d1 = new NumericData(m1, start.getMillis(), 101.1);
        NumericData d2 = new NumericData(m1, start.plusMinutes(2).getMillis(), 101.2);
        NumericData d3 = new NumericData(m2, start.plusMinutes(6).getMillis(), 102.2);
        NumericData d4 = new NumericData(m2, start.plusMinutes(8).getMillis(), 102.3);
        NumericData d5 = new NumericData(m2, start.plusMinutes(4).getMillis(), 102.1);
        NumericData d6 = new NumericData(m1, start.plusMinutes(4).getMillis(), 101.4);
        NumericData d7 = new NumericData(m2, start.plusMinutes(10).getMillis(), 102.4);
        NumericData d8 = new NumericData(m3, start.plusMinutes(6).getMillis(), 103.1);
        NumericData d9 = new NumericData(m3, start.plusMinutes(7).getMillis(), 103.1);

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(d1, d2, d3, d4, d5, d6, d7, d8, d9));
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

        Metric m1 = new Metric().setTenantId(tenant).setId(new MetricId("m1"));
        Metric m2 = new Metric().setTenantId(tenant).setId(new MetricId("m2"));
        Metric m3 = new Metric().setTenantId(tenant).setId(new MetricId("m3"));

        NumericData d1 = new NumericData(m1, start.getMillis(), 101.1);
        NumericData d2 = new NumericData(m1, start.plusMinutes(2).getMillis(), 101.2);
        NumericData d3 = new NumericData(m2, start.plusMinutes(6).getMillis(), 102.2);
        NumericData d4 = new NumericData(m2, start.plusMinutes(8).getMillis(), 102.3);
        NumericData d5 = new NumericData(m2, start.plusMinutes(4).getMillis(), 102.1);
        NumericData d6 = new NumericData(m1, start.plusMinutes(4).getMillis(), 101.4);
        NumericData d7 = new NumericData(m2, start.plusMinutes(10).getMillis(), 102.4);
        NumericData d8 = new NumericData(m3, start.plusMinutes(6).getMillis(), 103.1);
        NumericData d9 = new NumericData(m3, start.plusMinutes(7).getMillis(), 103.1);


        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(d1, d2, d3, d4, d5, d6, d7, d8, d9));
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
