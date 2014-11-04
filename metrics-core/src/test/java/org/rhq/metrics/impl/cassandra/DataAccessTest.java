package org.rhq.metrics.impl.cassandra;

import static java.util.Arrays.asList;
import static org.joda.time.DateTime.now;
import static org.rhq.metrics.util.TimeUUIDUtils.getTimeUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.List;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.metrics.core.AggregatedValue;
import org.rhq.metrics.core.AggregationTemplate;
import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.Tenant;
import org.rhq.metrics.test.MetricsTest;

/**
 * @author John Sanda
 */
public class DataAccessTest extends MetricsTest {

    private DataAccess dataAccess;

    private PreparedStatement truncateTenants;

    private PreparedStatement truncateNumericData;

    private PreparedStatement truncateCounters;

    @BeforeClass
    public void initClass() {
        initSession();
        dataAccess = new DataAccess(session);
        truncateTenants = session.prepare("TRUNCATE tenants");
        truncateNumericData = session.prepare("TRUNCATE numeric_data");
        truncateCounters = session.prepare("TRUNCATE counters");
    }

    @BeforeMethod
    public void initMethod() {
        session.execute(truncateTenants.bind());
        session.execute(truncateNumericData.bind());
        session.execute(truncateCounters.bind());
    }

    @Test
    public void insertAndFindTenant() throws Exception {
        Tenant tenant1 = new Tenant().setId("tenant-1")
            .addAggregationTemplate(new AggregationTemplate()
                .setType(MetricType.NUMERIC)
                .setInterval(new Interval(5, Interval.Units.MINUTES))
                .setFunctions(ImmutableSet.of("max", "min", "avg")))
            .setRetention(MetricType.NUMERIC, Days.days(31).toStandardHours().getHours())
            .setRetention(MetricType.NUMERIC, new Interval(5, Interval.Units.MINUTES),
                Days.days(100).toStandardHours().getHours());

        Tenant tenant2 = new Tenant().setId("tenant-2")
            .setRetention(MetricType.NUMERIC, Days.days(14).toStandardHours().getHours())
            .addAggregationTemplate(new AggregationTemplate()
                .setType(MetricType.NUMERIC)
                .setInterval(new Interval(5, Interval.Units.HOURS))
                .setFunctions(ImmutableSet.of("sum", "count")));


        ResultSetFuture insertFuture = dataAccess.insertTenant(tenant1);
        getUninterruptibly(insertFuture);

        insertFuture = dataAccess.insertTenant(tenant2);
        getUninterruptibly(insertFuture);

        ResultSetFuture queryFuture = dataAccess.findTenants();
        ListenableFuture<Set<Tenant>> tenantsFuture = Futures.transform(queryFuture, new TenantsMapper());
        Set<Tenant> actual = getUninterruptibly(tenantsFuture);
        Set<Tenant> expected = ImmutableSet.of(tenant1, tenant2);

        assertEquals(actual, expected, "The tenants do not match");
    }

    @Test
    public void doNotAllowDuplicateTenats() throws Exception {
        getUninterruptibly(dataAccess.insertTenant(new Tenant().setId("tenant-1")));
        ResultSet resultSet = getUninterruptibly(dataAccess.insertTenant(new Tenant().setId("tenant-1")));
        assertFalse(resultSet.wasApplied(), "Tenants should not be overwritten");
    }

    @Test
    public void insertAndFindNumericRawData() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(6);

        NumericData d1 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimestamp(start.getMillis())
            .setValue(1.23);

        NumericData d2 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimestamp(start.plusMinutes(1).getMillis())
            .setValue(1.234);

        NumericData d3 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimestamp(start.plusMinutes(2).getMillis())
            .setValue(1.234);

        NumericData d4 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimestamp(end.getMillis())
            .setValue(1.234);

        getUninterruptibly(dataAccess.insertNumericData(d1));
        getUninterruptibly(dataAccess.insertNumericData(d2));
        getUninterruptibly(dataAccess.insertNumericData(d3));
        getUninterruptibly(dataAccess.insertNumericData(d4));

        ResultSetFuture queryFuture = dataAccess.findNumericData(d1.getTenantId(), d1.getMetric(), Interval.NONE, 0L,
            start.getMillis(), end.getMillis());
        ListenableFuture<List<NumericData>> dataFuture = Futures.transform(queryFuture, new NumericDataMapper());
        List<NumericData> actual = getUninterruptibly(dataFuture);
        List<NumericData> expected = asList(d3, d2, d1);

        assertEquals(actual, expected, "The numeric data does not match");
    }

    @Test
    public void addAttributesToNumericRawData() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(6);

       ResultSetFuture insertFuture = dataAccess.addNumericAttributes("tenant-1", "metric-1", Interval.NONE, 0,
           ImmutableMap.of("units", "KB", "env", "test"));
       getUninterruptibly(insertFuture);

        NumericData d1 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimestamp(start.getMillis())
            .setValue(1.23)
            .putAttribute("test?", "true");

        NumericData d2 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimestamp(start.plusMinutes(2).getMillis())
            .setValue(1.234);

        NumericData d3 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimestamp(start.plusMinutes(4).getMillis())
            .setValue(1.234);

        NumericData d4 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("metric-1")
            .setTimestamp(end.getMillis())
            .setValue(1.234);

        getUninterruptibly(dataAccess.insertNumericData(d1));
        getUninterruptibly(dataAccess.insertNumericData(d2));
        getUninterruptibly(dataAccess.insertNumericData(d3));
        getUninterruptibly(dataAccess.insertNumericData(d4));

        ResultSetFuture queryFuture = dataAccess.findNumericData(d1.getTenantId(), d1.getMetric(), Interval.NONE, 0L,
            start.getMillis(), end.getMillis());
        ListenableFuture<List<NumericData>> dataFuture = Futures.transform(queryFuture, new NumericDataMapper());
        List<NumericData> actual = getUninterruptibly(dataFuture);
        List<NumericData> expected = asList(
            d3.putAttribute("units", "KB").putAttribute("env", "test").putAttribute("test?", "true"),
            d2.putAttribute("units", "KB").putAttribute("env", "test").putAttribute("test?", "true"),
            d1.putAttribute("units", "KB").putAttribute("env", "test")
        );

        assertEquals(actual, expected, "The numeric data does not match");
    }

    @Test
    public void insertAndFindAggregatedNumericData() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(6);

        NumericData d1 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("m1")
            .setInterval(Interval.parse("5min"))
            .setTimestamp(start.getMillis())
            .addAggregatedValue(new AggregatedValue("sum", 100.1))
            .addAggregatedValue(new AggregatedValue("max", 51.5, null, null, getTimeUUID(now().minusMinutes(3))));

        NumericData d2 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("m1")
            .setInterval(Interval.parse("5min"))
            .setTimestamp(start.plusMinutes(2).getMillis())
            .addAggregatedValue(new AggregatedValue("sum", 110.1))
            .addAggregatedValue(new AggregatedValue("max", 54.7, null, null, getTimeUUID(now().minusMinutes(3))));

        NumericData d3 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("m1")
            .setInterval(Interval.parse("5min"))
            .setTimestamp(start.plusMinutes(4).getMillis())
            .setValue(22.2);

        NumericData d4 = new NumericData()
            .setTenantId("tenant-1")
            .setMetric("m1")
            .setInterval(Interval.parse("5min"))
            .setTimestamp(end.getMillis())
            .setValue(22.2);

        getUninterruptibly(dataAccess.insertNumericData(d1));
        getUninterruptibly(dataAccess.insertNumericData(d2));
        getUninterruptibly(dataAccess.insertNumericData(d3));
        getUninterruptibly(dataAccess.insertNumericData(d4));

        ResultSetFuture queryFuture = dataAccess.findNumericData(d1.getTenantId(), d1.getMetric(), d1.getInterval(), 0L,
            start.getMillis(), end.getMillis());
        ListenableFuture<List<NumericData>> dataFuture = Futures.transform(queryFuture, new NumericDataMapper());
        List<NumericData> actual = getUninterruptibly(dataFuture);
        List<NumericData> expected = asList(d3, d2, d1);

        assertEquals(actual, expected, "The aggregated numeric data does not match");
    }

    @Test
    public void updateCounterAndFindCounter() throws Exception {
        Counter counter = new Counter("t1", "simple-test", "c1", 1);

        ResultSetFuture future = dataAccess.updateCounter(counter);
        getUninterruptibly(future);

        ResultSetFuture queryFuture = dataAccess.findCounters("t1", "simple-test", asList("c1"));
        List<Counter> actual = getUninterruptibly(Futures.transform(queryFuture, new CountersMapper()));
        List<Counter> expected = asList(counter);

        assertEquals(actual, expected, "The counters do not match");
    }

    @Test
    public void updateCounters() throws Exception {
        String tenantId = "t1";
        String group = "batch-test";
        List<Counter> expected = ImmutableList.of(
            new Counter(tenantId, group, "c1", 1),
            new Counter(tenantId, group, "c2", 2),
            new Counter(tenantId, group, "c3", 3)
        );

        ResultSetFuture future = dataAccess.updateCounters(expected);
        getUninterruptibly(future);

        ResultSetFuture queryFuture = dataAccess.findCounters(tenantId, group);
        List<Counter> actual = getUninterruptibly(Futures.transform(queryFuture, new CountersMapper()));

        assertEquals(actual, expected, "The counters do not match the expected values");
    }

    @Test
    public void findCountersByGroup() throws Exception {
        Counter c1 = new Counter("t1", "group1", "c1", 1);
        Counter c2 = new Counter("t1", "group1", "c2", 2);
        Counter c3 = new Counter("t2", "group2", "c1", 1);
        Counter c4 = new Counter("t2", "group2", "c2", 2);

        ResultSetFuture future = dataAccess.updateCounters(asList(c1, c2, c3, c4));
        getUninterruptibly(future);

        ResultSetFuture queryFuture = dataAccess.findCounters("t1", c1.getGroup());
        List<Counter> actual = getUninterruptibly(Futures.transform(queryFuture, new CountersMapper()));
        List<Counter> expected = asList(c1, c2);

        assertEquals(actual, expected, "The counters do not match the expected values when filtering by group");
    }

    @Test
    public void findCountersByGroupAndName() throws Exception {
        String tenantId = "t1";
        String group = "batch-test";
        Counter c1 = new Counter(tenantId, group, "c1", 1);
        Counter c2 = new Counter(tenantId, group, "c2", 2);
        Counter c3 = new Counter(tenantId, group, "c3", 3);

        ResultSetFuture future = dataAccess.updateCounters(asList(c1, c2, c3));
        getUninterruptibly(future);

        ResultSetFuture queryFuture = dataAccess.findCounters(tenantId, group, asList("c1", "c3"));
        List<Counter> actual = getUninterruptibly(Futures.transform(queryFuture, new CountersMapper()));
        List<Counter> expected = asList(c1, c3);

        assertEquals(actual, expected,
            "The counters do not match the expected values when filtering by group and by counter names");
    }

}
