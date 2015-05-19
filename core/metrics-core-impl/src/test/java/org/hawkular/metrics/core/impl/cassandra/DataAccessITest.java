/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.impl.cassandra;

import static java.util.Arrays.asList;

import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.List;

import org.hawkular.metrics.core.api.AggregationTemplate;
import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityData;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.Gauge;
import org.hawkular.metrics.core.api.GaugeData;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Tenant;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import rx.Observable;

/**
 * @author John Sanda
 */
public class DataAccessITest extends MetricsITest {

    private DataAccessImpl dataAccess;

    private PreparedStatement truncateTenants;

    private PreparedStatement truncateGaugeData;

    private PreparedStatement truncateCounters;

    @BeforeClass
    public void initClass() {
        initSession();
        dataAccess = new DataAccessImpl(session);
        truncateTenants = session.prepare("TRUNCATE tenants");
        truncateGaugeData = session.prepare("TRUNCATE data");
        truncateCounters = session.prepare("TRUNCATE counters");
    }

    @BeforeMethod
    public void initMethod() {
        session.execute(truncateTenants.bind());
        session.execute(truncateGaugeData.bind());
        session.execute(truncateCounters.bind());
    }

    @Test
    public void insertAndFindTenant() throws Exception {
        Tenant tenant1 = new Tenant().setId("tenant-1")
            .addAggregationTemplate(new AggregationTemplate()
                .setType(MetricType.GAUGE)
                .setInterval(new Interval(5, Interval.Units.MINUTES))
                .setFunctions(ImmutableSet.of("max", "min", "avg")))
            .setRetention(MetricType.GAUGE, Days.days(31).toStandardHours().getHours())
            .setRetention(MetricType.GAUGE, new Interval(5, Interval.Units.MINUTES),
                Days.days(100).toStandardHours().getHours());

        Tenant tenant2 = new Tenant().setId("tenant-2")
            .setRetention(MetricType.GAUGE, Days.days(14).toStandardHours().getHours())
            .addAggregationTemplate(new AggregationTemplate()
                .setType(MetricType.GAUGE)
                .setInterval(new Interval(5, Interval.Units.HOURS))
                .setFunctions(ImmutableSet.of("sum", "count")));


        dataAccess.insertTenant(tenant1).toBlocking().lastOrDefault(null);

        dataAccess.insertTenant(tenant2).toBlocking().lastOrDefault(null);

        Tenant actual = dataAccess.findTenant(tenant1.getId())
                                  .flatMap(Observable::from)
                                  .map(Functions::getTenant)
                                  .toBlocking().single();
        assertEquals(actual, tenant1, "The tenants do not match");
    }

    @Test
    public void doNotAllowDuplicateTenants() throws Exception {
        dataAccess.insertTenant(new Tenant().setId("tenant-1")).toBlocking().lastOrDefault(null);
        ResultSet resultSet = dataAccess.insertTenant(new Tenant().setId("tenant-1"))
                                        .toBlocking()
                                        .lastOrDefault(null);
        assertFalse(resultSet.wasApplied(), "Tenants should not be overwritten");
    }

    @Test
    public void insertAndFindGaugeRawData() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(6);

        Gauge metric = new Gauge("tenant-1", new MetricId("metric-1"));
        metric.addData(new GaugeData(start.getMillis(), 1.23));
        metric.addData(new GaugeData(start.plusMinutes(1).getMillis(), 1.234));
        metric.addData(new GaugeData(start.plusMinutes(2).getMillis(), 1.234));
        metric.addData(new GaugeData(end.getMillis(), 1.234));

        getUninterruptibly(dataAccess.insertData(metric, MetricsServiceCassandra.DEFAULT_TTL));

        Observable<ResultSet> observable = dataAccess.findData("tenant-1", new MetricId("metric-1"), start.getMillis(),
                end.getMillis());
        List<GaugeData> actual = ImmutableList.copyOf(observable
                .flatMap(Observable::from)
                .map(Functions::getGaugeData)
                .toBlocking()
                .toIterable());

        List<GaugeData> expected = asList(
            new GaugeData(start.plusMinutes(2).getMillis(), 1.234),
            new GaugeData(start.plusMinutes(1).getMillis(), 1.234),
            new GaugeData(start.getMillis(), 1.23)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
    }

    @Test
    public void addMetadataToGaugeRawData() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(6);

        Gauge metric = new Gauge("tenant-1", new MetricId("metric-1"),
            ImmutableMap.of("units", "KB", "env", "test"));

        dataAccess.addTagsAndDataRetention(metric).toBlocking();

        metric.addData(new GaugeData(start.getMillis(), 1.23));
        metric.addData(new GaugeData(start.plusMinutes(2).getMillis(), 1.234));
        metric.addData(new GaugeData(start.plusMinutes(4).getMillis(), 1.234));
        metric.addData(new GaugeData(end.getMillis(), 1.234));
        getUninterruptibly(dataAccess.insertData(metric, MetricsServiceCassandra.DEFAULT_TTL));

        Observable<ResultSet> observable = dataAccess.findData("tenant-1", new MetricId("metric-1"), start.getMillis(),
                end.getMillis());
        List<GaugeData> actual = ImmutableList.copyOf(observable
                .flatMap(Observable::from)
                .map(Functions::getGaugeData)
                .toBlocking()
                .toIterable());

        List<GaugeData> expected = asList(
            new GaugeData(start.plusMinutes(4).getMillis(), 1.234),
            new GaugeData(start.plusMinutes(2).getMillis(), 1.234),
            new GaugeData(start.getMillis(), 1.23)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
    }

//    @Test
//    public void insertAndFindAggregatedGaugeData() throws Exception {
//        DateTime start = now().minusMinutes(10);
//        DateTime end = start.plusMinutes(6);
//
//        Metric metric = new Metric()
//            .setTenantId("tenant-1")
//            .setId(new MetricId("m1", Interval.parse("5min")));
//        List<GaugeData> data = asList(
//
//        );
//
//        GaugeData d1 = new GaugeData()
//            .setTenantId("tenant-1")
//            .setId(new MetricId("m1", Interval.parse("5min")))
//            .setTimestamp(start.getMillis())
//            .addAggregatedValue(new AggregatedValue("sum", 100.1))
//            .addAggregatedValue(new AggregatedValue("max", 51.5, null, null, getTimeUUID(now().minusMinutes(3))));
//
//        GaugeData d2 = new GaugeData()
//            .setTenantId("tenant-1")
//            .setId(new MetricId("m1", Interval.parse("5min")))
//            .setTimestamp(start.plusMinutes(2).getMillis())
//            .addAggregatedValue(new AggregatedValue("sum", 110.1))
//            .addAggregatedValue(new AggregatedValue("max", 54.7, null, null, getTimeUUID(now().minusMinutes(3))));
//
//        GaugeData d3 = new GaugeData()
//            .setTenantId("tenant-1")
//            .setId(new MetricId("m1", Interval.parse("5min")))
//            .setTimestamp(start.plusMinutes(4).getMillis())
//            .setValue(22.2);
//
//        GaugeData d4 = new GaugeData()
//            .setTenantId("tenant-1")
//            .setId(new MetricId("m1", Interval.parse("5min")))
//            .setTimestamp(end.getMillis())
//            .setValue(22.2);
//
//        getUninterruptibly(dataAccess.insertGaugeData(d1));
//        getUninterruptibly(dataAccess.insertGaugeData(d2));
//        getUninterruptibly(dataAccess.insertGaugeData(d3));
//        getUninterruptibly(dataAccess.insertGaugeData(d4));
//
//        ResultSetFuture queryFuture = dataAccess.findGaugeData(d1.getTenantId(), d1.getId(), 0L, start.getMillis(),
//            end.getMillis());
//        ListenableFuture<List<GaugeData>> dataFuture = Futures.transform(queryFuture, new GaugeDataMapper());
//        List<GaugeData> actual = getUninterruptibly(dataFuture);
//        List<GaugeData> expected = asList(d3, d2, d1);
//
//        assertEquals(actual, expected, "The aggregated gauge data does not match");
//    }

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

    @Test
    public void insertAndFindAvailabilities() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(6);
        String tenantId = "avail-test";
        Availability metric = new Availability(tenantId, new MetricId("m1"));
        metric.addData(new AvailabilityData(start.getMillis(), "up"));

        getUninterruptibly(dataAccess.insertData(metric, 360));

        ResultSetFuture future = dataAccess.findAvailabilityData(tenantId, new MetricId("m1"), start.getMillis(),
                end.getMillis());
        ListenableFuture<List<AvailabilityData>> dataFuture = Futures
                .transform(future, Functions.MAP_AVAILABILITY_DATA);
        List<AvailabilityData> actual = getUninterruptibly(dataFuture);
        List<AvailabilityData> expected = asList(new AvailabilityData(start.getMillis(), "up"));

        assertEquals(actual, expected, "The availability data does not match the expected values");
    }

}
