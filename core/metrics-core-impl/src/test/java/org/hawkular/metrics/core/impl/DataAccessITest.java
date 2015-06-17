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
package org.hawkular.metrics.core.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hawkular.metrics.core.api.AvailabilityType.UP;
import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;
import static org.hawkular.metrics.core.impl.MetricsServiceImpl.DEFAULT_TTL;
import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.hawkular.metrics.core.api.AggregationTemplate;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.Tenant;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
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
    }

    @BeforeMethod
    public void initMethod() {
        session.execute(truncateTenants.bind());
        session.execute(truncateGaugeData.bind());
    }

    @Test
    public void insertAndFindTenant() throws Exception {
        Tenant tenant1 = new Tenant("tenant-1")
            .addAggregationTemplate(new AggregationTemplate()
                    .setType(GAUGE)
                    .setInterval(new Interval(5, Interval.Units.MINUTES))
                    .setFunctions(ImmutableSet.of("max", "min", "avg")))
            .setRetention(GAUGE, Days.days(31).toStandardHours().getHours())
            .setRetention(GAUGE, new Interval(5, Interval.Units.MINUTES),
                Days.days(100).toStandardHours().getHours());

        Tenant tenant2 = new Tenant("tenant-2")
            .setRetention(GAUGE, Days.days(14).toStandardHours().getHours())
            .addAggregationTemplate(new AggregationTemplate()
                .setType(GAUGE)
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
        dataAccess.insertTenant(new Tenant("tenant-1")).toBlocking().lastOrDefault(null);
        ResultSet resultSet = dataAccess.insertTenant(new Tenant("tenant-1"))
                                        .toBlocking()
                                        .lastOrDefault(null);
        assertFalse(resultSet.wasApplied(), "Tenants should not be overwritten");
    }

    @Test
    public void insertAndFindGaugeRawData() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(6);

        Metric<Double> metric = new Metric<>("tenant-1", GAUGE, new MetricId("metric-1"), asList(
                new DataPoint<>(start.getMillis(), 1.23),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 1.234),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 1.234),
                new DataPoint<>(end.getMillis(), 1.234)
        ));

        dataAccess.insertData(metric, DEFAULT_TTL).toBlocking().last();

        Observable<ResultSet> observable = dataAccess.findData("tenant-1", new MetricId("metric-1"), GAUGE,
                start.getMillis(), end.getMillis());
        List<DataPoint<Double>> actual = ImmutableList.copyOf(observable
                .flatMap(Observable::from)
                .map(Functions::getGaugeDataPoint)
                .toBlocking()
                .toIterable());

        List<DataPoint<Double>> expected = asList(
            new DataPoint<>(start.plusMinutes(2).getMillis(), 1.234),
            new DataPoint<>(start.plusMinutes(1).getMillis(), 1.234),
            new DataPoint<>(start.getMillis(), 1.23)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
    }

    @Test
    public void addMetadataToGaugeRawData() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(6);
        String tenantId = "tenant-1";

        Metric<Double> metric = new Metric<>(tenantId, GAUGE, new MetricId("metric-1"),
                ImmutableMap.of("units", "KB", "env", "test"), DEFAULT_TTL);

        dataAccess.addTagsAndDataRetention(metric).toBlocking().last();

        metric = new Metric<>(tenantId, GAUGE, new MetricId("metric-1"), asList(
                new DataPoint<>(start.getMillis(), 1.23),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 1.234),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 1.234),
                new DataPoint<>(end.getMillis(), 1.234)
        ));

        dataAccess.insertData(metric, DEFAULT_TTL).toBlocking().last();

        Observable<ResultSet> observable = dataAccess.findData("tenant-1", new MetricId("metric-1"), GAUGE,
                start.getMillis(), end.getMillis());
        List<DataPoint<Double>> actual = ImmutableList.copyOf(observable
                .flatMap(Observable::from)
                .map(Functions::getGaugeDataPoint)
                .toBlocking()
                .toIterable());

        List<DataPoint<Double>> expected = asList(
            new DataPoint<>(start.plusMinutes(4).getMillis(), 1.234),
            new DataPoint<>(start.plusMinutes(2).getMillis(), 1.234),
            new DataPoint<>(start.getMillis(), 1.23)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
    }

    @Test
    public void insertAndFindAvailabilities() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(6);
        String tenantId = "avail-test";
        Metric<AvailabilityType> metric = new Metric<>(tenantId, AVAILABILITY, new MetricId("m1"),
                singletonList(new DataPoint<>(start.getMillis(), UP)));

        dataAccess.insertAvailabilityData(metric, 360).toBlocking().lastOrDefault(null);

        List<DataPoint<AvailabilityType>> actual = dataAccess
            .findAvailabilityData(tenantId, new MetricId("m1"), start.getMillis(), end.getMillis())
                .flatMap(Observable::from)
                .map(Functions::getAvailabilityDataPoint)
                .toList().toBlocking().lastOrDefault(null);
        List<DataPoint<AvailabilityType>> expected = singletonList(new DataPoint<AvailabilityType>(start.getMillis(),
                UP));

        assertEquals(actual, expected, "The availability data does not match the expected values");
    }

}
