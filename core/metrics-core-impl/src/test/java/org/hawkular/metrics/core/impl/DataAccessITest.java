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
import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.List;

import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.Tenant;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import rx.Observable;

/**
 * @author John Sanda
 */
public class DataAccessITest extends MetricsITest {

    private static int DEFAULT_TTL = 600;

    private DataAccessImpl dataAccess;

    private PreparedStatement truncateTenants;

    private PreparedStatement truncateGaugeData;

    @BeforeClass
    public void initClass() {
        initSession();
        dataAccess = new DataAccessImpl(session, TestCacheManager.getCacheManager());
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
        Tenant tenant1 = new Tenant("tenant-1", ImmutableMap.of(GAUGE, 31));
        Tenant tenant2 = new Tenant("tenant-2", ImmutableMap.of(GAUGE, 14));


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

        Metric<Double> metric = new Metric<>(new MetricId<>("tenant-1", GAUGE, "metric-1"), asList(
                new DataPoint<>(start.getMillis(), 1.23),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 1.234),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 1.234),
                new DataPoint<>(end.getMillis(), 1.234)
        ));

        dataAccess.insertGaugeData(metric).toBlocking().last();

        Observable<ResultSet> observable = dataAccess.findGaugeData(new MetricId<>("tenant-1", GAUGE, "metric-1"),
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

//        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, "metric-1"),
//                ImmutableMap.of("units", "KB", "env", "test"), DEFAULT_TTL);
//
//        dataAccess.addDataRetention(metric).toBlocking().last();

        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, "metric-1"), asList(
                new DataPoint<>(start.getMillis(), 1.23),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 1.234),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 1.234),
                new DataPoint<>(end.getMillis(), 1.234)
        ));

        dataAccess.insertGaugeData(metric).toBlocking().last();

        Observable<ResultSet> observable = dataAccess.findGaugeData(new MetricId<>("tenant-1", GAUGE, "metric-1"),
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
        Metric<AvailabilityType> metric = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "m1"),
                singletonList(new DataPoint<>(start.getMillis(), UP)));

        dataAccess.insertAvailabilityData(metric).toBlocking().lastOrDefault(null);

        List<DataPoint<AvailabilityType>> actual = dataAccess
                .findAvailabilityData(new MetricId<>(tenantId, AVAILABILITY, "m1"), start.getMillis(), end.getMillis())
                .flatMap(Observable::from)
                .map(Functions::getAvailabilityDataPoint)
                .toList().toBlocking().lastOrDefault(null);
        List<DataPoint<AvailabilityType>> expected = singletonList(new DataPoint<AvailabilityType>(start.getMillis(),
                UP));

        assertEquals(actual, expected, "The availability data does not match the expected values");
    }

}
