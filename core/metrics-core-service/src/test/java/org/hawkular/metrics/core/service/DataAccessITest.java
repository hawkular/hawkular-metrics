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
package org.hawkular.metrics.core.service;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static org.hawkular.metrics.model.AvailabilityType.UP;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import org.hawkular.metrics.core.service.transformers.MetricFromFullDataRowTransformer;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.Tenant;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import rx.Observable;

/**
 * @author John Sanda
 */
public class DataAccessITest extends BaseITest {

    private static int DEFAULT_TTL = 600;

    private static int DEFAULT_PAGE_SIZE = 5000;

    private DataAccessImpl dataAccess;

    private PreparedStatement truncateTenants;

    private PreparedStatement truncateGaugeData;

    private PreparedStatement truncateCompressedData;

    @BeforeClass
    public void initClass() {
        dataAccess = new DataAccessImpl(session);
        truncateTenants = session.prepare("TRUNCATE tenants");
        truncateGaugeData = session.prepare("TRUNCATE data");
        truncateCompressedData = session.prepare("TRUNCATE data_compressed");
    }

    @BeforeMethod
    public void initMethod() {
        session.execute(truncateTenants.bind());
        session.execute(truncateGaugeData.bind());
        session.execute(truncateCompressedData.bind());
    }

    @Test
    public void insertAndFindTenant() throws Exception {
        Tenant tenant1 = new Tenant("tenant-1", ImmutableMap.of(GAUGE, 31));
        Tenant tenant2 = new Tenant("tenant-2", ImmutableMap.of(GAUGE, 14));


        dataAccess.insertTenant(tenant1, false).toBlocking().lastOrDefault(null);

        dataAccess.insertTenant(tenant2, false).toBlocking().lastOrDefault(null);

        Tenant actual = dataAccess.findTenant(tenant1.getId())
                                  .map(Functions::getTenant)
                                  .toBlocking().single();
        assertEquals(actual, tenant1, "The tenants do not match");
    }

    @Test
    public void doNotAllowDuplicateTenants() throws Exception {
        dataAccess.insertTenant(new Tenant("tenant-1"), false).toBlocking().lastOrDefault(null);
        ResultSet resultSet = dataAccess.insertTenant(new Tenant("tenant-1"), false)
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

        dataAccess.insertData(Observable.just(metric)).toBlocking().last();

        Observable<Row> observable = dataAccess.findTempData(new MetricId<>("tenant-1", GAUGE, "metric-1"),
                start.getMillis(), end.getMillis(), 0, Order.DESC, DEFAULT_PAGE_SIZE);
        List<DataPoint<Double>> actual = ImmutableList.copyOf(observable
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

        Metric<Double> metric = new Metric<>(new MetricId<>(tenantId, GAUGE, "metric-1"), asList(
                new DataPoint<>(start.getMillis(), 1.23),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 1.234),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 1.234),
                new DataPoint<>(end.getMillis(), 1.234)
        ));

        dataAccess.insertData(Observable.just(metric)).toBlocking().last();

        Observable<Row> observable = dataAccess.findTempData(new MetricId<>("tenant-1", GAUGE, "metric-1"),
                start.getMillis(), end.getMillis(), 0, Order.DESC, DEFAULT_PAGE_SIZE);
        List<DataPoint<Double>> actual = ImmutableList.copyOf(observable
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

        dataAccess.insertAvailabilityData(metric, 360).toBlocking().lastOrDefault(null);

        List<DataPoint<AvailabilityType>> actual = dataAccess
                .findAvailabilityData(new MetricId<>(tenantId, AVAILABILITY, "m1"), start.getMillis(), end.getMillis(),
                        0, Order.DESC, DEFAULT_PAGE_SIZE)
                .map(Functions::getAvailabilityDataPoint)
                .toList().toBlocking().lastOrDefault(null);
        List<DataPoint<AvailabilityType>> expected = singletonList(new DataPoint<AvailabilityType>(start.getMillis(),
                UP));

        assertEquals(actual, expected, "The availability data does not match the expected values");
    }

    @Test
    public void findAllMetricsPartitionKeys() throws Exception {
        long start = now().getMillis();

        Observable.from(asList(
                new Metric<>(new MetricId<>("t1", GAUGE, "m1"), singletonList(new DataPoint<>(start, 0.1))),
                new Metric<>(new MetricId<>("t1", GAUGE, "m2"), singletonList(new DataPoint<>(start+1, 0.1))),
                new Metric<>(new MetricId<>("t1", GAUGE, "m3"), singletonList(new DataPoint<>(start+2, 0.1))),
                new Metric<>(new MetricId<>("t1", GAUGE, "m4"), singletonList(new DataPoint<>(start+3, 0.1)))))
                .flatMap(m -> dataAccess.insertData(Observable.just(m)))
                .toBlocking().lastOrDefault(null);

        @SuppressWarnings("unchecked")
        List<Metric<Double>> metrics = toList(dataAccess.findAllMetricsInData()
                .compose(new MetricFromFullDataRowTransformer(Duration.standardDays(7).toStandardSeconds().getSeconds
                        ()))
                .map(m -> (Metric<Double>) m));

        assertEquals(metrics.size(), 4);
    }

    @Test
    public void testBucketIndexes() throws Exception {
        ZonedDateTime of = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime limit = ZonedDateTime.of(2017, 1, 1, 23, 59, 0, 0, ZoneOffset.UTC);
        int bucketIndex = dataAccess.getBucketIndex(of.toInstant().toEpochMilli());
        assertEquals(0, bucketIndex);

        bucketIndex = dataAccess.getBucketIndex(limit.toInstant().toEpochMilli());
        assertEquals(11, bucketIndex);
    }
}
