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
package org.hawkular.metrics.core.service.metrics;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

import static org.hawkular.metrics.core.service.DataAccessImpl.DPART;
import static org.hawkular.metrics.core.service.TimeUUIDUtils.getTimeUUID;
import static org.hawkular.metrics.model.AvailabilityType.ADMIN;
import static org.hawkular.metrics.model.AvailabilityType.DOWN;
import static org.hawkular.metrics.model.AvailabilityType.UNKNOWN;
import static org.hawkular.metrics.model.AvailabilityType.UP;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.joda.time.DateTime.now;
import static org.joda.time.Days.days;
import static org.joda.time.Hours.hours;
import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;

import org.hawkular.metrics.core.service.DelegatingDataAccess;
import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.Tenant;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.collect.ImmutableMap;

import rx.Observable;

public class AvailabilityITest extends BaseMetricsITest {

    protected PreparedStatement insertAvailabilityDateWithTimestamp;

    @BeforeClass
    public void initClass() {
        super.initClass();
        insertAvailabilityDateWithTimestamp = session.prepare(
                "INSERT INTO data (tenant_id, type, metric, dpart, time, availability) " +
                        "VALUES (?, ?, ?, ?, ?, ?) " +
                        "USING TTL ? AND TIMESTAMP ?");
    }

    @Test
    public void addAvailabilityForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        metricsService.createTenant(new Tenant(tenantId), false).toBlocking().lastOrDefault(null);

        Metric<AvailabilityType> m1 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "m1"), asList(
                new DataPoint<>(start.plusSeconds(10).getMillis(), UP),
                new DataPoint<>(start.plusSeconds(20).getMillis(), DOWN)));
        Metric<AvailabilityType> m2 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "m2"), asList(
                new DataPoint<>(start.plusSeconds(15).getMillis(), DOWN),
                new DataPoint<>(start.plusSeconds(30).getMillis(), UP)));
        Metric<AvailabilityType> m3 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "m3"));

        doAction(() -> metricsService.addDataPoints(AVAILABILITY, Observable.just(m1, m2, m3)));

        List<DataPoint<AvailabilityType>> actual = metricsService.findDataPoints(m1.getMetricId(),
                start.getMillis(), end.getMillis(), 0, Order.ASC).toList().toBlocking().last();
        assertEquals(actual, m1.getDataPoints(), "The availability data does not match expected values");

        actual = metricsService.findDataPoints(m2.getMetricId(), start.getMillis(), end.getMillis(), 0, Order.ASC)
                .toList().toBlocking().last();
        assertEquals(actual, m2.getDataPoints(), "The availability data does not match expected values");

        actual = metricsService.findDataPoints(m3.getMetricId(), start.getMillis(), end.getMillis(), 0, Order.DESC)
                .toList().toBlocking().last();
        assertEquals(actual.size(), 0, "Did not expect to get back results since there is no data for " + m3);

        Metric<AvailabilityType> m4 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "m4"), emptyMap(), 24,
                asList(
                        new DataPoint<>(start.plusMinutes(2).getMillis(), UP),
                        new DataPoint<>(end.plusMinutes(2).getMillis(), UP)));
        doAction(() -> metricsService.createMetric(m4, false));

        doAction(() -> metricsService.addDataPoints(AVAILABILITY, Observable.just(m4)));

        actual = metricsService.findDataPoints(m4.getMetricId(), start.getMillis(), end.getMillis(), 0, Order.DESC)
                .toList().toBlocking().last();
        Metric<AvailabilityType> expected = new Metric<>(m4.getMetricId(), emptyMap(), 24,
                singletonList(new DataPoint<>(start.plusMinutes(2).getMillis(), UP)));
        assertEquals(actual, expected.getDataPoints(), "The availability data does not match expected values");

        assertMetricIndexMatches(tenantId, AVAILABILITY, asList(
                new Metric<>(m1.getMetricId(), 7),
                new Metric<>(m2.getMetricId(), 7),
                new Metric<>(m4.getMetricId(), 24)));
    }

    @Test
    public void findDistinctAvailabilities() throws Exception {
        DateTime start = now();
        DateTime end = start.plusMinutes(20);
        String tenantId = "tenant1";
        MetricId<AvailabilityType> metricId = new MetricId<>("tenant1", AVAILABILITY, "A1");

        doAction(() -> metricsService.createTenant(new Tenant(tenantId), false));

        Metric<AvailabilityType> metric = new Metric<>(metricId, asList(
                new DataPoint<>(start.getMillis(), UP),
                new DataPoint<>(start.plusMinutes(1).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(3).getMillis(), UP),
                new DataPoint<>(start.plusMinutes(4).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(5).getMillis(), UP),
                new DataPoint<>(start.plusMinutes(6).getMillis(), UP),
                new DataPoint<>(start.plusMinutes(7).getMillis(), UNKNOWN),
                new DataPoint<>(start.plusMinutes(8).getMillis(), UNKNOWN),
                new DataPoint<>(start.plusMinutes(9).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(10).getMillis(), ADMIN),
                new DataPoint<>(start.plusMinutes(11).getMillis(), UP)));

        doAction(() -> metricsService.addDataPoints(AVAILABILITY, Observable.just(metric)));

        List<DataPoint<AvailabilityType>> actual = metricsService.findAvailabilityData(metricId,
                start.getMillis(), end.getMillis(), true, 0, Order.ASC)
                .doOnError(Throwable::printStackTrace)
                .toList().toBlocking().lastOrDefault(null);

        List<DataPoint<AvailabilityType>> expected = asList(
                metric.getDataPoints().get(0),
                metric.getDataPoints().get(1),
                metric.getDataPoints().get(3),
                metric.getDataPoints().get(4),
                metric.getDataPoints().get(5),
                metric.getDataPoints().get(7),
                metric.getDataPoints().get(9),
                metric.getDataPoints().get(10),
                metric.getDataPoints().get(11));

        assertEquals(actual, expected, "The availability data does not match the expected values");
    }

//    @Test
    // TODO Fix to match Compressed data
    public void verifyTTLsSetOnAvailabilityData() throws Exception {
        DateTime start = now().minusMinutes(10);

        metricsService.createTenant(new Tenant("t1"), false).toBlocking().lastOrDefault(null);
        metricsService.createTenant(
                new Tenant("t2", ImmutableMap.of(AVAILABILITY, 14)), false).toBlocking().lastOrDefault(null);

        VerifyTTLDataAccess verifyTTLDataAccess = new VerifyTTLDataAccess(dataAccess);

        metricsService.setDataAccess(verifyTTLDataAccess);
        metricsService.setDataAccess(verifyTTLDataAccess);

        List<DataPoint<AvailabilityType>> dataPoints = asList(
                new DataPoint<>(start.getMillis(), UP),
                new DataPoint<>(start.plusMinutes(1).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN));
        Metric<AvailabilityType> m1 = new Metric<>(new MetricId<>("t1", AVAILABILITY, "m1"), dataPoints);

        addAvailabilityDataInThePast(m1, days(2).toStandardDuration());

        verifyTTLDataAccess.setAvailabilityTTL(days(14).toStandardSeconds().getSeconds());
        Metric<AvailabilityType> m2 = new Metric<>(new MetricId<>("t2", AVAILABILITY, "m2"),
                singletonList(new DataPoint<>(start.plusMinutes(5).getMillis(), UP)));

        addAvailabilityDataInThePast(m2, days(5).toStandardDuration());

        metricsService.createTenant(new Tenant("t3", ImmutableMap.of(AVAILABILITY, 24)), false)
                .toBlocking()
                .lastOrDefault(null);
        verifyTTLDataAccess.setAvailabilityTTL(hours(24).toStandardSeconds().getSeconds());
        Metric<AvailabilityType> m3 = new Metric<>(new MetricId<>("t3", AVAILABILITY, "m3"),
                singletonList(new DataPoint<>(start.getMillis(), UP)));

        metricsService.addDataPoints(AVAILABILITY, Observable.just(m3)).toBlocking();
    }

    private void addAvailabilityDataInThePast(Metric<AvailabilityType> metric, final Duration duration)
            throws Exception {
        try {
            metricsService.setDataAccess(new DelegatingDataAccess(dataAccess) {
//                @Override
                public Observable<Integer> insertAvailabilityData(Metric<AvailabilityType> m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (DataPoint<AvailabilityType> a : m.getDataPoints()) {
                        batchStatement.add(insertAvailabilityDateWithTimestamp.bind(m.getMetricId().getTenantId(),
                                AVAILABILITY.getCode(), m.getMetricId().getName(), DPART,
                                getTimeUUID(a.getTimestamp()),
                                getBytes(a), actualTTL, writeTime));
                    }
                    return rxSession.execute(batchStatement).map(resultSet -> batchStatement.size());
                }
            });
            metricsService.addDataPoints(AVAILABILITY, Observable.just(metric));
        } finally {
            metricsService.setDataAccess(dataAccess);
        }
    }

    private ByteBuffer getBytes(DataPoint<AvailabilityType> dataPoint) {
        return ByteBuffer.wrap(new byte[] { dataPoint.getValue().getCode() });
    }
}
