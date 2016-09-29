/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hawkular.metrics.model.MetricType.STRING;
import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import rx.Observable;

public class StringITest extends BaseMetricsITest {

    private static final int MAX_STRING_LENGTH = 2048;

    @Test
    public void addAndFetchStringData() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "string-tenant";
        MetricId<String> metricId = new MetricId<>(tenantId, STRING, "S1");

        Metric<String> metric = new Metric<>(metricId, asList(
                new DataPoint<>(start.getMillis(), "X"),
                new DataPoint<>(start.plusMinutes(2).getMillis(), "Y"),
                new DataPoint<>(start.plusMinutes(4).getMillis(), "A"),
                new DataPoint<>(end.getMillis(), "B")));

        doAction(() -> metricsService.addDataPoints(STRING, Observable.just(metric)));
        List<DataPoint<String>> actual = getOnNextEvents(() -> metricsService.findDataPoints(metricId,
                start.getMillis(), end.getMillis(), 0, Order.DESC));
        List<DataPoint<String>> expected = asList(
                new DataPoint<>(start.plusMinutes(4).getMillis(), "A"),
                new DataPoint<>(start.plusMinutes(2).getMillis(), "Y"),
                new DataPoint<>(start.getMillis(), "X"));

        assertEquals(actual, expected, "The data does not match the expected values");
        assertMetricIndexMatches(tenantId, STRING, singletonList(new Metric<>(metricId, DEFAULT_TTL)));
    }

    @Test
    public void doNotAllowStringsThatExceedMaxLength() throws Exception {
        char[] chars = new char[MAX_STRING_LENGTH + 1];
        Arrays.fill(chars, 'X');
        String string = new String(chars);
        String tenantId = "string-tenant";
        MetricId<String> metricId = new MetricId<>(tenantId, STRING, "S1");
        Metric<String> metric = new Metric<>(metricId, singletonList(new DataPoint<>(12345L, string)));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();

        metricsService.addDataPoints(STRING, Observable.just(metric)).subscribe(
                aVoid -> latch.countDown(),
                t -> {
                    exceptionRef.set(t);
                    latch.countDown();
                });
        latch.await(10, SECONDS);

        assertNotNull(exceptionRef.get());
        assertTrue(exceptionRef.get() instanceof IllegalArgumentException);
    }

    @Test
    public void findDistinctStrings() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(20);
        String tenantId = "Distinct Strings";
        MetricId<String> metricId = new MetricId<>(tenantId, STRING, "S1");

        Metric<String> metric = new Metric<>(metricId, asList(
                new DataPoint<>(start.getMillis(), "up"),
                new DataPoint<>(start.plusMinutes(1).getMillis(), "down"),
                new DataPoint<>(start.plusMinutes(2).getMillis(), "down"),
                new DataPoint<>(start.plusMinutes(3).getMillis(), "up"),
                new DataPoint<>(start.plusMinutes(4).getMillis(), "down"),
                new DataPoint<>(start.plusMinutes(5).getMillis(), "up"),
                new DataPoint<>(start.plusMinutes(6).getMillis(), "up"),
                new DataPoint<>(start.plusMinutes(7).getMillis(), "unknown"),
                new DataPoint<>(start.plusMinutes(8).getMillis(), "unknown"),
                new DataPoint<>(start.plusMinutes(9).getMillis(), "down"),
                new DataPoint<>(start.plusMinutes(10).getMillis(), "up")));

        doAction(() -> metricsService.addDataPoints(STRING, Observable.just(metric)));

        List<DataPoint<String>> actual = getOnNextEvents(() -> metricsService.findStringData(metricId,
                start.getMillis(), end.getMillis(), true, 0, Order.ASC));
        List<DataPoint<String>> expected = asList(
                metric.getDataPoints().get(0),
                metric.getDataPoints().get(1),
                metric.getDataPoints().get(3),
                metric.getDataPoints().get(4),
                metric.getDataPoints().get(5),
                metric.getDataPoints().get(7),
                metric.getDataPoints().get(9),
                metric.getDataPoints().get(10));

        assertEquals(actual, expected, "The string data does not match the expected values");
    }
}
