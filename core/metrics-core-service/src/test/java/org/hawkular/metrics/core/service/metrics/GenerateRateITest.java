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
import static java.util.UUID.randomUUID;

import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.tasks.api.SingleExecutionTrigger;
import org.hawkular.metrics.tasks.api.Trigger;
import org.hawkular.metrics.tasks.impl.Task2Impl;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import rx.Observable;

/**
 * This class tests counter rates by directly calling {@link GenerateRate}. There is no
 * task scheduler running for these tests.
 */
public class GenerateRateITest extends BaseMetricsITest {

    @Test
    public void generateRates() {
        DateTime start = now().minusMinutes(5);
        String tenant = "rates-test";

        Metric<Long> c1 = new Metric<>(new MetricId<>(tenant, COUNTER, "C1"));
        Metric<Long> c2 = new Metric<>(new MetricId<>(tenant, COUNTER, "C2"));
        Metric<Long> c3 = new Metric<>(new MetricId<>(tenant, COUNTER, "C3"));

        doAction(() -> metricsService.createMetric(c1, false));
        doAction(() -> metricsService.createMetric(c2, false));
        doAction(() -> metricsService.createMetric(c3, false));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.from(asList(
                new Metric<>(c1.getMetricId(), asList(new DataPoint<>(start.getMillis(), 10L),
                        new DataPoint<>(start.plusSeconds(30).getMillis(), 25L))),
                new Metric<>(c2.getMetricId(), asList(new DataPoint<>(start.getMillis(), 100L),
                        new DataPoint<>(start.plusSeconds(30).getMillis(), 165L))),
                new Metric<>(c3.getMetricId(), asList(new DataPoint<>(start.getMillis(), 42L),
                        new DataPoint<>(start.plusSeconds(30).getMillis(), 77L)))
        ))));

        GenerateRate generateRate = new GenerateRate(metricsService);

        Trigger trigger = new SingleExecutionTrigger(start.getMillis());
        Task2Impl task = new Task2Impl(randomUUID(), tenant, 0, "generate-rates", ImmutableMap.of("tenant", tenant),
                trigger);

        generateRate.call(task);

        List<DataPoint<Double>> c1Rate = getOnNextEvents(() -> metricsService.findRateData(c1.getMetricId(),
                start.getMillis(), start.plusMinutes(1).getMillis(), 0, Order.ASC));
        List<DataPoint<Double>> c2Rate = getOnNextEvents(() -> metricsService.findRateData(c2.getMetricId(),
                start.getMillis(), start.plusMinutes(1).getMillis(), 0, Order.ASC));
        List<DataPoint<Double>> c3Rate = getOnNextEvents(() -> metricsService.findRateData(c3.getMetricId(),
                start.getMillis(), start.plusMinutes(1).getMillis(), 0, Order.ASC));

        assertEquals(c1Rate, singletonList(new DataPoint<>(start.plusSeconds(30).getMillis(), calculateRate(30, start,
                start.plusMinutes(1)))));
        assertEquals(c2Rate, singletonList(new DataPoint<>(start.plusSeconds(30).getMillis(), calculateRate(130, start,
                start.plusMinutes(1)))));
        assertEquals(c3Rate, singletonList(new DataPoint<>(start.plusSeconds(30).getMillis(), calculateRate(70, start,
                start.plusMinutes(1)))));
    }

}
