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
import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.joda.time.DateTime.now;
import static org.joda.time.Duration.standardSeconds;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.tasks.api.TaskService;
import org.hawkular.metrics.tasks.api.TaskServiceBuilder;
import org.hawkular.metrics.tasks.impl.TaskServiceImpl;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import rx.Observable;

/**
 * @author jsanda
 */
public class RatesITest extends MetricsITest {

    private MetricsServiceImpl metricsService;

    private TaskService taskService;

    private DateTimeService dateTimeService;

    @BeforeClass
    public void initClass() {
        initSession();

        dateTimeService = new DateTimeService();

        taskService = new TaskServiceBuilder()
                .withSession(session)
                .withTimeUnit(TimeUnit.SECONDS)
                .withTaskTypes(singletonList(TaskTypes.COMPUTE_RATE))
                .build();
        ((TaskServiceImpl) taskService).setTimeUnit(TimeUnit.SECONDS);

        metricsService = new MetricsServiceImpl();
        metricsService.setTaskService(taskService);

        ((TaskServiceImpl) taskService).subscribe(TaskTypes.COMPUTE_RATE, new GenerateRate(metricsService));

        String keyspace = "hawkulartest";
        System.setProperty("keyspace", keyspace);

        taskService.start();
        metricsService.startUp(session, keyspace, false, new MetricRegistry());
    }

    @BeforeMethod
    public void initMethod() {
        session.execute("TRUNCATE metrics_idx");
        session.execute("TRUNCATE data");
        session.execute("TRUNCATE leases");
        session.execute("TRUNCATE task_queue");
    }

    @AfterClass
    public void shutdown() {
        taskService.shutdown();
    }

    @Test
    public void generateRates() throws Exception {
        String tenantId = "generate-rates-test";
        MetricId id = new MetricId("c1");
        DateTime start = dateTimeService.getTimeSlice(now(), standardSeconds(5)).plusSeconds(5);
        DateTime end = start.plusSeconds(30);


        Metric<Long> counter = new Metric<>(tenantId, COUNTER, id, asList(
                new DataPoint<>(start.plusMillis(50).getMillis(), 11L),
                new DataPoint<>(start.plusSeconds(1).getMillis(), 17L),
                new DataPoint<>(start.plusSeconds(11).getMillis(), 29L),
                new DataPoint<>(start.plusSeconds(17).getMillis(), 46L),
                new DataPoint<>(start.plusSeconds(21).getMillis(), 69L)
        ));
        metricsService.createMetric(counter).toBlocking().lastOrDefault(null);
        metricsService.addCounterData(Observable.just(counter)).toBlocking().lastOrDefault(null);

        while (now().isBefore(end)) {
            Thread.sleep(100);
        }

        DataPoint<Double> actual = metricsService.findRateData(tenantId, id, start.getMillis(),
                start.plusSeconds(5).getMillis()).toBlocking().last();
        DataPoint<Double> expected = new DataPoint<>(start.getMillis(), calculateRate(17, start, start.plusSeconds(5)));
        assertEquals(actual, expected, "The rate for " + start + " does not match the expected value");

        actual = metricsService.findRateData(tenantId, id, start.plusSeconds(10).getMillis(),
                start.plusSeconds(15).getMillis()).toBlocking().last();
        expected = new DataPoint<>(start.plusSeconds(10).getMillis(), calculateRate(29, start.plusSeconds(10),
                start.plusSeconds(15)));
        assertEquals(actual, expected, "The rate for " + start.plusSeconds(10) + " does not match the expected value.");

        actual = metricsService.findRateData(tenantId, id, start.plusSeconds(15).getMillis(),
                start.plusSeconds(20).getMillis()).toBlocking().last();
        expected = new DataPoint<>(start.plusSeconds(15).getMillis(), calculateRate(46, start.plusSeconds(15),
                start.plusSeconds(20)));
        assertEquals(actual, expected, "The rate for " + start.plusSeconds(15) + " does not match the expected value.");

        actual = metricsService.findRateData(tenantId, id, start.plusSeconds(20).getMillis(),
                start.plusSeconds(25).getMillis()).toBlocking().last();
        expected = new DataPoint<>(start.plusSeconds(20).getMillis(), calculateRate(69, start.plusSeconds(20),
                start.plusSeconds(25)));
        assertEquals(actual, expected, "The rate for " + start.plusSeconds(20) + " does not match the expected value.");
    }

    private double calculateRate(double value, DateTime startTime, DateTime endTime) {
        return (value / (endTime.getMillis() - startTime.getMillis())) * 1000;
    }

}
