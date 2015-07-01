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
package org.hawkular.metrics.rest;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hawkular.metrics.rest.model.AvailabilityDataPoint;
import org.hawkular.metrics.rest.model.CounterDataPoint;
import org.hawkular.metrics.rest.model.DataPoints;
import org.hawkular.metrics.rest.model.GaugeDataPoint;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import rx.observers.TestSubscriber;

/**
 * @author jsanda
 */
public class ClientITest {

    private Client client;

    private AtomicInteger tenantIds = new AtomicInteger();

    @BeforeClass
    public void initClient() {
        String hostname = System.getProperty("server.hostname", "localhost");
        int port = Integer.parseInt(System.getProperty("server.port", "8080"));
        client = new Client(hostname, port);
    }

    @AfterClass
    public void shutdown() throws Exception {
        client.shutdown();
    }

    private String nextTenantId() {
        return "Tenant-" + tenantIds.getAndIncrement();
    }


    @Test
    public void addDataPointsForOneGauge() throws Exception {
        String tenantId = nextTenantId();
        String gauge = "addDataPointsForOneGauge";
        long endTime = System.currentTimeMillis();
        long startTime = endTime - TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
        List<GaugeDataPoint> dataPoints = asList(
                new GaugeDataPoint(startTime + 2000, 13.4783),
                new GaugeDataPoint(startTime + 1000, 11.1112),
                new GaugeDataPoint(startTime, 10.1)
        );

        TestSubscriber<Void> writeSubscriber = new TestSubscriber<>();

        client.addGaugeData(tenantId, gauge, dataPoints).subscribe(writeSubscriber);

        writeSubscriber.awaitTerminalEvent();
        writeSubscriber.assertNoErrors();

        TestSubscriber<GaugeDataPoint> readSubscriber = new TestSubscriber<>();
        client.findGaugeData(tenantId, gauge, startTime, endTime).subscribe(readSubscriber);

        readSubscriber.awaitTerminalEvent();
        readSubscriber.assertNoErrors();
        assertEquals(readSubscriber.getOnNextEvents(), dataPoints, "The data points do not match");
    }

    @Test
    public void addDataPointsForMultipleGauges() throws Exception {
        String tenantId = nextTenantId();
        String prefix="addDataForMultiple-";
        int id = 0;
        long endTime = System.currentTimeMillis();
        long startTime = endTime - TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

        List<DataPoints<GaugeDataPoint>> gauges = asList(
                new DataPoints<>(prefix + id++, asList(
                        new GaugeDataPoint(startTime + 2000, 7.421),
                        new GaugeDataPoint(startTime + 1000, 5.0367),
                        new GaugeDataPoint(startTime, 5.55))),
                new DataPoints<>(prefix + id++, asList(
                        new GaugeDataPoint(startTime, 25.474),
                        new GaugeDataPoint(endTime + 100, 24.7341)
                ))
        );

        TestSubscriber<Void> writeSubscriber = new TestSubscriber<>();

        client.addGaugeData(tenantId, gauges).subscribe(writeSubscriber);

        writeSubscriber.awaitTerminalEvent();
        writeSubscriber.assertNoErrors();

        TestSubscriber<GaugeDataPoint> readSubscriber = new TestSubscriber<>();
        client.findGaugeData(tenantId, prefix + 0, startTime, endTime).subscribe(readSubscriber);

        readSubscriber.awaitTerminalEvent();
        readSubscriber.assertNoErrors();
        assertEquals(readSubscriber.getOnNextEvents(), gauges.get(0).getData(), "The data points do not match");

        readSubscriber = new TestSubscriber<>();

        client.findGaugeData(tenantId, prefix + 1, startTime, endTime).subscribe(readSubscriber);

        readSubscriber.awaitTerminalEvent();
        readSubscriber.assertNoErrors();
        assertEquals(readSubscriber.getOnNextEvents(), singletonList(new GaugeDataPoint(startTime, 25.474)),
                "The data points do not match");
    }

    @Test
    public void addDataPointsForMultipleAvailabilityMetrics() throws Exception {
        String tenantId = nextTenantId();
        long endTime = System.currentTimeMillis();
        long startTime = endTime - TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

        List<DataPoints<AvailabilityDataPoint>> dataPoints = asList(
                new DataPoints<>("A1", asList(
                        new AvailabilityDataPoint(startTime + 2000, "up"),
                        new AvailabilityDataPoint(startTime + 1000, "down"),
                        new AvailabilityDataPoint(startTime, "down"))),
                new DataPoints<>("A2", asList(
                        new AvailabilityDataPoint(startTime, "up"),
                        new AvailabilityDataPoint(endTime + 100, "down")
                ))
        );
        TestSubscriber<Void> writeSubscriber = new TestSubscriber<>();
        client.addAvailabilty(tenantId, dataPoints).subscribe(writeSubscriber);

        writeSubscriber.awaitTerminalEvent();
        writeSubscriber.assertNoErrors();

        TestSubscriber<AvailabilityDataPoint> readSubscriber = new TestSubscriber<>();
        client.findAvailabilityData(tenantId, "A1", startTime, endTime).subscribe(readSubscriber);

        readSubscriber.awaitTerminalEvent();
        readSubscriber.assertNoErrors();

        List<AvailabilityDataPoint> expected = asList(
                new AvailabilityDataPoint(startTime, "down"),
                new AvailabilityDataPoint(startTime + 1000, "down"),
                new AvailabilityDataPoint(startTime + 2000, "up")
        );
        assertEquals(readSubscriber.getOnNextEvents(), expected, "The data points do not match");

        readSubscriber = new TestSubscriber<>();

        client.findAvailabilityData(tenantId, "A2", startTime, endTime).subscribe(readSubscriber);

        readSubscriber.awaitTerminalEvent();
        readSubscriber.assertNoErrors();
        assertEquals(readSubscriber.getOnNextEvents(), singletonList(new AvailabilityDataPoint(startTime, "up")),
                "The data points do not match");
    }

    @Test
    public void addDataPointsForMultipleCounterMetrics() throws Exception {
        String tenantId = nextTenantId();
        long endTime = System.currentTimeMillis();
        long startTime = endTime - TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

        List<DataPoints<CounterDataPoint>> dataPoints = asList(
                new DataPoints<>("C1", asList(
                        new CounterDataPoint(startTime + 2000, 50L),
                        new CounterDataPoint(startTime + 1000, 75L),
                        new CounterDataPoint(startTime, 87L))),
                new DataPoints<>("C2", asList(
                        new CounterDataPoint(startTime, 158L),
                        new CounterDataPoint(endTime + 100, 244L)
                ))
        );
        TestSubscriber<Void> writeSubscriber = new TestSubscriber<>();
        client.addCounterData(tenantId, dataPoints).subscribe(writeSubscriber);

        writeSubscriber.awaitTerminalEvent();
        writeSubscriber.assertNoErrors();

        TestSubscriber<CounterDataPoint> readSubscriber = new TestSubscriber<>();
        client.findCounterData(tenantId, "C1", startTime, endTime).subscribe(readSubscriber);

        readSubscriber.awaitTerminalEvent();
        readSubscriber.assertNoErrors();

        List<CounterDataPoint> expected = asList(
                new CounterDataPoint(startTime + 2000, 50L),
                new CounterDataPoint(startTime + 1000, 75L),
                new CounterDataPoint(startTime, 87L)
        );
        assertEquals(readSubscriber.getOnNextEvents(), expected, "The data points do not match");

        readSubscriber = new TestSubscriber<>();

        client.findCounterData(tenantId, "C2", startTime, endTime).subscribe(readSubscriber);

        readSubscriber.awaitTerminalEvent();
        readSubscriber.assertNoErrors();
        assertEquals(readSubscriber.getOnNextEvents(), singletonList(new CounterDataPoint(startTime, 158L)),
                "The data points do not match");
    }

}
