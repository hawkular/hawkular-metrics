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

import io.vertx.rxjava.core.http.HttpClientResponse;
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

        TestSubscriber<HttpClientResponse> writeSubscriber = new TestSubscriber<>();

        client.addGaugeData(tenantId, gauge, dataPoints).subscribe(writeSubscriber);

        writeSubscriber.awaitTerminalEvent();
        writeSubscriber.assertNoErrors();
        assertEquals(writeSubscriber.getOnNextEvents().size(), 1, "Expected to get back one http response");

        HttpClientResponse response = writeSubscriber.getOnNextEvents().get(0);
        assertEquals(200, response.statusCode(), "Expected a 200 status when data points are successfully stored");

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

        TestSubscriber<HttpClientResponse> writeSubscriber = new TestSubscriber<>();

        client.addGaugeData(tenantId, gauges).subscribe(writeSubscriber);

        writeSubscriber.awaitTerminalEvent();
        writeSubscriber.assertNoErrors();
        assertEquals(writeSubscriber.getOnNextEvents().size(), 1, "Expected to get back one http response");

        HttpClientResponse response = writeSubscriber.getOnNextEvents().get(0);
        assertEquals(200, response.statusCode(), "Expected a 200 status when data points are successfully stored");

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

}
