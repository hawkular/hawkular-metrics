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
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.rxjava.core.http.HttpClientResponse;
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
        List<GaugeDataPoint> dataPoints = asList(
                new GaugeDataPoint(System.currentTimeMillis() - 500, 10.1),
                new GaugeDataPoint(System.currentTimeMillis() - 400, 11.1112),
                new GaugeDataPoint(System.currentTimeMillis() - 300, 13.4783)
        );

        TestSubscriber<HttpClientResponse> subscriber = new TestSubscriber<>();

        client.addGaugeData(tenantId, gauge, dataPoints).subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        assertEquals(subscriber.getOnNextEvents().size(), 1, "Expected to get back one http response");

        HttpClientResponse response = subscriber.getOnNextEvents().get(0);
        assertEquals(200, response.statusCode(), "Expected a 200 status when data points are successfully stored");
    }

}
