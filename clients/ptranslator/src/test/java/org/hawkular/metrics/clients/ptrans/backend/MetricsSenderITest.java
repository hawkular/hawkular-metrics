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
package org.hawkular.metrics.clients.ptrans.backend;

import static java.util.concurrent.TimeUnit.MINUTES;

import static org.hawkular.metrics.clients.ptrans.backend.Constants.METRIC_ADDRESS;
import static org.hawkular.metrics.clients.ptrans.util.TenantUtil.getRandomTenantId;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.hawkular.metrics.client.common.SingleMetric;
import org.hawkular.metrics.clients.ptrans.Configuration;
import org.hawkular.metrics.clients.ptrans.ConfigurationKey;
import org.hawkular.metrics.clients.ptrans.data.Point;
import org.hawkular.metrics.clients.ptrans.data.ServerDataHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import io.vertx.core.Vertx;

/**
 * @author Thomas Segismont
 */
public class MetricsSenderITest {
    private static final String BASE_URI;

    static {
        BASE_URI = System.getProperty("hawkular-metrics.base-uri", "127.0.0.1:8080/hawkular/metrics");
    }

    @Rule
    public final Timeout timeout = new Timeout(1, MINUTES);

    private String tenant;
    private AtomicLong idGenerator;
    private Vertx vertx;

    @Before
    public void setUp() throws Exception {
        tenant = getRandomTenantId();
        idGenerator = new AtomicLong(0);
        String addGaugeDataUrl = "http://" + BASE_URI + "/gauges/data";

        Properties properties = new Properties();
        properties.setProperty(ConfigurationKey.REST_URL.toString(), addGaugeDataUrl);
        properties.setProperty(ConfigurationKey.TENANT.toString(), tenant);
        Configuration configuration = Configuration.from(properties);

        vertx = Vertx.vertx();

        CountDownLatch latch = new CountDownLatch(1);
        vertx.deployVerticle(new MetricsSender(configuration), handler -> latch.countDown());
        latch.await();

    }

    @Test
    public void shouldForwardMetrics() throws Exception {
        int total = 3000;

        List<Point> expectedData = new ArrayList<>(total);
        List<SingleMetric> metrics = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            long id = idGenerator.incrementAndGet();
            String name = String.valueOf(id);
            expectedData.add(new Point(name, id, (double) id));
            metrics.add(new SingleMetric(name, id, (double) id));
        }

        for (int i = 0; !metrics.isEmpty(); i++) {
            // Simulate time before a server publishes another batch of metrics
            long wait = i % 2 == 0 ? 12 : 25;
            // Simulate servers publishing N metrics in a row
            int count = i % 2 == 0 ? 63 : 12;

            Thread.sleep(wait);
            List<SingleMetric> batch = metrics.subList(0, Math.min(count, metrics.size()));
            batch.forEach(metric -> vertx.eventBus().publish(METRIC_ADDRESS, metric));
            batch.clear();
        }

        ServerDataHelper dataHelper = new ServerDataHelper(tenant);
        List<Point> serverData;
        do {
            Thread.sleep(1000);
            serverData = dataHelper.getServerData();
        }
        while (serverData.size() != expectedData.size());

        Collections.sort(expectedData, Point.POINT_COMPARATOR);
        Collections.sort(serverData, Point.POINT_COMPARATOR);

        String failureMsg = String.format(
                Locale.ROOT, "Expected:%n%s%nActual:%n%s%n",
                Point.listToString(expectedData), Point.listToString(serverData)
        );

        for (int i = 0; i < expectedData.size(); i++) {
            Point expectedPoint = expectedData.get(i);
            Point serverPoint = serverData.get(i);

            assertEquals(failureMsg, expectedPoint.getTimestamp(), serverPoint.getTimestamp());
            assertEquals(failureMsg, expectedPoint.getName(), serverPoint.getName());
            assertEquals(failureMsg, expectedPoint.getValue(), serverPoint.getValue(), 0.1);
        }
    }

    @After
    public void tearDown() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        vertx.close(h -> latch.countDown());
        latch.await();
    }
}