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

package org.hawkular.metrics.clients.ptrans.fullstack;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import static org.hawkular.metrics.clients.ptrans.fullstack.ServerDataHelper.BASE_URI;
import static org.hawkular.metrics.clients.ptrans.util.ProcessUtil.kill;
import static org.hawkular.metrics.clients.ptrans.util.TenantUtil.getRandomTenantId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.hawkular.metrics.clients.ptrans.ConfigurationKey;
import org.hawkular.metrics.clients.ptrans.ExecutableITestBase;
import org.hawkular.metrics.clients.ptrans.Service;
import org.jmxtrans.embedded.EmbeddedJmxTrans;
import org.jmxtrans.embedded.QueryResult;
import org.jmxtrans.embedded.config.ConfigurationParser;
import org.jmxtrans.embedded.output.AbstractOutputWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import jnr.constants.platform.Signal;

/**
 * @author Thomas Segismont
 */
public class GraphiteITest extends ExecutableITestBase {

    private String tenant;
    private EmbeddedJmxTrans embeddedJmxTrans;


    @Before
    public void setUp() throws Exception {
        tenant = getRandomTenantId();
        configureEmbeddedJmxTrans();
        configurePTrans();
    }

    private void configureEmbeddedJmxTrans() throws Exception {
        ConfigurationParser configurationParser = new ConfigurationParser();
        embeddedJmxTrans = configurationParser.newEmbeddedJmxTrans("classpath:jmxtrans.json");
    }

    public void configurePTrans() throws Exception {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(ptransConfFile)) {
            properties.load(in);
        }
        properties.setProperty(ConfigurationKey.SERVICES.toString(), Service.GRAPHITE.getExternalForm());
        String restUrl = "http://" + BASE_URI + "/gauges/data";
        properties.setProperty(ConfigurationKey.REST_URL.toString(), restUrl);
        properties.setProperty(ConfigurationKey.TENANT.toString(), tenant);
        try (OutputStream out = new FileOutputStream(ptransConfFile)) {
            properties.store(out, "");
        }
    }

    @Test
    public void shouldFindGraphiteMetricsOnServer() throws Exception {
        ptransProcessBuilder.command().addAll(ImmutableList.of("-c", ptransConfFile.getAbsolutePath()));
        ptransProcess = ptransProcessBuilder.start();
        assertPtransHasStarted(ptransProcess, ptransOut);

        embeddedJmxTrans.start();

        waitForEmbeddedJmxTransValues();

        embeddedJmxTrans.stop();

        Thread.sleep(MILLISECONDS.convert(1, SECONDS)); // Wait to make sure pTrans can send everything

        kill(ptransProcess, Signal.SIGTERM);
        ptransProcess.waitFor();

        List<Point> expectedData = getExpectedData();

        ServerDataHelper serverDataHelper = new ServerDataHelper(tenant);
        List<Point> serverData = serverDataHelper.getServerData();

        String failureMsg = String.format(
                Locale.ROOT, "Expected:%n%s%nActual:%n%s%n",
                pointsToString(expectedData), pointsToString(serverData)
        );

        assertEquals(failureMsg, expectedData.size(), serverData.size());

        for (int i = 0; i < expectedData.size(); i++) {
            Point expectedPoint = expectedData.get(i);
            Point serverPoint = serverData.get(i);

            long timeDiff = expectedPoint.getTimestamp() - serverPoint.getTimestamp();
            assertTrue(failureMsg, Math.abs(timeDiff) < 1);

            assertEquals(failureMsg, expectedPoint.getName(), serverPoint.getName());
            assertEquals(failureMsg, expectedPoint.getValue(), serverPoint.getValue(), 0.1);
        }
    }

    private List<Point> getExpectedData() {
        List<QueryResult> results = ListAppenderWriter.results;
        return results.stream()
                      .map(this::queryResultToPoint)
                      .sorted(Comparator.comparing(Point::getName).thenComparing(Point::getTimestamp))
                      .collect(toList());
    }

    private Point queryResultToPoint(QueryResult result) {
        return new Point(
                result.getName(),
                MILLISECONDS.convert(result.getEpoch(SECONDS), SECONDS),
                Double.valueOf(String.valueOf(result.getValue()))
        );
    }

    private String pointsToString(List<Point> data) {
        return data.stream().map(Point::toString).collect(joining(System.getProperty("line.separator")));
    }

    private void waitForEmbeddedJmxTransValues() throws Exception {
        do {
            Thread.sleep(MILLISECONDS.convert(1, SECONDS));
        } while (ListAppenderWriter.results.size() < 10);
    }

    @After
    public void tearDown() throws Exception {
        if (embeddedJmxTrans != null) {
            embeddedJmxTrans.stop();
        }
    }

    public static final class ListAppenderWriter extends AbstractOutputWriter {
        private static final List<QueryResult> results = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void write(Iterable<QueryResult> results) {
            results.forEach(ListAppenderWriter.results::add);
        }
    }
}
