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
package org.hawkular.metrics.clients.ptrans.fullstack;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hawkular.metrics.clients.ptrans.data.ServerDataHelper.BASE_URI;
import static org.hawkular.metrics.clients.ptrans.util.ProcessUtil.kill;
import static org.hawkular.metrics.clients.ptrans.util.TenantUtil.getRandomTenantId;
import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

import org.hawkular.metrics.clients.ptrans.ConfigurationKey;
import org.hawkular.metrics.clients.ptrans.data.Point;
import org.hawkular.metrics.clients.ptrans.data.ServerDataHelper;
import org.hawkular.metrics.clients.ptrans.exec.ExecutableITestBase;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import jnr.constants.platform.Signal;

/**
 * @author Thomas Segismont
 */
abstract class FullStackITest extends ExecutableITestBase {
    private String tenant;

    @Before
    public void setUp() throws Exception {
        tenant = getRandomTenantId();
        configureSource();
        configurePTrans();
    }

    private void configurePTrans() throws Exception {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(ptransConfFile)) {
            properties.load(in);
        }
        String restUrl = "http://" + BASE_URI;
        properties.setProperty(ConfigurationKey.METRICS_URL.toString(), restUrl);
        properties.setProperty(ConfigurationKey.METRICS_TENANT.toString(), tenant);
        changePTransConfig(properties);
        try (OutputStream out = new FileOutputStream(ptransConfFile)) {
            properties.store(out, "");
        }
    }

    @Test
    public void shouldFindMetricsOnServer() throws Exception {
        ptransProcessBuilder.command().addAll(ImmutableList.of("-c", ptransConfFile.getAbsolutePath()));
        ptransProcess = ptransProcessBuilder.start();
        assertPtransHasStarted(ptransProcess, ptransOut);

        startSource();
        waitForSourceValues();
        stopSource();

        Thread.sleep(MILLISECONDS.convert(2, SECONDS)); // Wait to make sure pTrans can send everything

        kill(ptransProcess, Signal.SIGTERM);
        ptransProcess.waitFor();

        List<Point> expectedData = getExpectedData();
        Collections.sort(expectedData, Point.POINT_COMPARATOR);

        ServerDataHelper serverDataHelper = new ServerDataHelper(tenant);
        List<Point> serverData = serverDataHelper.getServerData().stream()
                .filter(point -> !point.getName().startsWith("vertx."))
                .sorted(Point.POINT_COMPARATOR)
                .collect(Collectors.toList());

        String failureMsg = String.format(
                Locale.ROOT, "Expected:%n%s%nActual:%n%s%n",
                Point.listToString(expectedData), Point.listToString(serverData)
        );

        assertEquals(failureMsg, expectedData.size(), serverData.size());

        for (int i = 0; i < expectedData.size(); i++) {
            Point expectedPoint = expectedData.get(i);
            Point serverPoint = serverData.get(i);

            checkTimestamps(failureMsg, expectedPoint.getTimestamp(), serverPoint.getTimestamp());
            assertEquals(failureMsg, expectedPoint.getName(), serverPoint.getName());
            assertEquals(failureMsg, expectedPoint.getValue(), serverPoint.getValue(), 0.1);
        }
    }

    protected abstract void configureSource() throws Exception;

    protected abstract void changePTransConfig(Properties properties);

    protected abstract void startSource() throws Exception;

    protected abstract void waitForSourceValues() throws Exception;

    protected abstract void stopSource() throws Exception;

    protected abstract List<Point> getExpectedData() throws Exception;

    protected abstract void checkTimestamps(String failureMsg, long expected, long actual);
}
