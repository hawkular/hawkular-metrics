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
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.BATCH_DELAY;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.BATCH_SIZE;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.REST_URL;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES;
import static org.hawkular.metrics.clients.ptrans.util.ProcessUtil.kill;
import static org.hawkular.metrics.clients.ptrans.util.TenantUtil.getRandomTenantId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Stream;

import jnr.constants.platform.Signal;

import org.hawkular.metrics.clients.ptrans.ExecutableITestBase;
import org.hawkular.metrics.clients.ptrans.PrintOutputOnFailureWatcher;
import org.hawkular.metrics.clients.ptrans.Service;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

/**
 * @author Thomas Segismont
 */
public class CollectdITest extends ExecutableITestBase {
    private static final String COLLECTD_PATH = System.getProperty("collectd.path", "/usr/sbin/collectd");
    private static final String BASE_URI = System.getProperty(
            "hawkular-metrics.base-uri",
            "127.0.0.1:8080/hawkular/metrics"
    );

    private String tenant;
    private String findGuageMetricsUrl;
    private File collectdConfFile;
    private File collectdOut;
    private File collectdErr;
    private ProcessBuilder collectdProcessBuilder;
    private Process collectdProcess;

    @Rule
    public final PrintOutputOnFailureWatcher collectdOutputRule = new PrintOutputOnFailureWatcher(
            "collectd",
            () -> collectdOut,
            () -> collectdErr
    );

    @Before
    public void setUp() throws Exception {
        tenant = getRandomTenantId();
        findGuageMetricsUrl = "http://" + BASE_URI + "/" + tenant + "/metric?type=gauge";
        assumeCollectdIsPresent();
        configureCollectd();
        assertCollectdConfIsValid();
        configurePTrans();
    }

    private void assumeCollectdIsPresent() {
        Path path = Paths.get(COLLECTD_PATH);
        assumeTrue(COLLECTD_PATH + " does not exist", Files.exists(path));
        assumeTrue(COLLECTD_PATH + " is not a file", Files.isRegularFile(path));
        assumeTrue(COLLECTD_PATH + " is not executable", Files.isExecutable(path));
    }

    private void configureCollectd() throws Exception {
        collectdConfFile = temporaryFolder.newFile();
        try (FileOutputStream out = new FileOutputStream(collectdConfFile)) {
            Resources.copy(Resources.getResource("collectd.conf"), out);
        }
    }

    private void assertCollectdConfIsValid() throws Exception {
        collectdOut = temporaryFolder.newFile();
        collectdErr = temporaryFolder.newFile();

        collectdProcessBuilder = new ProcessBuilder();
        collectdProcessBuilder.directory(temporaryFolder.getRoot());
        collectdProcessBuilder.redirectOutput(collectdOut);
        collectdProcessBuilder.redirectError(collectdErr);

        collectdProcessBuilder.command(COLLECTD_PATH, "-C", collectdConfFile.getAbsolutePath(), "-t", "-T", "-f");

        collectdProcess = collectdProcessBuilder.start();

        int exitCode = collectdProcess.waitFor();
        assertEquals("Collectd configuration doesn't seem to be valid", 0, exitCode);

        boolean hasErrorInLog = Stream.concat(Files.lines(collectdOut.toPath()), Files.lines(collectdErr.toPath()))
                                      .anyMatch(l -> l.contains("[error]"));
        assertFalse("Collectd configuration doesn't seem to be valid", hasErrorInLog);
    }

    public void configurePTrans() throws Exception {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(ptransConfFile)) {
            properties.load(in);
        }
        properties.setProperty(SERVICES.getExternalForm(), Service.COLLECTD.getExternalForm());
        properties.setProperty(BATCH_DELAY.getExternalForm(), String.valueOf(1));
        properties.setProperty(BATCH_SIZE.getExternalForm(), String.valueOf(1));
        String restUrl = "http://" + BASE_URI + "/" + tenant + "/gauge/data";
        properties.setProperty(REST_URL.getExternalForm(), restUrl);
        try (OutputStream out = new FileOutputStream(ptransConfFile)) {
            properties.store(out, "");
        }
    }

    @Test
    public void shouldFindCollectdMetricsOnServer() throws Exception {
        ptransProcessBuilder.command().addAll(ImmutableList.of("-c", ptransConfFile.getAbsolutePath()));
        ptransProcess = ptransProcessBuilder.start();
        assertPtransHasStarted(ptransProcess, ptransOut);

        File stdbuf = new File("/usr/bin/stdbuf");
        ImmutableList.Builder<String> collectdCmd = ImmutableList.builder();
        if (stdbuf.exists() && stdbuf.canExecute()) {
            collectdCmd.add(stdbuf.getAbsolutePath(), "-o0", "-e0");
        }
        collectdCmd.add(COLLECTD_PATH, "-C", collectdConfFile.getAbsolutePath(), "-f");
        collectdProcessBuilder.command(collectdCmd.build());
        collectdProcess = collectdProcessBuilder.start();

        waitForCollectdValues();

        kill(collectdProcess, Signal.SIGUSR1); // Flush data
        kill(collectdProcess, Signal.SIGTERM);
        collectdProcess.waitFor();

        Thread.sleep(MILLISECONDS.convert(1, SECONDS)); // Wait to make sure pTrans can send everything

        kill(ptransProcess, Signal.SIGTERM);
        ptransProcess.waitFor();

        List<Point> expectedData = getExpectedData();
        List<Point> serverData = getServerData();

        String failureMsg = String.format(
                Locale.ROOT, "Expected:%n%s%nActual:%n%s%n",
                pointsToString(expectedData), pointsToString(serverData)
        );

        assertEquals(failureMsg, expectedData.size(), serverData.size());

        for (int i = 0; i < expectedData.size(); i++) {
            Point expectedPoint = expectedData.get(i);
            Point serverPoint = serverData.get(i);

            long timeDiff = expectedPoint.getTimestamp() - serverPoint.getTimestamp();
            assertTrue(failureMsg, Math.abs(timeDiff) < 2);

            assertEquals(failureMsg, expectedPoint.getType(), serverPoint.getType());
            assertEquals(failureMsg, expectedPoint.getValue(), serverPoint.getValue(), 0.1);
        }
    }

    private String pointsToString(List<Point> data) {
        return data.stream().map(Point::toString).collect(joining(System.getProperty("line.separator")));
    }

    private void waitForCollectdValues() throws Exception {
        long c;
        do {
            Thread.sleep(MILLISECONDS.convert(1, SECONDS));
            c = Files.lines(collectdOut.toPath())
                     .filter(l -> l.startsWith("PUTVAL"))
                     .collect(counting());
        } while (c < 1);
    }

    private List<Point> getExpectedData() throws Exception {
        return Files.lines(collectdOut.toPath())
                    .filter(l -> l.startsWith("PUTVAL"))
                    .map(this::collectdLogToPoint)
                    .sorted(Comparator.comparing(Point::getType).thenComparing(Point::getTimestamp))
                    .collect(toList());
    }

    private Point collectdLogToPoint(String line) {
        String[] split = line.split(" ");
        assertEquals("Unexpected format: " + line, 4, split.length);
        String[] metric = split[1].split("/");
        assertEquals("Unexpected format: " + line, 3, metric.length);
        metric = metric[2].split("-");
        assertEquals("Unexpected format: " + line, 2, metric.length);
        String[] data = split[3].split(":");
        assertEquals("Unexpected format: " + line, 2, data.length);
        long timestamp = Long.parseLong(data[0].replace(".", ""));
        if (!data[0].contains(".")) {
            // collectd v4 does not have high resolution timestamps
            timestamp *= 1000;
        }
        double value = Double.parseDouble(data[1]);
        return new Point(metric[1], timestamp, value);
    }

    private List<Point> getServerData() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        HttpURLConnection urlConnection = (HttpURLConnection) new URL(findGuageMetricsUrl).openConnection();
        urlConnection.connect();
        int responseCode = urlConnection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            String msg = "Could not get metrics list from server: %s, %d";
            fail(String.format(Locale.ROOT, msg, findGuageMetricsUrl, responseCode));
        }
        List<String> metricNames;
        try (InputStream inputStream = urlConnection.getInputStream()) {
            TypeFactory typeFactory = objectMapper.getTypeFactory();
            CollectionType valueType = typeFactory.constructCollectionType(List.class, MetricName.class);
            List<MetricName> value = objectMapper.readValue(inputStream, valueType);
            metricNames = value.stream().map(MetricName::getId).collect(toList());
        }

        Stream<Point> points = Stream.empty();

        for (String metricName : metricNames) {
            String[] split = metricName.split("\\.");
            String type = split[split.length - 1];

            urlConnection = (HttpURLConnection) new URL(findGaugeDataUrl(metricName)).openConnection();
            urlConnection.connect();
            responseCode = urlConnection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                fail("Could not load metric data from server: " + responseCode);
            }

            try (InputStream inputStream = urlConnection.getInputStream()) {
                TypeFactory typeFactory = objectMapper.getTypeFactory();
                CollectionType valueType = typeFactory.constructCollectionType(List.class, MetricData.class);
                List<MetricData> data = objectMapper.readValue(inputStream, valueType);
                Stream<Point> metricPoints = data.stream()
                                                 .map(
                                                         metricData -> new Point(
                                                                 type,
                                                                 metricData.timestamp,
                                                                 metricData.value
                                                         )
                                                 );
                points = Stream.concat(points, metricPoints);
            }
        }

        return points.sorted(Comparator.comparing(Point::getType).thenComparing(Point::getTimestamp)).collect(toList());
    }

    private String findGaugeDataUrl(String metricName) {
        return "http://" + BASE_URI + "/" + tenant + "/gauge/" + metricName + "/data";
    }

    @After
    public void tearDown() {
        if (collectdProcess != null && collectdProcess.isAlive()) {
            collectdProcess.destroy();
        }
    }

    private static final class Point {
        final String type;
        final long timestamp;
        final double value;

        Point(String type, long timestamp, double value) {
            this.type = type;
            this.timestamp = timestamp;
            this.value = value;
        }

        String getType() {
            return type;
        }

        long getTimestamp() {
            return timestamp;
        }

        double getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "Point[" +
                   "type='" + type + '\'' +
                   ", timestamp=" + timestamp +
                   ", value=" + value +
                   ']';
        }
    }

    @SuppressWarnings("unused")
    private static final class MetricData {
        long timestamp;
        double value;

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @SuppressWarnings("unused")
    private static final class MetricName {
        String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }
}
