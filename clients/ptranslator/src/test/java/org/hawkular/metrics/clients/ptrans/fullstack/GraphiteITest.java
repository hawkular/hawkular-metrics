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
import static java.util.stream.Collectors.groupingBy;

import static org.junit.Assert.assertEquals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.hawkular.metrics.clients.ptrans.ConfigurationKey;
import org.hawkular.metrics.clients.ptrans.Service;
import org.hawkular.metrics.clients.ptrans.data.Point;
import org.jmxtrans.embedded.EmbeddedJmxTrans;
import org.jmxtrans.embedded.QueryResult;
import org.jmxtrans.embedded.config.ConfigurationParser;
import org.jmxtrans.embedded.output.AbstractOutputWriter;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * @author Thomas Segismont
 */
public class GraphiteITest extends FullStackITest {

    @Rule
    public final TestWatcher testWatcher = new DumpDataSetOnFailureWatcher();

    private EmbeddedJmxTrans embeddedJmxTrans;

    @Override
    protected void configureSource() throws Exception {
        ConfigurationParser configurationParser = new ConfigurationParser();
        embeddedJmxTrans = configurationParser.newEmbeddedJmxTrans("classpath:jmxtrans.json");
    }

    @Override
    protected void changePTransConfig(Properties properties) {
        properties.setProperty(ConfigurationKey.SERVICES.toString(), Service.GRAPHITE.getExternalForm());
        properties.setProperty(ConfigurationKey.SERVICES_GRAPHITE_PORT.toString(), String.valueOf(12003));
    }

    @Override
    protected void startSource() throws Exception {
        embeddedJmxTrans.start();
    }

    @Override
    protected void waitForSourceValues() throws Exception {
        do {
            Thread.sleep(MILLISECONDS.convert(1, SECONDS));
        } while (ListAppenderWriter.results.size() < 10);
    }

    @Override
    protected void stopSource() throws Exception {
        embeddedJmxTrans.stop();
    }

    @Override
    protected List<Point> getExpectedData() {
        // When EmbeddedJMXTrans is stopped, it collects and sends metrics one last time after the schedulers have
        // been shut down. So two data points can have very close timestamps. But Graphite only supports second
        // resolution. So it is possible that two points with the same timestamp (see #getTimestamp logic) are sent
        // to the server. Consequently, when building the expected data set, we need to check if one metric has two
        // points with the same timestamp, and, in this case, keep the older one only.
        List<Point> points = new ArrayList<>();
        ListAppenderWriter.results.stream().collect(groupingBy(QueryResult::getName)).entrySet().forEach(entry -> {
            Map<Long, QueryResult> resultByTimestamp = new HashMap<>();
            entry.getValue().forEach(result -> resultByTimestamp.put(getTimestamp(result), result));
            resultByTimestamp.values().stream().map(this::queryResultToPoint).forEach(points::add);
        });
        return points;
    }

    private long getTimestamp(QueryResult result) {
        return MILLISECONDS.convert(result.getEpoch(SECONDS), SECONDS);
    }

    private Point queryResultToPoint(QueryResult result) {
        return new Point(
                result.getName(),
                getTimestamp(result),
                Double.valueOf(String.valueOf(result.getValue()))
        );
    }

    @Override
    protected void checkTimestamps(String failureMsg, long expected, long actual) {
        assertEquals(failureMsg, expected, actual);
    }

    @After
    public void tearDown() throws Exception {
        if (embeddedJmxTrans != null) {
            embeddedJmxTrans.stop();
        }
    }

    public static final class ListAppenderWriter extends AbstractOutputWriter {

        public ListAppenderWriter() {
            results.clear();
        }

        private static final List<QueryResult> results = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void write(Iterable<QueryResult> results) {
            results.forEach(System.out::println);
            results.forEach(ListAppenderWriter.results::add);
        }
    }

    private class DumpDataSetOnFailureWatcher extends TestWatcher {
        static final String SEPARATOR = "###########";

        @Override
        protected void failed(Throwable e, Description description) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            printWriter.println(GraphiteITest.class.getSimpleName() + ", graphite captured dataset:");
            printWriter.println(SEPARATOR);
            ListAppenderWriter.results.forEach(printWriter::println);
            printWriter.println(SEPARATOR);
            printWriter.close();
            System.out.println(stringWriter.toString());
        }
    }
}
