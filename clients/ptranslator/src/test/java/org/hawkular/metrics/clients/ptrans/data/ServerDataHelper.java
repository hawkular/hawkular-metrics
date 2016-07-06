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

package org.hawkular.metrics.clients.ptrans.data;

import static java.util.stream.Collectors.toList;

import static org.junit.Assert.fail;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Helps full stack tests developers to get all metric data from the server, for comparison with expected data.
 *
 * @author Thomas Segismont
 */
public class ServerDataHelper {
    private static final String HAWKULAR_TENANT_HEADER = "Hawkular-Tenant";

    public static final String BASE_URI;

    static {
        String baseUri = System.getProperty("hawkular-metrics.base-uri");
        if (baseUri == null || baseUri.trim().isEmpty()) {
            baseUri = "127.0.0.1:8080/hawkular/metrics";
        }
        BASE_URI = baseUri;
    }

    private final String tenant;
    private final String findGaugeMetricsUrl;

    public ServerDataHelper(String tenant) {
        this.tenant = tenant;
        findGaugeMetricsUrl = "http://" + BASE_URI + "/metrics?type=gauge";
    }

    public List<Point> getServerData() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        HttpURLConnection urlConnection = (HttpURLConnection) new URL(findGaugeMetricsUrl).openConnection();
        urlConnection.setRequestProperty(HAWKULAR_TENANT_HEADER, tenant);
        urlConnection.connect();
        int responseCode = urlConnection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
            return Collections.emptyList();
        }
        if (responseCode != HttpURLConnection.HTTP_OK) {
            String msg = "Could not get metrics list from server: %s, %d";
            fail(String.format(Locale.ROOT, msg, findGaugeMetricsUrl, responseCode));
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
            urlConnection = (HttpURLConnection) new URL(findGaugeDataUrl(metricName)).openConnection();
            urlConnection.setRequestProperty(HAWKULAR_TENANT_HEADER, tenant);
            urlConnection.connect();
            responseCode = urlConnection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
                continue;
            }
            if (responseCode != HttpURLConnection.HTTP_OK) {
                fail("Could not load metric data from server: " + responseCode);
            }

            try (InputStream inputStream = urlConnection.getInputStream()) {
                TypeFactory typeFactory = objectMapper.getTypeFactory();
                CollectionType valueType = typeFactory.constructCollectionType(List.class, MetricData.class);
                List<MetricData> data = objectMapper.readValue(inputStream, valueType);
                Stream<Point> metricPoints = data.stream().map(
                        metricData -> new Point(metricName, metricData.timestamp, metricData.value)
                );
                points = Stream.concat(points, metricPoints);
            }
        }

        return points.collect(toList());
    }

    private String findGaugeDataUrl(String metricName) {
        return "http://" + BASE_URI + "/gauges/" + metricName + "/raw?start=0";
    }
}
