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

import static org.hawkular.metrics.clients.ptrans.backend.RestForwardingHandler.TENANT_HEADER_NAME;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.client.common.SingleMetric;
import org.hawkular.metrics.clients.ptrans.Configuration;
import org.hawkular.metrics.clients.ptrans.ConfigurationKey;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

/**
 * @author Thomas Segismont
 */
@RunWith(MockitoJUnitRunner.class)
public class RestForwardingHandlerITest {
    private static final String BASE_URI = System.getProperty(
            "hawkular-metrics.base-uri",
            "127.0.0.1:8080/hawkular/metrics"
    );
    private static final String TENANT = "test";
    private static final String METRIC_NAME = RestForwardingHandler.class.getName();

    @Mock
    private ChannelHandlerContext channelHandlerContext;
    private RestForwardingHandler restForwardingHandler;
    private String findGaugeDataUrl;

    @Before
    public void setUp() throws Exception {
        Channel channel = mock(Channel.class);
        when(channelHandlerContext.channel()).thenReturn(channel);
        EventLoop eventLoop = mock(EventLoop.class);
        when(channel.eventLoop()).thenReturn(eventLoop);
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        when(eventLoop.parent()).thenReturn(eventLoopGroup);

        Properties properties = new Properties();
        String addGaugeDataUrl = "http://" + BASE_URI + "/gauges/data";
        properties.setProperty(ConfigurationKey.REST_URL.toString(), addGaugeDataUrl);
        properties.setProperty(ConfigurationKey.TENANT.toString(), TENANT);
        Configuration configuration = Configuration.from(properties);

        restForwardingHandler = new RestForwardingHandler(configuration);

        findGaugeDataUrl = "http://" + BASE_URI + "/gauges/" + METRIC_NAME + "/data";
    }

    @Test
    public void shouldForwardMetrics() throws Exception {
        SingleMetric metric = new SingleMetric(METRIC_NAME, System.currentTimeMillis(), 13.5d);
        restForwardingHandler.channelRead(channelHandlerContext, Arrays.asList(metric));

        boolean foundMetricOnServer = false;
        for (int i = 0; i < 20; i++) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            JsonNode jsonNode = findGaugeDataOnServer();
            if (jsonNode != null && expectedMetricIsPresent(metric, jsonNode)) {
                foundMetricOnServer = true;
                break;
            }
        }
        assertTrue("Did not find expected metric on server", foundMetricOnServer);
    }

    private JsonNode findGaugeDataOnServer() throws IOException {
        HttpURLConnection urlConnection = (HttpURLConnection) new URL(findGaugeDataUrl).openConnection();
        urlConnection.setRequestProperty(TENANT_HEADER_NAME, TENANT);
        urlConnection.connect();
        int responseCode = urlConnection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try (InputStream inputStream = urlConnection.getInputStream()) {
            return objectMapper.readTree(inputStream);
        }
    }

    private boolean expectedMetricIsPresent(SingleMetric metric, JsonNode jsonNode) {
        assertTrue("Data is not an array", jsonNode.isArray());

        for (JsonNode gaugeData : jsonNode) {
            JsonNode timestamp = gaugeData.get("timestamp");
            assertNotNull("Gauge data has no timestamp attribute", timestamp);
            assertTrue("Timestamp is not a number", timestamp.isNumber());
            JsonNode value = gaugeData.get("value");
            assertNotNull("Gauge data has no value attribute", value);
            assertTrue("Value is not a floating point number", value.isFloatingPointNumber());

            if (timestamp.asLong() == metric.getTimestamp() && value.asDouble() == metric.getValue()) {
                return true;
            }
        }
        return false;
    }
}