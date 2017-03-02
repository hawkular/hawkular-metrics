/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.client.common.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link HawkularHttpClient}, using the JDK HTTP client.
 * This class does not aim to be any generic. It's very tied to what is needed for the dropwizard reporter.
 * @author Joel Takvorian
 */
public class JdkHawkularHttpClient implements HawkularHttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(JdkHawkularHttpClient.class);

    private final String uri;
    private final Map<String, String> headers = new HashMap<>();
    private Optional<Long> failoverCacheDuration = Optional.empty();
    private Optional<Integer> failoverCacheMaxSize = Optional.empty();
    private Queue<Message> failoverCache = new LinkedBlockingQueue<>();

    public JdkHawkularHttpClient(String uri) {
        this.uri = uri + "/hawkular/metrics";
    }

    @Override
    public void addHeaders(Map<String, String> headers) {
        this.headers.putAll(headers);
    }

    @Override
    public HawkularHttpResponse postMetrics(String jsonBody) {
        return buildURLAndSend("POST", "/metrics/raw", jsonBody.getBytes());
    }

    @Override
    public HawkularHttpResponse putTags(String metricType, String metricName, String jsonBody) {
        try {
            String encodedFullName = URLEncoder.encode(metricName, "UTF-8");
            String resourcePath = "/" + metricType + "/" + encodedFullName + "/tags";
            return buildURLAndSend("PUT", resourcePath, jsonBody.getBytes());
        } catch (UnsupportedEncodingException e) {
            return new HawkularHttpResponse("", -1, "Message not sent, unsupported encoding: " + e.getMessage());
        }
    }

    public HawkularHttpResponse readMetric(String type, String name) throws IOException {
        URL url = new URL(uri + "/" + type + "/" + name + "/raw");
        return get(url);
    }

    private HawkularHttpResponse buildURLAndSend(String verb, String resourcePath, byte[] content) {
        final URL url;
        try {
            url = new URL(uri + resourcePath);
        } catch (MalformedURLException e) {
            LOG.error("Bad URL", e);
            return new HawkularHttpResponse("", -1, "Message not sent, bad URL: " + e.getMessage());
        }
        return sendAndHandleError(new Message(verb, url, content));
    }

    private HawkularHttpResponse sendAndHandleError(Message msg) {
        try {
            HawkularHttpResponse response = send(msg);
            int code = response.getResponseCode();
            if (code >= 400) {
                LOG.debug("Server response: {}, {}", code, response.getErrorMsg());
                addToFailoverCache(msg);
            } else if (code != 200 && code != 204) {
                LOG.debug("Server response: {}, {}", code, response.getErrorMsg());
            }
            return response;
        } catch (IOException e) {
            LOG.debug("Failed to send data:", e);
            addToFailoverCache(msg);
            return new HawkularHttpResponse("", -1, "Message not sent: " + e.getMessage());
        }
    }

    private HawkularHttpResponse send(Message message) throws IOException {
        InputStream is = null;
        byte[] data = null;
        int responseCode = -1;
        ByteArrayOutputStream baos = null;
        try {
            final HttpURLConnection connection = (HttpURLConnection) message.getUrl().openConnection();
            connection.setDoOutput(true);
            connection.setUseCaches(false);
            connection.setRequestMethod(message.getVerb());
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Content-Length", String.valueOf(message.getContent().length));
            headers.forEach(connection::setRequestProperty);
            OutputStream os = connection.getOutputStream();
            os.write(message.getContent());
            os.close();
            responseCode = connection.getResponseCode();
            is = connection.getInputStream();
            final byte[] buffer = new byte[2 * 1024];
            baos = new ByteArrayOutputStream();
            int n;
            while ((n = is.read(buffer)) >= 0) {
                baos.write(buffer, 0, n);
            }
            data = baos.toByteArray();
        } catch (IOException e) {
            if (responseCode > 0) {
                return new HawkularHttpResponse("", responseCode, e.getMessage());
            } else {
                throw e;
            }
        } finally {
            if (is != null) {
                is.close();
            }
            if (baos != null) {
                baos.close();
            }
        }
        return new HawkularHttpResponse(new String(data), responseCode);
    }

    private HawkularHttpResponse get(URL url) throws IOException {
        InputStream is = null;
        byte[] data = null;
        int responseCode;
        ByteArrayOutputStream baos = null;
        try {
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            headers.forEach(connection::setRequestProperty);
            responseCode = connection.getResponseCode();
            is = connection.getInputStream();
            final byte[] buffer = new byte[2 * 1024];
            baos = new ByteArrayOutputStream();
            int n;
            while ((n = is.read(buffer)) >= 0) {
                baos.write(buffer, 0, n);
            }
            data = baos.toByteArray();
        } finally {
            if (is != null) {
                is.close();
            }
            if (baos != null) {
                baos.close();
            }
        }
        return new HawkularHttpResponse(new String(data), responseCode);
    }

    @Override
    public void setFailoverOptions(Optional<Long> failoverCacheDuration, Optional<Integer> failoverCacheMaxSize) {
        this.failoverCacheDuration = failoverCacheDuration;
        this.failoverCacheMaxSize = failoverCacheMaxSize;
    }

    @Override
    public void manageFailover() {
        Long oldestAllowed = failoverCacheDuration.map(d -> System.currentTimeMillis() - d).orElse(0L);
        // Elements might be added during the cache list processing, so get its size once for all and don't process more than that
        int size = failoverCache.size();
        int countTrashed = 0;
        for (int i = 0; i < size; i++) {
            Message msg = failoverCache.poll();
            if (msg.getTimestamp() >= oldestAllowed) {
                sendAndHandleError(msg);
            } else {
                countTrashed++;
            }
        }
        if (countTrashed > 0) {
            LOG.warn("Failover cache contained {} old items that have been trashed", countTrashed);
        }
    }

    private void addToFailoverCache(Message msg) {
        failoverCacheMaxSize.ifPresent(max -> {
            int size = failoverCache.size();
            if (size >= max) {
                LOG.warn("Failover cache reached its maximum capacity ({} requests). Oldest elements will be lost.", max);
            }
            while (size >= max) {
                // Trash oldest items
                failoverCache.poll();
                size--;
            }
        });
        if (failoverCache.isEmpty()) {
            LOG.info("Failed to send data to Hawkular. Data is kept in memory and will be sent again later. More info on DEBUG logs.");
        }
        failoverCache.offer(msg);
    }

    public int getFailoverCacheSize() {
        return failoverCache.size();
    }

    private static class Message {
        private final String verb;
        private final URL url;
        private final byte[] content;
        private final long timestamp;

        private Message(String verb, URL url, byte[] content) {
            this.verb = verb;
            this.url = url;
            this.content = content;
            this.timestamp = System.currentTimeMillis();
        }

        String getVerb() {
            return verb;
        }

        URL getUrl() {
            return url;
        }

        byte[] getContent() {
            return content;
        }

        Long getTimestamp() {
            return timestamp;
        }
    }
}
