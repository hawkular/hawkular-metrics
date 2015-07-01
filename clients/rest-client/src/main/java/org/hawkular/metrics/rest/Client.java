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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.http.HttpClientResponse;
import org.hawkular.metrics.rest.model.AvailabilityDataPoint;
import org.hawkular.metrics.rest.model.DataPoint;
import org.hawkular.metrics.rest.model.DataPoints;
import org.hawkular.metrics.rest.model.GaugeDataPoint;
import rx.Observable;
import rx.Observer;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

public class Client {

    private static final String BASE_PATH = "/hawkular/metrics/";

    private static final String TENANT_HEADER = "Hawkular-Tenant";

    private Vertx vertx;

    private HttpClient httpClient;

    private ObjectMapper mapper;

    public Client(String host, int port) {
        this.vertx = Vertx.vertx();
        HttpClientOptions options = new HttpClientOptions().setDefaultHost(host).setDefaultPort(port);
        httpClient = vertx.createHttpClient(options);

        mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    public void shutdown() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        httpClient.close();
        vertx.close(aVoid -> latch.countDown());
        latch.await(5, TimeUnit.SECONDS);
    }

    public Observable<Void> addGaugeData(String tenant, List<DataPoints<GaugeDataPoint>> gauges) {
        return addDataPoints(tenant, "gauges/data", gauges);
    }

    public Observable<Void> addGaugeData(String tenant, String gauge, List<GaugeDataPoint> dataPoints) {
        try {
            String json = mapper.writeValueAsString(dataPoints);
            HttpClientRequest request = httpClient.post(BASE_PATH + "gauges/" + gauge + "/data")
                    .putHeader("Content-Type", "application/json")
                    .putHeader(TENANT_HEADER, tenant);

            WriteObserver writeObserver = new WriteObserver();
            request.toObservable().subscribe(writeObserver);
            request.end(json);

            return writeObserver.getObservable();
        } catch (JsonProcessingException e) {
            throw new ClientException("Failed to parse data points", e);
        }
    }

    public Observable<Void> addAvailabilty(String tenant, List<DataPoints<AvailabilityDataPoint>> dataPoints) {
        return addDataPoints(tenant, "availability/data", dataPoints);
    }

//    public Observable<Void> addCounterData(String tenant, List<DataPoints<CounterDataPoint>> dataPoints) {
//        try {
//            String json = mapper.writeValueAsString(dataPoints);
//            HttpClientRequest request
//        } catch (JsonProcessingException e) {
//            throw new ClientException("Failed to parse data points", e);
//        }
//    }

    private <T extends DataPoint> Observable<Void> addDataPoints(String tenant, String path,
            List<DataPoints<T>> dataPoints) {
        try {
            PublishSubject<Void> subject = PublishSubject.create();
            String json = mapper.writeValueAsString(dataPoints);
            HttpClientRequest request = httpClient.post(BASE_PATH + path)
                    .putHeader("Content-Type", "application/json")
                    .putHeader(TENANT_HEADER, tenant);
            request.toObservable().subscribe(
                    response -> {
                        if (response.statusCode() != 200) {
                            subject.onError(new WriteException(response.statusMessage(), response.statusCode()));
                        }
                    },
                    t -> subject.onError(new ClientException("There was an unexpected error while adding data", t)),
                    subject::onCompleted

            );
            request.end(json);

            return subject;
        } catch (JsonProcessingException e) {
            throw new ClientException("Failed to parse data points", e);
        }
    }

    public Observable<GaugeDataPoint> findGaugeData(String tenantId, String gauge, long start, long end) {
        String path = "gauges/" + gauge + "/data?start=" + start + "&end=" + end;
        return getDataPoints(tenantId, path, this::getGaugeDataPoints);
    }

    public Observable<AvailabilityDataPoint> findAvailabilityData(String tenantId, String metric, long start,
            long end) {
        String path = "availability/" + metric + "/data?start=" + start + "&end=" + end;
        return getDataPoints(tenantId, path, this::getAvailabilityDataPoints);
    }

    private <T extends DataPoint> Observable<T> getDataPoints(String tenantId, String path,
            Func1<Buffer, List<T>> getDataPoints) {

        PublishSubject<T> subject = PublishSubject.create();
        HttpClientRequest request = httpClient.get(BASE_PATH + path)
                .putHeader(TENANT_HEADER, tenantId)
                .putHeader("Content-Type", "application/json");
        Observable<T> observable = request.toObservable()
                .flatMap(response -> {
                    if (response.statusCode() == 200 || response.statusCode() == 204) {
                        return response.toObservable();
                    }
                    throw new ReadException(response.statusMessage(), response.statusCode());
                })
                .flatMap(buffer -> Observable.from(getDataPoints.call(buffer)));
        observable.subscribe(
                subject::onNext,
                t -> {
                    if (t instanceof ReadException) {
                        subject.onError(t);
                    } else {
                        subject.onError(new ClientException("There was an unexpected error while adding data", t));
                    }
                },
                subject::onCompleted
        );
        request.end();

        return subject;
    }

    private List<GaugeDataPoint> getGaugeDataPoints(Buffer buffer) throws RuntimeException {
        try {
            return mapper.readValue(buffer.toString("UTF-8"), new TypeReference<List<GaugeDataPoint>>() {});
        } catch (IOException e) {
            throw new ClientException("Failed to parse response", e);
        }
    }

    private List<AvailabilityDataPoint> getAvailabilityDataPoints(Buffer buffer) throws RuntimeException {
        try {
            return mapper.readValue(buffer.toString("UTF-8"), new TypeReference<List<AvailabilityDataPoint>>() {});
        } catch (IOException e) {
            throw new ClientException("Failed to parse response", e);
        }
    }

    private class WriteObserver implements Observer<HttpClientResponse> {

        private PublishSubject<Void> subject = PublishSubject.create();

        public Observable<Void> getObservable() {
            return subject;
        }

        @Override
        public void onCompleted() {
            subject.onCompleted();
        }

        @Override
        public void onError(Throwable e) {
            subject.onError(new ClientException("There was an unexpected error while adding data", e));
        }

        @Override
        public void onNext(HttpClientResponse response) {
            if (response.statusCode() != 200) {
                subject.onError(new WriteException(response.statusMessage(), response.statusCode()));
            }
        }
    }

}
