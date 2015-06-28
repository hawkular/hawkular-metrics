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
import java.util.ArrayList;
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
import org.hawkular.metrics.rest.model.DataPoints;
import org.hawkular.metrics.rest.model.GaugeDataPoint;
import rx.Observable;
import rx.subjects.PublishSubject;

public class Client {

    private static final String BASE_PATH = "/hawkular/metrics/";

    private Vertx vertx;

    private HttpClient httpClient;

    private String host;

    private int port;

    private String baseURI;

    private ObjectMapper mapper;

    public Client(String host, int port) {
//        this.host = host;
//        this.port = port;
        this.baseURI = baseURI;
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

    public Observable<HttpClientResponse> addGaugeData(String tenant, List<DataPoints<GaugeDataPoint>> gauges) {
        try {
            String json = mapper.writeValueAsString(gauges);
            PublishSubject<HttpClientResponse> subject = PublishSubject.create();
            HttpClientRequest request = httpClient.post(BASE_PATH + "gauges/data")
                    .putHeader("Content-Type", "application/json")
                    .putHeader("Hawkular-Tenant", tenant);
            request.toObservable().subscribe(subject::onNext, subject::onError, subject::onCompleted);
            request.end(json);
            return subject;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse " + gauges, e);
        }
    }

    public Observable<HttpClientResponse> addGaugeData(String tenant, String gauge, List<GaugeDataPoint> dataPoints) {
        try {
            PublishSubject<HttpClientResponse> subject = PublishSubject.create();
            String json = mapper.writeValueAsString(dataPoints);
//            HttpClientRequest request = httpClient.post(baseURI + "/gauges/" + gauge + "/data")
            HttpClientRequest request = httpClient.post(BASE_PATH + "gauges/" + gauge + "/data")
                    .putHeader("Content-Type", "application/json")
                    .putHeader("Hawkular-Tenant", tenant);
//                .write(json.toString());
//        Observable<HttpClientResponse> responseObservable = request.toObservable();
//        responseObservable.subscribe(
//                response -> System.out.println("status: {code: " + response.statusCode() + ", message: " +
//                    response.statusMessage() + "}"),
//                Throwable::printStackTrace
//        );
            request.toObservable().subscribe(subject::onNext, subject::onError, subject::onCompleted);
            request.end(json);
            return subject;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse data points", e);
        }
    }

    public Observable<GaugeDataPoint> findGaugeData(String tenantId, String gauge, long start, long end) {
        PublishSubject<GaugeDataPoint> subject = PublishSubject.create();
        HttpClientRequest request = httpClient.get(BASE_PATH + "gauges/" + gauge + "/data?start=" + start + "&end=" +
                end)
                .putHeader("Hawkular-Tenant", tenantId)
                .putHeader("Content-Type", "application/json");
        Observable<GaugeDataPoint> observable = request.toObservable()
                .flatMap(HttpClientResponse::toObservable)
                .flatMap(buffer -> Observable.from(getGaugeDataPoints(buffer)));
        observable.subscribe(subject::onNext, subject::onError, subject::onCompleted);
        request.end();
        return subject;
    }

    private List<GaugeDataPoint> getGaugeDataPoints(Buffer buffer) throws RuntimeException {
        try {
            return mapper.readValue(buffer.toString("UTF-8"), new TypeReference<List<GaugeDataPoint>>() {});
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse response", e);
        }
    }

//    public static void main(String[] args) throws InterruptedException {
//        Client client = new Client("localhost", 8080);
////        long end = System.currentTimeMillis();
////        long start = end - TimeUnit.MILLISECONDS.convert(8, TimeUnit.HOURS);
////        client.findGaugeData("Vert.x", "Test1", start, end).subscribe(
////                System.out::println,
////                t -> {
////                    t.printStackTrace();
////                    try {
////                        client.shutdown();
////                    } catch (InterruptedException e) {
////                        e.printStackTrace();
////                    }
////                },
////                () -> {
////                    try {
////                        client.shutdown();
////                    } catch (InterruptedException e) {
////                        e.printStackTrace();
////                    }
////                }
////        );
//
//
////        List<GaugeDataPoint> dataPoints = asList(
////                new GaugeDataPoint(System.currentTimeMillis() - 500, 10.1),
////                new GaugeDataPoint(System.currentTimeMillis() - 400, 11.1112),
////                new GaugeDataPoint(System.currentTimeMillis() - 300, 13.4783)
////        );
////        client.addGaugeData("Vert.x", "TEST3", dataPoints).subscribe(
////                response -> System.out.println("status: {code: " + response.statusCode() + ", message: " +
////                        response.statusMessage() + "}"),
////                t -> {
////                    t.printStackTrace();
////                    try {
////                        client.shutdown();
////                    } catch (InterruptedException e) {
////                        e.printStackTrace();
////                    }
////                },
////                () -> {
////                    try {
////                        client.shutdown();
////                    } catch (InterruptedException e) {
////                        e.printStackTrace();
////                    }
////                }
////        );
//
//        client.addGaugeData("Multi-Metric-Test", createDataPoints()).subscribe(
//                response -> System.out.println("status: {code: " + response.statusCode() + ", message: " +
//                        response.statusMessage() + "}"),
//                t -> {
//                    t.printStackTrace();
//                    try {
//                        client.shutdown();
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                },
//                () -> {
//                    try {
//                        client.shutdown();
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//        );
//    }

    private static List<DataPoints<GaugeDataPoint>> createDataPoints() {
        List<DataPoints<GaugeDataPoint>> dataPointsList = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            List<GaugeDataPoint> dataPoints = new ArrayList<>();
            for (int j = 0; j < 3; ++j) {
                dataPoints.add(new GaugeDataPoint(System.currentTimeMillis() - (j * 500), j * Math.PI));
            }
            dataPointsList.add(new DataPoints<>("G" + i, dataPoints));
        }
        return dataPointsList;
    }

}
