/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.api.servlet;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.hawkular.metrics.api.servlet.rx.ObservableServlet;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.Metric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import rx.Observable;
import rx.Observer;
import rx.exceptions.Exceptions;
import rx.subjects.PublishSubject;

/**
 * @author Michael Burman
 */
@WebServlet(urlPatterns = "/openshift/*", asyncSupported = true)
public class OpenshiftServlet extends HttpServlet {

    private static final ObjectMapper objectMapper;
    private static final ObjectWriter objectWriter;
    private static final byte[] comma = ",".getBytes(Charset.forName("UTF-8"));
    private static final String DESCRIPTOR_TAG = "descriptor_name";

    static {
        objectMapper = new ObjectMapper();
        objectWriter = objectMapper.writer();
    }

    @Inject
    private MetricsService metricsService;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setCharacterEncoding("UTF-8");
        resp.setContentType("application/json");
        AsyncContext asyncContext = getAsyncContext(req);

        PublishSubject<byte[]> byteSubject = PublishSubject.create();

        Observable<Metric<Object>> metricObservable = metricsService.scanAllMetricIndexes()
                .filter(m -> m.getTags().containsKey(DESCRIPTOR_TAG))
                .onBackpressureBuffer();

        serializeMetrics(byteSubject, metricObservable);

        ObservableServlet.write(byteSubject, resp.getOutputStream())
                .subscribe(v -> {},
                        t -> {
                            t.printStackTrace();
                            // invoked..
                            asyncContext.complete();
                        },
                        asyncContext::complete);
    }

    public static void serializeMetrics(PublishSubject<byte[]> targetSubject, Observable<Metric<Object>>
            sourceObservable) {
        // Transform above on the fly to ByteBuffers and write them as soon as we have them
        Observable<byte[]> buffers =
                sourceObservable.map(m -> {
                    try {
                        return objectWriter.writeValueAsBytes(m);
                    } catch (JsonProcessingException e) {
                        throw Exceptions.propagate(e);
                    }
                }).onBackpressureBuffer();

        buffers.subscribe(new Observer<byte[]>() {
            AtomicBoolean first = new AtomicBoolean(true);

            @Override public void onCompleted() {
                if(!first.get()) {
                    targetSubject.onNext("]".getBytes(Charset.forName("UTF-8")));
                }
                targetSubject.onCompleted();
            }

            @Override public void onError(Throwable throwable) {
                targetSubject.onError(throwable);
            }

            @Override public void onNext(byte[] bytes) {
                if(first.compareAndSet(true, false)) {
                    targetSubject.onNext("[".getBytes(Charset.forName("UTF-8")));
                    targetSubject.onNext(bytes);
                } else {
                    targetSubject.onNext(comma);
                    targetSubject.onNext(bytes);
                }
            }
        });
    }

    private AsyncContext getAsyncContext(HttpServletRequest req) {
        if(req.isAsyncStarted()) {
            return req.getAsyncContext();
        } else {
            return req.startAsync();
        }
    }
}
