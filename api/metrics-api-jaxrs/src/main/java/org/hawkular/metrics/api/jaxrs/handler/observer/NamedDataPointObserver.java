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
package org.hawkular.metrics.api.jaxrs.handler.observer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.NamedDataPoint;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonGenerator;

import rx.Subscriber;
import rx.functions.Action1;

/**
 * @author jsanda
 */
public class NamedDataPointObserver<T> extends Subscriber<NamedDataPoint<T>> {

    private static final Logger log = Logger.getLogger(NamedDataPointObserver.class);

    private String currentMetric;

    private JsonGenerator generator;

    private CountDownLatch latch;

    private Action1<NamedDataPoint<T>> writeValue;

    public NamedDataPointObserver(JsonGenerator generator, CountDownLatch latch, MetricType<T> type) {
        try {
            this.generator = generator;
            this.latch = latch;

            if (type == MetricType.GAUGE || type == MetricType.GAUGE_RATE || type == MetricType.COUNTER_RATE) {
                writeValue = dataPoint -> {
                    try {
                        generator.writeNumberField("value", (Double) dataPoint.getValue());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };
            } else if (type == MetricType.COUNTER) {
                writeValue = dataPoint -> {
                    try {
                        generator.writeNumberField("value", (Long) dataPoint.getValue());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };
            } else if (type == MetricType.AVAILABILITY) {
                writeValue = dataPoint -> {
                    try {
                        AvailabilityType availability = (AvailabilityType) dataPoint.getValue();
                        generator.writeStringField("value", availability.getText());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };
            } else if (type == MetricType.STRING) {
                writeValue = dataPoint -> {
                    try {
                        generator.writeStringField("value", (String) dataPoint.getValue());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };
            } else {
                throw new IllegalArgumentException(type + " is not supported metric type. This class should be " +
                        "updated to add support for it!");
            }

            this.generator.writeStartArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    @Override
    public void onNext(NamedDataPoint<T> dataPoint) {
        log.debug("Next data point is " + dataPoint);
        try {

            if (currentMetric == null) {
                generator.writeStartObject();
                generator.writeStringField("id", dataPoint.getName());
                generator.writeArrayFieldStart("data");

                generator.writeStartObject();
                generator.writeNumberField("timestamp", dataPoint.getTimestamp());
                writeValue.call(dataPoint);
                generator.writeEndObject();
            } else if (currentMetric.equals(dataPoint.getName())) {
                generator.writeStartObject();
                generator.writeNumberField("timestamp", dataPoint.getTimestamp());
                writeValue.call(dataPoint);
                generator.writeEndObject();
            } else {
                generator.writeEndArray();
                generator.writeEndObject();
                generator.writeStartObject();
                generator.writeStringField("id", dataPoint.getName());
                generator.writeArrayFieldStart("data");

                generator.writeStartObject();
                generator.writeNumberField("timestamp", dataPoint.getTimestamp());
                writeValue.call(dataPoint);
                generator.writeEndObject();
            }
            currentMetric = dataPoint.getName();
        } catch (IOException e) {
                log.warn("Streaming data to client failed", e);
        }
    }

    @Override
    public void onError(Throwable e) {
        try {
                log.warn("Fetching data failed", e);
            generator.close();
        } catch (IOException e1) {
                log.error("Failed to close " + generator.getClass().getName());
        }
    }

    @Override
    public void onCompleted() {
        try {
            generator.writeEndArray();
            generator.writeEndObject();
            generator.writeEndArray();
            generator.close();
                latch.countDown();
        } catch (IOException e) {
                log.warn("Error while finishing streaming data", e);
        }
    }
}
