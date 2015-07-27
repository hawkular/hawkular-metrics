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
package org.hawkular.metrics.api.jaxrs.util;

import static java.util.stream.Collectors.toList;
import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.ws.rs.core.Response;

import com.google.common.base.Throwables;
import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.model.Availability;
import org.hawkular.metrics.api.jaxrs.model.AvailabilityDataPoint;
import org.hawkular.metrics.api.jaxrs.model.Counter;
import org.hawkular.metrics.api.jaxrs.model.CounterDataPoint;
import org.hawkular.metrics.api.jaxrs.model.Gauge;
import org.hawkular.metrics.api.jaxrs.model.GaugeDataPoint;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import rx.Observable;

/**
 * @author jsanda
 */
public class ApiUtils {

    private ApiUtils() {
    }

    public static Response collectionToResponse(Collection<?> collection) {
        return collection.isEmpty() ? noContent() : Response.ok(collection).build();
    }

    public static Response serverError(Throwable t, String message) {
        String errorMsg = message + ": " + Throwables.getRootCause(t).getMessage();
        return Response.serverError().entity(new ApiError(errorMsg)).build();
    }

    public static Response serverError(Throwable t) {
        return serverError(t, "Failed to perform operation due to an error");
    }

    public static Response valueToResponse(Optional<?> optional) {
        return optional.map(value -> Response.ok(value).build()).orElse(noContent());
    }

    // TODO We probably want to return an Observable here
    public static List<DataPoint<Double>> requestToGaugeDataPoints(List<GaugeDataPoint> gaugeDataPoints) {
        return gaugeDataPoints.stream()
                .map(p -> new DataPoint<>(p.getTimestamp(), p.getValue(), p.getTags()))
                .collect(toList());
    }

    public static Observable<Metric<Double>> requestToGauges(String tenantId, List<Gauge> gauges) {
        return Observable.from(gauges).map(g ->
                new Metric<>(tenantId, GAUGE, new MetricId(g.getId()), requestToGaugeDataPoints(g.getData())));
    }

    public static Observable<Metric<Long>> requestToCounters(String tenantId, List<Counter> counters) {
        return Observable.from(counters).map(c ->
                new Metric<>(tenantId, COUNTER, new MetricId(c.getId()), requestToCounterDataPoints(c.getData())));
    }

    public static Observable<Metric<AvailabilityType>> requestToAvailabilities(String tenantId,
            List<Availability> avails) {
        return Observable.from(avails).map(a -> new Metric<>(tenantId, AVAILABILITY, new MetricId(a.getId()),
                requestToAvailabilityDataPoints(a.getData())));
    }

    public static List<DataPoint<Long>> requestToCounterDataPoints(List<CounterDataPoint> dataPoints) {
        return dataPoints.stream()
                .map(p -> new DataPoint<>(p.getTimestamp(), p.getValue(), p.getTags()))
                .collect(toList());
    }

    // TODO We probably want to return an Observable here
    public static List<DataPoint<AvailabilityType>> requestToAvailabilityDataPoints(
            List<AvailabilityDataPoint> dataPoints) {
        return dataPoints.stream()
                .map(p -> new DataPoint<>(p.getTimestamp(), AvailabilityType.fromString(p.getValue())))
                .collect(toList());
    }

    public static Response noContent() {
        return Response.noContent().build();
    }

    public static Response emptyPayload() {
        return badRequest(new ApiError("Payload is empty"));
    }

    public static Response badRequest(ApiError error) {
        return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
    }
}
