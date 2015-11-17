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
package org.hawkular.metrics.api.jaxrs.model;

import static org.hawkular.metrics.core.api.MetricType.COUNTER;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import rx.Observable;

/**
 * @author John Sanda
 */
@ApiModel(description = "A timestamp and a value where the value is interpreted as a signed 64 bit integer")
public class CounterDataPoint extends DataPoint<Long> {

    @JsonCreator(mode = Mode.PROPERTIES)
    @org.codehaus.jackson.annotate.JsonCreator
    public CounterDataPoint(
            @JsonProperty("timestamp")
            @org.codehaus.jackson.annotate.JsonProperty("timestamp")
            Long timestamp,
            @JsonProperty("value")
            @org.codehaus.jackson.annotate.JsonProperty("value")
            Long value
    ) {
        super(timestamp, value);
        checkArgument(timestamp != null, "Data point timestamp is null");
        checkArgument(value != null, "Data point value is null");
    }

    public CounterDataPoint(DataPoint<Long> other) {
        super(other.getTimestamp(), other.getValue());
    }

    @ApiModelProperty(required = true)
    public long getTimestamp() {
        return timestamp;
    }

    @ApiModelProperty(required = true)
    public Long getValue() {
        return value;
    }

    public static List<DataPoint<Long>> asDataPoints(List<CounterDataPoint> points) {
        return Lists.transform(points, p -> new DataPoint<>(p.getTimestamp(), p.getValue()));
    }

    public static Observable<Metric<Long>> toObservable(String tenantId, String metricId, List<CounterDataPoint>
            points) {
        List<DataPoint<Long>> dataPoints = asDataPoints(points);
        Metric<Long> metric = new Metric<>(new MetricId<>(tenantId, COUNTER, metricId), dataPoints);
        return Observable.just(metric);
    }
}
