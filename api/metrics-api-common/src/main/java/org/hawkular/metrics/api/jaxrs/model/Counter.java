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

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

import static org.hawkular.metrics.core.api.MetricType.COUNTER;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Inclusion;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import rx.Observable;

/**
 * A container for data points for a single counter metric.
 *
 * @author John Sanda
 */
@ApiModel(description = "A counter metric with one or more data points")
public class Counter {
    private final String id;
    private final List<CounterDataPoint> data;

    @JsonCreator(mode = Mode.PROPERTIES)
    @org.codehaus.jackson.annotate.JsonCreator
    public Counter(
            @JsonProperty("id")
            @org.codehaus.jackson.annotate.JsonProperty("id")
            String id,
            @JsonProperty("data")
            @org.codehaus.jackson.annotate.JsonProperty("data")
            List<CounterDataPoint> data
    ) {
        checkArgument(id != null, "Counter id is null");
        this.id = id;
        this.data = data == null || data.isEmpty() ? emptyList() : unmodifiableList(data);
    }

    @ApiModelProperty(value = "Identifier of the metric", required = true)
    public String getId() {
        return id;
    }

    @ApiModelProperty("Counter data points")
    @JsonSerialize(include = Inclusion.NON_EMPTY)
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY
    )
    public List<CounterDataPoint> getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Counter counter = (Counter) o;
        return id.equals(counter.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("id", id)
                .add("data", data)
                .omitNullValues()
                .toString();
    }

    public static Observable<Metric<Long>> toObservable(String tenantId, List<Counter> counters) {
        return Observable.from(counters).map(c -> {
            List<DataPoint<Long>> dataPoints = CounterDataPoint.asDataPoints(c.getData());
            return new Metric<>(new MetricId<>(tenantId, COUNTER, c.getId()), dataPoints);
        });
    }
}
