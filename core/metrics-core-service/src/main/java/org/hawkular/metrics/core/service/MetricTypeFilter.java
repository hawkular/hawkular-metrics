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
package org.hawkular.metrics.core.service;

import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;

import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricType;

import rx.Observable;
import rx.Observable.Transformer;

/**
 * Filters metrics of a given type. Use with {@link Observable#compose(Transformer)}.
 *
 * @author Thomas Segismont
 */
public class MetricTypeFilter<T> implements Transformer<Metric<?>, Metric<T>> {
    public static final MetricTypeFilter<Double> GAUGE_FILTER = new MetricTypeFilter<>(GAUGE);
    public static final MetricTypeFilter<Long> COUNTER_FILTER = new MetricTypeFilter<>(COUNTER);
    public static final MetricTypeFilter<AvailabilityType> AVAILABILITY_FILTER = new MetricTypeFilter<>(AVAILABILITY);

    private final MetricType<T> metricType;

    public MetricTypeFilter(MetricType<T> metricType) {
        this.metricType = metricType;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<Metric<T>> call(Observable<Metric<?>> observable) {
        return observable.filter(metric -> metric.getMetricId().getType() == metricType)
                .map(metric -> (Metric<T>) metric);
    }
}
