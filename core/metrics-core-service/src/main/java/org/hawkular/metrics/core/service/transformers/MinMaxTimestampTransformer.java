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
package org.hawkular.metrics.core.service.transformers;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;

import rx.Observable;
import rx.Observable.Transformer;

/**
 * Transforms a sequence of {@code Metric} to provide min and max timestamp fields.
 *
 * @author Thomas Segismont
 */
public class MinMaxTimestampTransformer<T> implements Transformer<Metric<T>, Metric<T>> {
    private final MetricsService metricsService;

    public MinMaxTimestampTransformer(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @Override
    public Observable<Metric<T>> call(Observable<Metric<T>> metricObservable) {
        return metricObservable.flatMap(metric -> {
            long now = System.currentTimeMillis();
            MetricId<T> metricId = metric.getMetricId();
            return metricsService.findDataPoints(metricId, 0, now, 1, Order.ASC)
                    .zipWith(metricsService.findDataPoints(metricId, 0, now, 1, Order.DESC), (p1, p2)
                            -> new Metric<>(metric, p1.getTimestamp(), p2.getTimestamp()))
                    .switchIfEmpty(Observable.just(metric));
        });
    }
}
