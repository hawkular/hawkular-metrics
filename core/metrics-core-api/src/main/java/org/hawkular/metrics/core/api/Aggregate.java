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
package org.hawkular.metrics.core.api;

import rx.Observable;
import rx.functions.Func1;
import rx.observables.MathObservable;

/**
 * Predefined aggregate functions usable with
 * {@link MetricsService#findGaugeData(String, MetricId, Long, Long, Func1...).
 *
 * @author john sanda
 * @author jay shaughnessy
 */
public interface Aggregate {
    Func1<Observable<DataPoint<Double>>, Observable<Double>> Average = data -> {
        return MathObservable.averageDouble(data.map(DataPoint::getValue));
    };

    Func1<Observable<DataPoint<Double>>, Observable<Double>> Max = data -> {
        return MathObservable.max(data.map(DataPoint::getValue));
    };

    Func1<Observable<DataPoint<Double>>, Observable<Double>> Min = data -> {
        return MathObservable.min(data.map(DataPoint::getValue));
    };

    Func1<Observable<DataPoint<Double>>, Observable<Double>> Sum = data -> {
        return MathObservable.sumDouble(data.map(DataPoint::getValue));
    };
}
