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

import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.MetricId;

import rx.Observable;
import rx.functions.Func1;
import rx.observables.MathObservable;

/**
 * Predefined aggregate functions usable with
 * {@link MetricsService#findGaugeData(MetricId, long, long, Func1[])}.
 *
 * @author john sanda
 * @author jay shaughnessy
 */
public interface Aggregate {

    /**
     * A function that emits a single item, the average of {@link DataPoint data points} as a double.
     */
    Func1<Observable<DataPoint<Double>>, Observable<Double>> Average = data ->
            MathObservable.averageDouble(data.map((DataPoint<Double> dataPoint) -> dataPoint.getValue()));

    /**
     * A function that emits a single item, the max of {@link DataPoint data points} as a double.
     */
    Func1<Observable<DataPoint<Double>>, Observable<Double>> Max = data ->
            MathObservable.max(data.map((DataPoint<Double> dataPoint) -> dataPoint.getValue()));

    /**
     * A function that emits a single item, the min of {@link DataPoint data points} as a double.
     */
    Func1<Observable<DataPoint<Double>>, Observable<Double>> Min = data ->
            MathObservable.min(data.map((DataPoint<Double> dataPoint) -> dataPoint.getValue()));

    /**
     * A function that emits a single item, the sum of {@link DataPoint data points} as a double.
     */
    Func1<Observable<DataPoint<Double>>, Observable<Double>> Sum = data ->
            MathObservable.sumDouble(data.map((DataPoint<Double> dataPoint) -> dataPoint.getValue()));

}
