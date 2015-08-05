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
package org.hawkular.metrics.core.impl;

import org.hawkular.metrics.core.api.DataPoint;

import rx.Observable;

/**
 * @author Thomas Segismont
 */
final class BucketData<T> {
    private final int index;
    private final Observable<DataPoint<T>> dataPoints;

    public BucketData(int index, Observable<DataPoint<T>> dataPoints) {
        this.index = index;
        this.dataPoints = dataPoints;
    }

    public int getIndex() {
        return index;
    }

    public Observable<DataPoint<T>> getDataPoints() {
        return dataPoints;
    }
}
