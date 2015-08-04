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

import java.util.ArrayList;
import java.util.List;

import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;

import rx.Observable.Operator;
import rx.Subscriber;

/**
 * Base class for an operator taking {@link DataPoint<T>} values from a source {@link rx.Observable} and emits bucketed
 * data.
 * <p>
 * The number of buckets emitted is determined by the {@link Buckets} configuration specified in the constructor.
 * <p>
 * <strong>The source observable is expected to emit data ordered by timestamp.</strong>
 *
 * @param <T>     type of metric value
 * @param <POINT> type of bucket points, like {@link org.hawkular.metrics.core.api.GaugeBucketDataPoint}
 *
 * @author Thomas Segismont
 */
public abstract class BucketedOutputOperator<T, POINT> implements Operator<POINT, DataPoint<T>> {
    protected final Buckets buckets;

    /**
     * @param buckets the bucket configuration
     */
    protected BucketedOutputOperator(Buckets buckets) {
        this.buckets = buckets;
    }

    @Override
    public Subscriber<? super DataPoint<T>> call(Subscriber<? super POINT> subscriber) {
        return new Subscriber<DataPoint<T>>() {
            List<DataPoint<T>> buffer;
            int emittedBuckets;

            @Override
            public void onNext(DataPoint<T> dataPoint) {
                while (dataPoint.getTimestamp() - getFrom() > buckets.getStep()) {
                    // Data point does not belong to the current bucket
                    if (buffer != null) {
                        // The current bucket has data
                        POINT point = newPointInstance(getFrom(), getTo(), buffer);
                        buffer = null;
                        emitBucket(point);
                    } else {
                        // No data in the current bucket
                        emitBucket(newEmptyPointInstance(getFrom(), getTo()));
                    }
                }
                if (buffer == null) {
                    buffer = new ArrayList<>();
                }
                buffer.add(dataPoint);
            }

            @Override
            public void onCompleted() {
                List<DataPoint<T>> oldBuffer = buffer;
                buffer = null;
                try {
                    // First emit the last bucket with data
                    if (oldBuffer != null) {
                        emitBucket(newPointInstance(getFrom(), getTo(), oldBuffer));
                    }
                    // Then emit missing buckets
                    while (emittedBuckets < buckets.getCount()) {
                        emitBucket(newEmptyPointInstance(getFrom(), getTo()));
                    }
                } catch (Throwable t) {
                    onError(t);
                    return;
                }
                subscriber.onCompleted();
            }

            @Override
            public void onError(Throwable t) {
                buffer = null;
                subscriber.onError(t);
            }

            private void emitBucket(POINT point) {
                subscriber.onNext(point);
                emittedBuckets++;
            }

            private long getFrom() {
                return buckets.getStart() + emittedBuckets * buckets.getStep();
            }

            private long getTo() {
                return getFrom() + buckets.getStep();
            }
        };
    }

    /**
     * Create an empty bucket data point instance.
     *
     * @param from start timestamp of the bucket
     * @param to   end timestamp of the bucket
     *
     * @return an empty bucket data point
     */
    protected abstract POINT newEmptyPointInstance(long from, long to);

    /**
     * Create a bucket data point from the metric data in this bucket.
     *
     * @param from        start timestamp of the bucket
     * @param to          end timestamp of the bucket
     * @param metricDatas metric data in this bucket, ordered by timestamp
     *
     * @return a bucket data point summurazing the metric data
     */
    protected abstract POINT newPointInstance(long from, long to, List<DataPoint<T>> metricDatas);
}
