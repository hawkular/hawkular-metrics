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
package org.hawkular.metrics.api.jaxrs.param;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.exception.RuntimeApiError;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.Duration;
import org.hawkular.metrics.model.param.Percentiles;
import org.hawkular.metrics.model.param.TimeRange;

import rx.Observable;
import rx.functions.Func4;

/**
 * @author Joel Takvorian
 */

public class TimeAndBucketParams {

    private final TimeRange timeRange;
    private final BucketConfig bucketConfig;
    private final List<Percentile> percentiles;

    private TimeAndBucketParams(TimeRange timeRange, BucketConfig bucketConfig, List<Percentile> percentiles) {
        this.timeRange = timeRange;
        this.bucketConfig = bucketConfig;
        this.percentiles = percentiles;
    }

    public static <T> DeferredBuilder<T> deferredBuilder(String start, String end) {
        return new DeferredBuilder<>(start, end);
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public BucketConfig getBucketConfig() {
        return bucketConfig;
    }

    public List<Percentile> getPercentiles() {
        return percentiles;
    }

    public static class DeferredBuilder<T> {
        private final String start;
        private final String end;
        private Boolean fromEarliest;
        private List<MetricId<T>> metricIds;
        private Integer bucketsCount;
        private Duration bucketDuration;
        private List<Percentile> percentiles;
        private Func4<String, String, Boolean, Collection<MetricId<T>>, Observable<TimeRange>> findTimeRange;

        private DeferredBuilder(String start, String end) {
            this.start = start;
            this.end = end;
        }

        public DeferredBuilder<T> fromEarliest(Boolean fromEarliest, List<MetricId<T>> metricIds, Func4<String, String,
                Boolean, Collection<MetricId<T>>, Observable<TimeRange>> findTimeRange) {
            this.fromEarliest = fromEarliest;
            this.metricIds = metricIds;
            this.findTimeRange = findTimeRange;
            return this;
        }

        public DeferredBuilder<T> fromEarliest(Boolean fromEarliest, MetricId<T> metricId, Func4<String, String,
                Boolean, Collection<MetricId<T>>, Observable<TimeRange>> findTimeRange) {
            this.fromEarliest = fromEarliest;
            this.metricIds = Collections.singletonList(metricId);
            this.findTimeRange = findTimeRange;
            return this;
        }

        public DeferredBuilder<T> bucketConfig(Integer bucketsCount, String bucketDuration) {
            this.bucketsCount = bucketsCount;
            this.bucketDuration = bucketDuration == null ? null : new DurationConverter().fromString(bucketDuration);
            return this;
        }

        public DeferredBuilder<T> bucketConfig(Integer bucketsCount, Duration bucketDuration) {
            this.bucketsCount = bucketsCount;
            this.bucketDuration = bucketDuration;
            return this;
        }

        public DeferredBuilder<T> percentiles(String percentiles) {
            this.percentiles = percentiles == null ? Collections.emptyList() : new PercentilesConverter()
                .fromString(percentiles).getPercentiles();
            return this;
        }

        public DeferredBuilder<T> percentiles(Percentiles percentiles) {
            this.percentiles = percentiles == null ? Collections.emptyList() : percentiles.getPercentiles();
            return this;
        }

        public Observable<TimeAndBucketParams> toObservable() {
            if (bucketsCount == null && bucketDuration == null) {
                return Observable.error(new RuntimeApiError("Either the buckets or bucketDuration parameter must be " +
                        "used"));
            }
            return findTimeRange.call(start, end, fromEarliest, metricIds).map(tr -> {
                BucketConfig bucketConfig = new BucketConfig(bucketsCount, bucketDuration, tr);
                if (!bucketConfig.isValid()) {
                    throw new RuntimeApiError(bucketConfig.getProblem());
                }
                return new TimeAndBucketParams(tr, bucketConfig, percentiles);
            });
        }
    }
}
