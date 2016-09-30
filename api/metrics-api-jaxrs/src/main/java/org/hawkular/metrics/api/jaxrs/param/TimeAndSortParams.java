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

import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.param.TimeRange;

import rx.Observable;
import rx.functions.Func4;

/**
 * @author Joel Takvorian
 */

public class TimeAndSortParams {

    private final TimeRange timeRange;
    private final int limit;
    private final Order order;

    private TimeAndSortParams(TimeRange timeRange, int limit, Order order) {
        this.timeRange = timeRange;
        this.limit = limit;
        this.order = order;
    }

    public static <T> DeferredBuilder<T> deferredBuilder(String start, String end) {
        return new DeferredBuilder<>(start, end);
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public int getLimit() {
        return limit;
    }

    public Order getOrder() {
        return order;
    }

    public static class DeferredBuilder<T> {
        private final String start;
        private final String end;
        private Boolean fromEarliest;
        private List<MetricId<T>> metricIds;
        private Order order;
        private int limit;
        private boolean forString = false;
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

        public DeferredBuilder<T> sortOptions(Integer limit, String order) {
            this.limit = limit == null ? 0 : limit;
            this.order = order == null ? null : Order.fromText(order);
            return this;
        }

        public DeferredBuilder<T> sortOptions(Integer limit, Order order) {
            this.limit = limit == null ? 0 : limit;
            this.order = order;
            return this;
        }

        public DeferredBuilder<T> forString() {
            this.forString = true;
            return this;
        }

        private Order getOrderForString() {
            if (order != null) {
                return order;
            }
            if (limit != 0 && start != null && end == null) {
                return Order.ASC;
            }
            return Order.DESC;
        }

        public Observable<TimeAndSortParams> toObservable() {
            final Order fOrder;
            if (forString) {
                // Specific sorting behaviour for String metric type
                fOrder = getOrderForString();
            } else {
                fOrder = order == null ? Order.defaultValue(limit, start, end) : order;
            }
            return findTimeRange.call(start, end, fromEarliest, metricIds).map(tr -> new TimeAndSortParams(tr, limit,
                    fOrder));
        }
    }
}
