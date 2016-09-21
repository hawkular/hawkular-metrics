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
package org.hawkular.metrics.core.service.transformers;

import static java.util.stream.Collectors.toSet;

import static org.hawkular.metrics.core.service.Order.ASC;
import static org.hawkular.metrics.core.service.Order.DESC;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Stream;

import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.RatioMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import rx.Observable;

/**
 * Accumulates availability (or string) data points from multiple series to create an aggregated series of
 * {@link RatioMap}
 *
 * @author Joel Takvorian
 */
public final class RatioMapCollector<T> {

    private final Collection<SingleSeries<T>> series = new ArrayList<>();
    private final Order order;
    private final Function<T, String> valueToString;

    public RatioMapCollector(final Order order, Function<T, String> valueToString) {
        this.order = order;
        this.valueToString = valueToString;
    }

    public SingleSeries<T> start() {
        SingleSeries<T> single = new SingleSeries<>();
        series.add(single);
        return single;
    }

    public void increment(SingleSeries<T> single, DataPoint<T> dataPoint) {
        if (order == ASC) {
            single.increment(dataPoint);
        } else {
            single.incrementReversed(dataPoint);
        }
    }

    public Aggregator aggregator() {
        return new Aggregator();
    }

    public static class SingleSeries<T> {

        private final Map<T, RangeSet<Long>> rangeSets = new HashMap<>();
        private DataPoint<T> previous;
        private DataPoint<T> first;

        private SingleSeries() {
        }

        @VisibleForTesting
        RangeSet<Long> getRangeSet(T key) {
            if (!rangeSets.containsKey(key)) {
                rangeSets.put(key, TreeRangeSet.create());
            }
            return rangeSets.get(key);
        }

        private void increment(DataPoint<T> dataPoint) {
            if (previous == null) {
                previous = dataPoint;
                getRangeSet(dataPoint.getValue()).add(Range.singleton(dataPoint.getTimestamp()));
                return;
            }
            if (dataPoint.getTimestamp() <= previous.getTimestamp()) {
                throw new IllegalStateException("Expected stream sorted in time ascending order");
            }
            if (previous.getValue() == dataPoint.getValue()) {
                getRangeSet(previous.getValue()).add(Range.closed(previous.getTimestamp(), dataPoint.getTimestamp()));
            } else {
                getRangeSet(previous.getValue()).add(Range.closedOpen(previous.getTimestamp(), dataPoint.getTimestamp()));
                getRangeSet(dataPoint.getValue()).add(Range.singleton(dataPoint.getTimestamp()));
            }
            previous = dataPoint;
        }

        private void incrementReversed(DataPoint<T> dataPoint) {
            if (first == null) {
                first = dataPoint;
                previous = dataPoint;
                getRangeSet(dataPoint.getValue()).add(Range.singleton(dataPoint.getTimestamp()));
                return;
            }
            if (dataPoint.getTimestamp() >= previous.getTimestamp()) {
                throw new IllegalStateException("Expected stream sorted in time descending order");
            }
            if (previous.getValue() == dataPoint.getValue()) {
                getRangeSet(previous.getValue()).add(Range.closed(dataPoint.getTimestamp(), previous.getTimestamp()));
            } else {
                getRangeSet(dataPoint.getValue()).add(Range.closedOpen(dataPoint.getTimestamp(), previous.getTimestamp()));
            }
            previous = dataPoint;
        }

        private void terminate(long endFrame, Order order) {
            if (previous != null && order == ASC) {
                getRangeSet(previous.getValue())
                        .add(Range.closed(previous.getTimestamp(), endFrame));
            } else if (first != null && order == DESC) {
                getRangeSet(first.getValue())
                        .add(Range.closed(first.getTimestamp(), endFrame));
            }
        }

        private Collection<Long> timestamps() {
            return rangeSets.values().stream()
                    .flatMap(rangeset -> rangeset.asRanges().stream())
                    .flatMap(range -> Stream.of(range.lowerEndpoint(), range.upperEndpoint()))
                    .collect(toSet());
        }

        private Optional<T> getValueAt(long timestamp) {
            return rangeSets.entrySet().stream()
                    .filter(entry -> entry.getValue().contains(timestamp))
                    .map(Map.Entry::getKey)
                    .findAny();
        }
    }

    public class Aggregator {
        private final Set<Long> allTimestamps;

        private Aggregator() {
            if (order == ASC) {
                allTimestamps = new TreeSet<>();
            } else {
                allTimestamps = new TreeSet<>(Comparator.reverseOrder());
            }
        }

        public Aggregator aggregate(SingleSeries<T> single) {
            allTimestamps.addAll(single.timestamps());
            return this;
        }

        public Aggregated done() {
            allTimestamps.stream().max(Comparator.naturalOrder())
                    .ifPresent(endFrame -> series.forEach(s -> s.terminate(endFrame, order)));
            return new Aggregated(allTimestamps);
        }
    }

    public class Aggregated {
        private final Set<Long> allTimestamps;

        private Aggregated(Set<Long> allTimestamps) {
            this.allTimestamps = allTimestamps;
        }

        public Observable<DataPoint<RatioMap>> toDataPoints() {
            return Observable.from(allTimestamps).map(this::toDataPoint);
        }

        private DataPoint<RatioMap> toDataPoint(long timestamp) {
            RatioMap.Builder builder = RatioMap.builder();
            series.forEach(c -> c.getValueAt(timestamp)
                    .map(valueToString)
                    .ifPresent(builder::add));
            return new DataPoint<>(timestamp, builder.build());
        }
    }
}
