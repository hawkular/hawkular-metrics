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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hawkular.metrics.core.service.Order.ASC;
import static org.hawkular.metrics.core.service.Order.DESC;

import java.util.List;
import java.util.function.Function;

import org.assertj.core.util.DoubleComparator;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.RatioMap;
import org.junit.Test;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

/**
 * @author Joel Takvorian
 */
public class RatioMapCollectorTest {

    private final DoubleComparator dc = new DoubleComparator(0.001);
    private final Function<AvailabilityType, String> valueToString = AvailabilityType::getText;

    @Test
    public void shouldIncrementSingleSerie() {
        RatioMapCollector<AvailabilityType> collector = new RatioMapCollector<>(ASC, valueToString);
        RatioMapCollector.SingleSeries<AvailabilityType> singleSeries = collector.start();
        collector.increment(singleSeries, new DataPoint<>(0L, AvailabilityType.UP));
        collector.increment(singleSeries, new DataPoint<>(1L, AvailabilityType.UP));
        collector.increment(singleSeries, new DataPoint<>(2L, AvailabilityType.DOWN));
        collector.increment(singleSeries, new DataPoint<>(3L, AvailabilityType.UP));
        RatioMapCollector<AvailabilityType>.Aggregator aggregator = collector.aggregator();
        aggregator.aggregate(singleSeries).done();

        RangeSet<Long> upRanges = singleSeries.getRangeSet(AvailabilityType.UP);
        assertThat(upRanges).isNotNull();
        assertThat(upRanges.asRanges()).containsExactly(Range.closedOpen(0L, 2L), Range.singleton(3L));

        RangeSet<Long> downRanges = singleSeries.getRangeSet(AvailabilityType.DOWN);
        assertThat(downRanges).isNotNull();
        assertThat(downRanges.asRanges()).containsExactly(Range.closedOpen(2L, 3L));

        RangeSet<Long> adminRanges = singleSeries.getRangeSet(AvailabilityType.ADMIN);
        assertThat(adminRanges).isNotNull();
        assertThat(adminRanges.asRanges()).isEmpty();

        RangeSet<Long> unknownRanges = singleSeries.getRangeSet(AvailabilityType.UNKNOWN);
        assertThat(unknownRanges).isNotNull();
        assertThat(unknownRanges.asRanges()).isEmpty();
    }

    @Test
    public void shouldMixMultipleSeries() {
        RatioMapCollector<AvailabilityType> collector = new RatioMapCollector<>(ASC, valueToString);
        // Mixing "first" and "two" increment is deliberate
        RatioMapCollector.SingleSeries<AvailabilityType> first = collector.start();
        collector.increment(first, new DataPoint<>(0L, AvailabilityType.UP));
        collector.increment(first, new DataPoint<>(10L, AvailabilityType.UP));
        RatioMapCollector.SingleSeries<AvailabilityType> second = collector.start();
        collector.increment(second, new DataPoint<>(5L, AvailabilityType.ADMIN));
        collector.increment(second, new DataPoint<>(15L, AvailabilityType.DOWN));
        collector.increment(second, new DataPoint<>(25L, AvailabilityType.DOWN));
        collector.increment(first, new DataPoint<>(20L, AvailabilityType.DOWN));
        collector.increment(first, new DataPoint<>(30L, AvailabilityType.UP));
        collector.increment(second, new DataPoint<>(30L, AvailabilityType.UP));
        collector.increment(second, new DataPoint<>(35L, AvailabilityType.ADMIN));
        RatioMapCollector<AvailabilityType>.Aggregated aggregated = collector.aggregator()
                .aggregate(first)
                .aggregate(second)
                .done();

        List<DataPoint<RatioMap>> result = aggregated.toDataPoints().toList().toBlocking().single();
        assertThat(result).extracting(DataPoint::getTimestamp).containsExactly(0L, 5L, 15L, 20L, 30L, 35L);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("up"))
                .usingElementComparator(dc)
                .containsExactly(1d, 0.5, 0.5, 0d, 1d, 0.5);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("down"))
                .usingElementComparator(dc)
                .containsExactly(0d, 0d, 0.5, 1d, 0d, 0d);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("admin"))
                .usingElementComparator(dc)
                .containsExactly(0d, 0.5, 0d, 0d, 0d, 0.5);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("unknown"))
                .usingElementComparator(dc)
                .containsExactly(0d, 0d, 0d, 0d, 0d, 0d);
        assertThat(result).extracting(dp -> dp.getValue().getSamples()).containsExactly(1, 2, 2, 2, 2, 2);
    }

    @Test
    public void shouldGetEmptyResultWhenNoSeries() {
        RatioMapCollector<AvailabilityType> collector = new RatioMapCollector<>(ASC, valueToString);
        List<DataPoint<RatioMap>> result =  collector.aggregator().done().toDataPoints().toList()
                .toBlocking().single();
        assertThat(result).isEmpty();
    }

    @Test
    public void shouldGetEmptyResultWhenNoIncrement() {
        RatioMapCollector<AvailabilityType> collector = new RatioMapCollector<>(ASC, valueToString);
        RatioMapCollector.SingleSeries<AvailabilityType> singleSeries = collector.start();
        RatioMapCollector<AvailabilityType>.Aggregated aggregated = collector.aggregator()
                .aggregate(singleSeries)
                .done();
        List<DataPoint<RatioMap>> result =  aggregated.toDataPoints().toList().toBlocking().single();
        assertThat(result).isEmpty();
    }

    @Test
    public void shouldIncrementSinglePoint() {
        RatioMapCollector<AvailabilityType> collector = new RatioMapCollector<>(ASC, valueToString);
        RatioMapCollector.SingleSeries<AvailabilityType> singleSeries = collector.start();
        collector.increment(singleSeries, new DataPoint<>(0L, AvailabilityType.UP));
        RatioMapCollector<AvailabilityType>.Aggregator aggregator = collector.aggregator();
        aggregator.aggregate(singleSeries).done();

        RangeSet<Long> upRanges = singleSeries.getRangeSet(AvailabilityType.UP);
        assertThat(upRanges.asRanges()).containsExactly(Range.singleton(0L));

        RangeSet<Long> downRanges = singleSeries.getRangeSet(AvailabilityType.DOWN);
        assertThat(downRanges.asRanges()).isEmpty();
    }

    @Test
    public void shouldIncrementSinglePointDescending() {
        RatioMapCollector<AvailabilityType> collector = new RatioMapCollector<>(DESC, valueToString);
        RatioMapCollector.SingleSeries<AvailabilityType> singleSeries = collector.start();
        collector.increment(singleSeries, new DataPoint<>(0L, AvailabilityType.UP));
        RatioMapCollector<AvailabilityType>.Aggregator aggregator = collector.aggregator();
        aggregator.aggregate(singleSeries).done();

        RangeSet<Long> upRanges = singleSeries.getRangeSet(AvailabilityType.UP);
        assertThat(upRanges.asRanges()).containsExactly(Range.singleton(0L));

        RangeSet<Long> downRanges = singleSeries.getRangeSet(AvailabilityType.DOWN);
        assertThat(downRanges.asRanges()).isEmpty();
    }

    @Test
    public void shouldIncrementSingleSegment() {
        RatioMapCollector<AvailabilityType> collector = new RatioMapCollector<>(ASC, valueToString);
        RatioMapCollector.SingleSeries<AvailabilityType> singleSeries = collector.start();
        collector.increment(singleSeries, new DataPoint<>(0L, AvailabilityType.UP));
        collector.increment(singleSeries, new DataPoint<>(5L, AvailabilityType.DOWN));
        RatioMapCollector<AvailabilityType>.Aggregator aggregator = collector.aggregator();
        aggregator.aggregate(singleSeries).done();

        RangeSet<Long> upRanges = singleSeries.getRangeSet(AvailabilityType.UP);
        assertThat(upRanges.asRanges()).containsExactly(Range.closedOpen(0L, 5L));

        RangeSet<Long> downRanges = singleSeries.getRangeSet(AvailabilityType.DOWN);
        assertThat(downRanges.asRanges()).containsExactly(Range.singleton(5L));
    }

    @Test
    public void shouldIncrementSingleSegmentDescending() {
        RatioMapCollector<AvailabilityType> collector = new RatioMapCollector<>(DESC, valueToString);
        RatioMapCollector.SingleSeries<AvailabilityType> singleSeries = collector.start();
        collector.increment(singleSeries, new DataPoint<>(5L, AvailabilityType.DOWN));
        collector.increment(singleSeries, new DataPoint<>(0L, AvailabilityType.UP));
        RatioMapCollector<AvailabilityType>.Aggregator aggregator = collector.aggregator();
        aggregator.aggregate(singleSeries).done();

        RangeSet<Long> upRanges = singleSeries.getRangeSet(AvailabilityType.UP);
        assertThat(upRanges.asRanges()).containsExactly(Range.closedOpen(0L, 5L));

        RangeSet<Long> downRanges = singleSeries.getRangeSet(AvailabilityType.DOWN);
        assertThat(downRanges.asRanges()).containsExactly(Range.singleton(5L));
    }

    @Test
    public void shouldIncrementSingleSerieDescending() {
        RatioMapCollector<AvailabilityType> collector = new RatioMapCollector<>(DESC, valueToString);
        RatioMapCollector.SingleSeries<AvailabilityType> singleSeries = collector.start();
        collector.increment(singleSeries, new DataPoint<>(3L, AvailabilityType.UP));
        collector.increment(singleSeries, new DataPoint<>(2L, AvailabilityType.DOWN));
        collector.increment(singleSeries, new DataPoint<>(1L, AvailabilityType.UP));
        collector.increment(singleSeries, new DataPoint<>(0L, AvailabilityType.UP));
        RatioMapCollector<AvailabilityType>.Aggregator aggregator = collector.aggregator();
        aggregator.aggregate(singleSeries).done();

        RangeSet<Long> upRanges = singleSeries.getRangeSet(AvailabilityType.UP);
        assertThat(upRanges).isNotNull();
        assertThat(upRanges.asRanges()).containsExactly(Range.closedOpen(0L, 2L), Range.singleton(3L));

        RangeSet<Long> downRanges = singleSeries.getRangeSet(AvailabilityType.DOWN);
        assertThat(downRanges).isNotNull();
        assertThat(downRanges.asRanges()).containsExactly(Range.closedOpen(2L, 3L));

        RangeSet<Long> adminRanges = singleSeries.getRangeSet(AvailabilityType.ADMIN);
        assertThat(adminRanges).isNotNull();
        assertThat(adminRanges.asRanges()).isEmpty();

        RangeSet<Long> unknownRanges = singleSeries.getRangeSet(AvailabilityType.UNKNOWN);
        assertThat(unknownRanges).isNotNull();
        assertThat(unknownRanges.asRanges()).isEmpty();
    }

    @Test
    public void shouldMixMultipleSeriesDescending() {
        RatioMapCollector<AvailabilityType> collector = new RatioMapCollector<>(DESC, valueToString);
        // Mixing "first" and "two" increment is deliberate
        RatioMapCollector.SingleSeries<AvailabilityType> first = collector.start();
        collector.increment(first, new DataPoint<>(30L, AvailabilityType.UP));
        collector.increment(first, new DataPoint<>(20L, AvailabilityType.DOWN));
        RatioMapCollector.SingleSeries<AvailabilityType> second = collector.start();
        collector.increment(second, new DataPoint<>(35L, AvailabilityType.ADMIN));
        collector.increment(second, new DataPoint<>(30L, AvailabilityType.UP));
        collector.increment(second, new DataPoint<>(25L, AvailabilityType.DOWN));
        collector.increment(second, new DataPoint<>(15L, AvailabilityType.DOWN));
        collector.increment(first, new DataPoint<>(10L, AvailabilityType.UP));
        collector.increment(first, new DataPoint<>(0L, AvailabilityType.UP));
        collector.increment(second, new DataPoint<>(5L, AvailabilityType.ADMIN));
        RatioMapCollector<AvailabilityType>.Aggregated aggregated = collector.aggregator()
                .aggregate(first)
                .aggregate(second)
                .done();

        List<DataPoint<RatioMap>> result = aggregated.toDataPoints().toList().toBlocking().single();
        assertThat(result).extracting(DataPoint::getTimestamp).containsExactly(35L, 30L, 20L, 15L, 5L, 0L);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("up"))
                .usingElementComparator(dc)
                .containsExactly(0.5, 1d, 0d, 0.5, 0.5, 1d);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("down"))
                .usingElementComparator(dc)
                .containsExactly(0d, 0d, 1d, 0.5, 0d, 0d);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("admin"))
                .usingElementComparator(dc)
                .containsExactly(0.5, 0d, 0d, 0d, 0.5, 0d);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("unknown"))
                .usingElementComparator(dc)
                .containsExactly(0d, 0d, 0d, 0d, 0d, 0d);
        assertThat(result).extracting(dp -> dp.getValue().getSamples()).containsExactly(2, 2, 2, 2, 2, 1);
    }
}
