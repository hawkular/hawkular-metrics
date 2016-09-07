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
package org.hawkular.metrics.core.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.assertj.core.util.DoubleComparator;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.RatioMap;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import rx.Observable;

/**
 * @author Joel Takvorian
 */
public class MetricsServiceImplTest {

    private final MetricsServiceImpl service = new MetricsServiceImpl();
    private final DoubleComparator dc = new DoubleComparator(0.001);

    @Test
    public void shouldBuildRatioMapSeries() throws Exception {
        List<DataPoint<AvailabilityType>> serie1 = ImmutableList.of(
                new DataPoint<>(0L, AvailabilityType.UP),
                new DataPoint<>(5L, AvailabilityType.DOWN),
                new DataPoint<>(10L, AvailabilityType.UP));
        List<DataPoint<AvailabilityType>> serie2 = ImmutableList.of(
                new DataPoint<>(0L, AvailabilityType.UP),
                new DataPoint<>(7L, AvailabilityType.DOWN),
                new DataPoint<>(10L, AvailabilityType.ADMIN));
        List<DataPoint<AvailabilityType>> serie3 = ImmutableList.of(
                new DataPoint<>(1L, AvailabilityType.UP),
                new DataPoint<>(7L, AvailabilityType.DOWN));
        Observable<Observable<DataPoint<AvailabilityType>>> allSeries = Observable.just(
                Observable.from(serie1),
                Observable.from(serie2),
                Observable.from(serie3));
        List<DataPoint<RatioMap>> result = service.buildRatioMapSeries(allSeries, Order.ASC, AvailabilityType::getText)
                .toList()
                .toBlocking()
                .single();

        assertThat(result).extracting(DataPoint::getTimestamp).containsExactly(0L, 1L, 5L, 7L, 10L);
        assertThat(result).extracting(dp -> dp.getValue().getSamples())
                .containsExactly(2, 3, 3, 3, 3);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("up"))
                .usingElementComparator(dc)
                .containsExactly(1d, 1d, 0.6667, 0d, 0.3333);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("down"))
                .usingElementComparator(dc)
                .containsExactly(0d, 0d, 0.3333, 1d, 0.3333);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("admin"))
                .usingElementComparator(dc)
                .containsExactly(0d, 0d, 0d, 0d, 0.3333);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("unknown"))
                .usingElementComparator(dc)
                .containsExactly(0d, 0d, 0d, 0d, 0d);
    }

    @Test
    public void shouldBuildRatioMapSeriesDescendingOrder() throws Exception {
        List<DataPoint<AvailabilityType>> serie1 = ImmutableList.of(
                new DataPoint<>(10L, AvailabilityType.UP),
                new DataPoint<>(5L, AvailabilityType.DOWN),
                new DataPoint<>(0L, AvailabilityType.UP));
        List<DataPoint<AvailabilityType>> serie2 = ImmutableList.of(
                new DataPoint<>(10L, AvailabilityType.ADMIN),
                new DataPoint<>(7L, AvailabilityType.DOWN),
                new DataPoint<>(0L, AvailabilityType.UP));
        List<DataPoint<AvailabilityType>> serie3 = ImmutableList.of(
                new DataPoint<>(7L, AvailabilityType.DOWN),
                new DataPoint<>(1L, AvailabilityType.UP));
        Observable<Observable<DataPoint<AvailabilityType>>> allSeries = Observable.just(
                Observable.from(serie1),
                Observable.from(serie2),
                Observable.from(serie3));
        List<DataPoint<RatioMap>> result = service.buildRatioMapSeries(allSeries, Order.DESC, AvailabilityType::getText)
                .toList()
                .toBlocking()
                .single();

        assertThat(result).extracting(DataPoint::getTimestamp).containsExactly(10L, 7L, 5L, 1L, 0L);
        assertThat(result).extracting(dp -> dp.getValue().getSamples())
                .containsExactly(3, 3, 3, 3, 2);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("up"))
                .usingElementComparator(dc)
                .containsExactly(0.3333, 0d, 0.6667, 1d, 1d);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("down"))
                .usingElementComparator(dc)
                .containsExactly(0.3333, 1d, 0.3333, 0d, 0d);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("admin"))
                .usingElementComparator(dc)
                .containsExactly(0.3333, 0d, 0d, 0d, 0d);
        assertThat(result).extracting(dp -> dp.getValue().getRatio("unknown"))
                .usingElementComparator(dc)
                .containsExactly(0d, 0d, 0d, 0d, 0d);
    }

}