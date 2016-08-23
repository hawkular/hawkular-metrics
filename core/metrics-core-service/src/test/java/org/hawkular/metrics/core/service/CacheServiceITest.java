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

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.testng.Assert.assertEquals;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.cache.DataPointKey;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.MetricId;
import org.infinispan.Cache;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;

import rx.Single;

/**
 * @author jsanda
 */
public class CacheServiceITest extends BaseITest {

    @Test
    public void storeGaugeDataPoints() {
        long timeSlice = currentMinute().getMillis();

        String tenantId = "storeGaugeDataPoints";
        MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, "G1");

        DataPoint<Double> d1 = new DataPoint<>(timeSlice + 100L, 1.1);
        DataPoint<Double> d2 = new DataPoint<>(timeSlice + 200L, 2.2);
        DataPoint<Double> d3 = new DataPoint<>(timeSlice + 300L, 3.3);

        Single<DataPoint<? extends Number>> s1 = cacheService.put(metricId, d1);
        Single<DataPoint<? extends Number>> s2 = cacheService.put(metricId, d2);
        Single<DataPoint<? extends Number>> s3 = cacheService.put(metricId, d3);

        Single.merge(s1, s2, s3).toCompletable().await(10, TimeUnit.SECONDS);

        Cache<DataPointKey, DataPoint<? extends Number>> cache = cacheService.getRawDataCache();
        Collection<DataPoint<? extends Number>> values = cache.getAdvancedCache().getGroup(Long.toString(timeSlice))
                .values();

        assertEquals(ImmutableSet.copyOf(values), ImmutableSet.of(d1, d2, d3));
     }

}
