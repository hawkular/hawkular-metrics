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

import static org.hawkular.metrics.core.impl.AvailabilityBucketPointMatcher.matchesAvailabilityBucketPoint;
import static org.hawkular.metrics.models.AvailabilityType.DOWN;
import static org.hawkular.metrics.models.AvailabilityType.UP;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import org.hawkular.metrics.models.AvailabilityBucketPoint;
import org.hawkular.metrics.models.AvailabilityType;
import org.hawkular.metrics.models.Buckets;
import org.hawkular.metrics.models.DataPoint;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class AvailabilityDataPointCollectorTest {
    private AvailabilityDataPointCollector collector;

    @Before
    public void setup() {
        Buckets buckets = new Buckets(10, 10, 10);
        collector = new AvailabilityDataPointCollector(buckets, 0);
    }

    @Test
    public void testWithOneUp() throws Exception {
        DataPoint<AvailabilityType> a1 = new DataPoint<>(15L, UP);
        collector.increment(a1);
        AvailabilityBucketPoint actual = collector.toBucketPoint();
        AvailabilityBucketPoint expected = new AvailabilityBucketPoint.Builder(10, 20)
                .setUptimeRatio(1.0)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketPoint(expected));
    }

    @Test
    public void testWithOneDown() throws Exception {
        DataPoint<AvailabilityType> a1 = new DataPoint<>(15L, DOWN);
        collector.increment(a1);
        AvailabilityBucketPoint actual = collector.toBucketPoint();
        AvailabilityBucketPoint expected = new AvailabilityBucketPoint.Builder(10, 20)
                .setDowntimeCount(1)
                .setDowntimeDuration(10)
                .setLastDowntime(20)
                .setUptimeRatio(0.0)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketPoint(expected));
    }

    @Test
    public void testWithOneDownOneUp() throws Exception {
        DataPoint<AvailabilityType> a1 = new DataPoint<>(12L, DOWN);
        DataPoint<AvailabilityType> a2 = new DataPoint<>(18L, UP);
        collector.increment(a1);
        collector.increment(a2);
        AvailabilityBucketPoint actual = collector.toBucketPoint();
        AvailabilityBucketPoint expected = new AvailabilityBucketPoint.Builder(10, 20)
                .setDowntimeCount(1)
                .setDowntimeDuration(8)
                .setLastDowntime(18)
                .setUptimeRatio(0.2)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketPoint(expected));
    }

    @Test
    public void testWithOneUpOneDown() throws Exception {
        DataPoint<AvailabilityType> a1 = new DataPoint<>(13L, UP);
        DataPoint<AvailabilityType> a2 = new DataPoint<>(17L, DOWN);
        collector.increment(a1);
        collector.increment(a2);
        AvailabilityBucketPoint actual = collector.toBucketPoint();
        AvailabilityBucketPoint expected = new AvailabilityBucketPoint.Builder(10, 20)
                .setDowntimeCount(1)
                .setDowntimeDuration(3)
                .setLastDowntime(20)
                .setUptimeRatio(0.7)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketPoint(expected));
    }

    @Test
    public void testWithTwoDown() throws Exception {
        DataPoint<AvailabilityType> a1 = new DataPoint<>(13L, DOWN);
        DataPoint<AvailabilityType> a2 = new DataPoint<>(17L, DOWN);
        collector.increment(a1);
        collector.increment(a2);
        AvailabilityBucketPoint actual = collector.toBucketPoint();
        AvailabilityBucketPoint expected = new AvailabilityBucketPoint.Builder(10, 20)
                .setDowntimeCount(1)
                .setDowntimeDuration(10)
                .setLastDowntime(20)
                .setUptimeRatio(0.0)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketPoint(expected));
    }

    @Test
    public void testWithTwoUp() throws Exception {
        DataPoint<AvailabilityType> a1 = new DataPoint<>(13L, UP);
        DataPoint<AvailabilityType> a2 = new DataPoint<>(17L, UP);
        collector.increment(a1);
        collector.increment(a2);
        AvailabilityBucketPoint actual = collector.toBucketPoint();
        AvailabilityBucketPoint expected = new AvailabilityBucketPoint.Builder(10, 20)
                .setDowntimeCount(0)
                .setDowntimeDuration(0)
                .setLastDowntime(0)
                .setUptimeRatio(1.0)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketPoint(expected));
    }
}