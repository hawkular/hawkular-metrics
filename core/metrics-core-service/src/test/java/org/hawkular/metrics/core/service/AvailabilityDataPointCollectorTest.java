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

import static org.hawkular.metrics.core.service.AvailabilityBucketPointMatcher.matchesAvailabilityBucketPoint;
import static org.hawkular.metrics.model.AvailabilityType.ADMIN;
import static org.hawkular.metrics.model.AvailabilityType.DOWN;
import static org.hawkular.metrics.model.AvailabilityType.UNKNOWN;
import static org.hawkular.metrics.model.AvailabilityType.UP;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import org.hawkular.metrics.model.AvailabilityBucketPoint;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
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
                .setUpDuration(10)
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
                .setNotUptimeCount(1)
                .setDownDuration(10)
                .setLastNotUptime(20)
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
                .setNotUptimeCount(1)
                .setUpDuration(2)
                .setDownDuration(8)
                .setLastNotUptime(18)
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
                .setNotUptimeCount(1)
                .setUpDuration(7)
                .setDownDuration(3)
                .setLastNotUptime(20)
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
                .setNotUptimeCount(1)
                .setDownDuration(10)
                .setLastNotUptime(20)
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
                .setUpDuration(10)
                .setNotUptimeCount(0)
                .setLastNotUptime(0)
                .setUptimeRatio(1.0)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketPoint(expected));
    }

    @Test
    public void testWithAll() throws Exception {
        DataPoint<AvailabilityType> a1 = new DataPoint<>(13L, UP);
        DataPoint<AvailabilityType> a2 = new DataPoint<>(14L, DOWN);
        DataPoint<AvailabilityType> a3 = new DataPoint<>(15L, UNKNOWN);
        DataPoint<AvailabilityType> a4 = new DataPoint<>(16L, UP);
        DataPoint<AvailabilityType> a5 = new DataPoint<>(17L, ADMIN);
        DataPoint<AvailabilityType> a6 = new DataPoint<>(18L, ADMIN);
        collector.increment(a1);
        collector.increment(a2);
        collector.increment(a3);
        collector.increment(a4);
        collector.increment(a5);
        collector.increment(a6);
        AvailabilityBucketPoint actual = collector.toBucketPoint();
        AvailabilityBucketPoint expected = new AvailabilityBucketPoint.Builder(10, 20)
                .setNotUptimeCount(2)
                .setAdminDuration(3)
                .setDownDuration(1)
                .setUnknownDuration(1)
                .setUpDuration(5)
                .setLastNotUptime(20)
                .setUptimeRatio(0.5)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketPoint(expected));
    }

    @Test
    public void testWithAll2() throws Exception {
        DataPoint<AvailabilityType> a1 = new DataPoint<>(13L, DOWN);
        DataPoint<AvailabilityType> a2 = new DataPoint<>(14L, DOWN);
        DataPoint<AvailabilityType> a3 = new DataPoint<>(15L, UNKNOWN);
        DataPoint<AvailabilityType> a4 = new DataPoint<>(16L, UP);
        DataPoint<AvailabilityType> a5 = new DataPoint<>(17L, ADMIN);
        DataPoint<AvailabilityType> a6 = new DataPoint<>(18L, UP);
        collector.increment(a1);
        collector.increment(a2);
        collector.increment(a3);
        collector.increment(a4);
        collector.increment(a5);
        collector.increment(a6);
        AvailabilityBucketPoint actual = collector.toBucketPoint();
        AvailabilityBucketPoint expected = new AvailabilityBucketPoint.Builder(10, 20)
                .setNotUptimeCount(2)
                .setAdminDuration(1)
                .setDownDuration(5)
                .setUnknownDuration(1)
                .setUpDuration(3)
                .setLastNotUptime(18)
                .setUptimeRatio(0.3)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketPoint(expected));
    }

}