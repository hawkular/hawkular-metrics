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
package org.hawkular.metrics.core.impl.cassandra;

import static org.hawkular.metrics.core.api.AvailabilityType.DOWN;
import static org.hawkular.metrics.core.api.AvailabilityType.UP;
import static org.hawkular.metrics.core.impl.cassandra.AvailabilityBucketDataPointMatcher
        .matchesAvailabilityBucketDataPoint;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityBucketDataPoint;
import org.hawkular.metrics.core.api.Buckets;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * @author Thomas Segismont
 */
public class AvailabilityBucketedOutputMapperTest {
    private AvailabilityBucketedOutputMapper mapper;

    @Before
    public void setup() {
        Buckets buckets = new Buckets(1, 10, 10);
        mapper = new AvailabilityBucketedOutputMapper(buckets);
    }

    @Test
    public void newEmptyPointInstance() throws Exception {
        assertTrue("Expected an empty instance", mapper.newEmptyPointInstance(1, 100).isEmpty());
    }

    @Test
    public void testWithOneUp() throws Exception {
        Availability a1 = new Availability(15, UP);
        AvailabilityBucketDataPoint actual = mapper.newPointInstance(10, 20, ImmutableList.of(a1));
        AvailabilityBucketDataPoint expected = new AvailabilityBucketDataPoint.Builder(10, 20)
                .setUptimeRatio(1.0)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketDataPoint(expected));
    }

    @Test
    public void testWithOneDown() throws Exception {
        Availability a1 = new Availability(15, DOWN);
        AvailabilityBucketDataPoint actual = mapper.newPointInstance(10, 20, ImmutableList.of(a1));
        AvailabilityBucketDataPoint expected = new AvailabilityBucketDataPoint.Builder(10, 20)
                .setDowntimeCount(1)
                .setDowntimeDuration(10)
                .setLastDowntime(20)
                .setUptimeRatio(0.0)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketDataPoint(expected));
    }

    @Test
    public void testWithOneDownOneUp() throws Exception {
        Availability a1 = new Availability(12, DOWN);
        Availability a2 = new Availability(18, UP);
        AvailabilityBucketDataPoint actual = mapper.newPointInstance(10, 20, ImmutableList.of(a1, a2));
        AvailabilityBucketDataPoint expected = new AvailabilityBucketDataPoint.Builder(10, 20)
                .setDowntimeCount(1)
                .setDowntimeDuration(8)
                .setLastDowntime(18)
                .setUptimeRatio(0.2)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketDataPoint(expected));
    }

    @Test
    public void testWithOneUpOneDown() throws Exception {
        Availability a1 = new Availability(13, UP);
        Availability a2 = new Availability(17, DOWN);
        AvailabilityBucketDataPoint actual = mapper.newPointInstance(10, 20, ImmutableList.of(a1, a2));
        AvailabilityBucketDataPoint expected = new AvailabilityBucketDataPoint.Builder(10, 20)
                .setDowntimeCount(1)
                .setDowntimeDuration(3)
                .setLastDowntime(20)
                .setUptimeRatio(0.7)
                .build();
        assertFalse("Expected non empty instance", actual.isEmpty());
        assertThat(actual, matchesAvailabilityBucketDataPoint(expected));
    }
}