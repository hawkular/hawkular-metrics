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

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.hawkular.metrics.core.api.AvailabilityBucketDataPoint;

import com.google.common.math.DoubleMath;

/**
 * @author Thomas Segismont
 */
public class AvailabilityBucketDataPointMatcher extends TypeSafeMatcher<AvailabilityBucketDataPoint> {
    private final AvailabilityBucketDataPoint expected;

    public AvailabilityBucketDataPointMatcher(AvailabilityBucketDataPoint expected) {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(AvailabilityBucketDataPoint item) {
        return item.getStart() == expected.getStart()
               && item.getEnd() == expected.getEnd()
               && item.getDowntimeCount() == expected.getDowntimeCount()
               && item.getDowntimeDuration() == expected.getDowntimeDuration()
               && item.getLastDowntime() == expected.getLastDowntime()
               && DoubleMath.fuzzyEquals(item.getUptimeRatio(), expected.getUptimeRatio(), 0.001);
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue(expected);
    }

    @Factory
    public static Matcher<AvailabilityBucketDataPoint> matchesAvailabilityBucketDataPoint(
            AvailabilityBucketDataPoint expected
    ) {
        return new AvailabilityBucketDataPointMatcher(expected);
    }
}
