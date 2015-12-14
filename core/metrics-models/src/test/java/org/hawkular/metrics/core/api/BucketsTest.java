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
package org.hawkular.metrics.core.api;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author Thomas Segismont
 */
public class BucketsTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testFromCount() throws Exception {
        // Try with (count * step) == end - start
        assertEquals(new Buckets(5, 1, 5), Buckets.fromCount(5, 10, 5));
        // Try with (count * step) < end - start
        assertEquals(new Buckets(5, 1, 4), Buckets.fromCount(5, 10, 4));
        // Try with (count * step) > end - start
        assertEquals(new Buckets(14, 9, 3), Buckets.fromCount(14, 40, 3));
    }

    @Test
    public void testComputedStepIsEqualToZero() {
        expectedException.expect(instanceOf(IllegalArgumentException.class));
        expectedException.expectMessage(equalTo("Computed step is equal to zero"));

        Buckets.fromCount(28, 37, (37 - 28) + 1);
    }

    @Test
    public void testFromStep() throws Exception {
        // Try with step > end - start
        assertEquals(new Buckets(4, 50, 1), Buckets.fromStep(4, 43, 50));
        // Try with (count * step) == end - start
        assertEquals(new Buckets(11, 47, 2734), Buckets.fromStep(11, 11 + 47 * 2734, 47));
        // Try with (count * step) > end - start
        assertEquals(new Buckets(5, 7, 2), Buckets.fromStep(5, 13, 7));
    }

    @Test
    public void testComputedNumberOfBucketsIsTooBig() {
        expectedException.expect(instanceOf(IllegalArgumentException.class));
        expectedException.expectMessage(equalTo("Computed number of buckets is too big: " + Long.MAX_VALUE));

        Buckets.fromStep(0, Long.MAX_VALUE, 1);
    }
}