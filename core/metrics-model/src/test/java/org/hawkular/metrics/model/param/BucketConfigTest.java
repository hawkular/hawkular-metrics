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

package org.hawkular.metrics.model.param;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class BucketConfigTest {

    @Test
    public void testNoBuckets() {
        TimeRange timeRange = new TimeRange(5L, 7L);
        BucketConfig bucketConfig = new BucketConfig(null, null, timeRange);
        assertTrue(bucketConfig.getProblem(), bucketConfig.isValid());
        assertNull(bucketConfig.getBuckets());
    }

    @Test
    public void testCreateFromBucketCount() {
        TimeRange timeRange = new TimeRange(100L, 200L);
        BucketConfig bucketConfig = new BucketConfig(10, null, timeRange);
        assertTrue(bucketConfig.getProblem(), bucketConfig.isValid());
        assertNotNull(bucketConfig.getBuckets());
    }

    @Test
    public void testCreateFromBucketDuration() {
        TimeRange timeRange = new TimeRange(100L, 200L);
        BucketConfig bucketConfig = new BucketConfig(null, new Duration(10, MILLISECONDS), timeRange);
        assertTrue(bucketConfig.getProblem(), bucketConfig.isValid());
        assertNotNull(bucketConfig.getBuckets());
    }

    @Test
    public void testInvalidConfig() {
        TimeRange timeRange = new TimeRange(1L, Long.MAX_VALUE);
        BucketConfig bucketConfig = new BucketConfig(null, new Duration(1, MILLISECONDS), timeRange);
        assertFalse(bucketConfig.isValid());
        assertNull(bucketConfig.getBuckets());
    }

    @Test
    public void testBothParamsSpecified() {
        TimeRange timeRange = new TimeRange(5L, 7L);
        BucketConfig bucketConfig = new BucketConfig(5, new Duration(2, MILLISECONDS), timeRange);
        assertFalse(bucketConfig.isValid());
        assertNull(bucketConfig.getBuckets());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullTimeRange() {
        new BucketConfig(5, null, new TimeRange(7L, 5L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeRange() {
        new BucketConfig(5, null, new TimeRange(7L, 5L));
    }
}