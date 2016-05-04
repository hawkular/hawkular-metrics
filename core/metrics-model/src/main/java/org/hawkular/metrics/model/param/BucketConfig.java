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
package org.hawkular.metrics.model.param;

import static com.google.common.base.Preconditions.checkArgument;

import org.hawkular.metrics.model.Buckets;

import com.google.common.base.MoreObjects;

/**
 * A JAX-RS parameter object used to build bucket configurations from query params.
 *
 * @author Thomas Segismont
 */
public class BucketConfig {
    private final Buckets buckets;
    private final TimeRange timeRange;
    private final boolean valid;
    private final String problem;

    public BucketConfig(Integer bucketsCount, Duration bucketDuration, TimeRange timeRange) {
        checkArgument(timeRange != null, "Time range is null");
        this.timeRange = timeRange;

        if (!timeRange.isValid()) {
            throw new IllegalArgumentException("Invalid time range: " + timeRange.getProblem());
        }
        if (bucketsCount == null && bucketDuration == null) {
            buckets = null;
            valid = true;
            problem = null;
        } else if (bucketsCount != null && bucketDuration != null) {
            buckets = null;
            valid = false;
            problem = "Both buckets and bucketDuration parameters are specified";
        } else {
            Buckets buckets = null;
            Exception cause = null;
            try {
                if (bucketsCount != null) {
                    buckets = Buckets.fromCount(timeRange.getStart(), timeRange.getEnd(), bucketsCount);
                } else {
                    buckets = Buckets.fromStep(timeRange.getStart(), timeRange.getEnd(), bucketDuration.toMillis());
                }
            } catch (IllegalArgumentException e) {
                cause = e;
            }
            this.buckets = buckets;
            if (cause == null) {
                valid = true;
                problem = null;
            } else {
                valid = false;
                problem = cause.getMessage();
            }
        }
    }

    public Buckets getBuckets() {
        return buckets;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public boolean isValid() {
        return valid;
    }

    public boolean isEmpty() {
        return buckets == null;
    }

    public String getProblem() {
        return problem;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("buckets", buckets)
                .add("valid", valid)
                .add("problem", problem)
                .omitNullValues()
                .toString();
    }
}
