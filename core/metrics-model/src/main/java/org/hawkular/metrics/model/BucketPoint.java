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
package org.hawkular.metrics.model;

import static java.lang.Double.isNaN;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Statistics for metric data in a time range (bucket).
 *
 * @author Thomas Segismont
 */
public abstract class BucketPoint {
    private final long start;
    private final long end;

    protected BucketPoint(long start, long end) {
        this.start = start;
        this.end = end;
    }

    /**
     * @return start of the time range
     */
    public long getStart() {
        return start;
    }

    /**
     * @return end of the time range
     */
    public long getEnd() {
        return end;
    }

    protected Double getDoubleValue(double value) {
        if (isNaN(value)) {
            return null;
        }
        return value;
    }

    /**
     * @return returns true if there was no data to build this bucket
     */
    public abstract boolean isEmpty();

    /**
     * Converts bucket points indexed by start time into a list, ordered by start time. Blanks will be filled with
     * empty bucket points.
     */
    public static <T extends BucketPoint> List<T> toList(Map<Long, T> pointMap, Buckets buckets, BiFunction<Long,
            Long, T> emptyBucketFactory) {
        List<T> result = new ArrayList<>(buckets.getCount());
        for (int index = 0; index < buckets.getCount(); index++) {
            long from = buckets.getBucketStart(index);
            T bucketPoint = pointMap.get(from);
            if (bucketPoint == null) {
                long to = from + buckets.getStep();
                bucketPoint = emptyBucketFactory.apply(from, to);
            }
            result.add(bucketPoint);
        }
        return result;
    }
}
