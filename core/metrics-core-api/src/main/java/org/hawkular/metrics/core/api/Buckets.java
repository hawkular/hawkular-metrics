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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Bucket configuration attributes. The configuration determines:
 * <ul>
 *     <li>the first point in time ({@link #getStart()})</li>
 *     <li>the bucket size ({@link #getStep()})</li>
 *     <li>the number of buckets ({@link #getCount()})</li>
 * </ul>
 * <p>
 * Instances can be created with the constructor {@link #Buckets(long, long, int)} or with one of the factory methods:
 * <ul>
 *     <li>{@link #fromStep(long, long, long)}</li>
 *     <li>{@link #fromCount(long, long, int)}</li>
 * </ul>
 * The former forces the size of buckets, the latter the number of buckets. The former is preferred has it guarantees
 * that the last bucket will always include the {@code end} value.
 *
 * @author Thomas Segismont
 */
public final class Buckets {
    private final long start;
    private final long step;
    private final int count;

    public Buckets(long start, long step, int count) {
        checkArgument(start > 0, "start is not positive");
        this.start = start;
        checkArgument(step > 0, "step is not positive");
        this.step = step;
        checkArgument(count > 0, "count is not positive");
        this.count = count;
    }

    public long getStart() {
        return start;
    }

    public long getStep() {
        return step;
    }

    public int getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Buckets buckets = (Buckets) o;
        return count == buckets.count && start == buckets.start && step == buckets.step;

    }

    @Override
    public int hashCode() {
        int result = (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (step ^ (step >>> 32));
        result = 31 * result + count;
        return result;
    }

    @Override
    public String toString() {
        return "Buckets[" +
               "start=" + start +
               ", step=" + step +
               ", count=" + count +
               ']';
    }

    /**
     * Force bucket count. This method does not guarantee that the last bucket includes the {@code end} value.
     *
     * @param start start time
     * @param end   end time
     * @param count desired number of buckets
     *
     * @return an instance of {@link Buckets}, starting at {@code start}, separated by {@code count} intervals
     */
    public static Buckets fromCount(long start, long end, int count) {
        checkTimeRange(start, end);
        checkArgument(count > 0, "count is not positive: %s", count);
        long quotient = (end - start) / count;
        long remainder = (end - start) % count;
        long step;
        // count * quotient + remainder = end - start
        // If the remainder is greater than zero, we should try with (quotient + 1), provided that this greater step
        // does not make the number of buckets smaller than the expected count
        if (remainder != 0 && (count - 1) * (quotient + 1) < (end - start)) {
            step = quotient + 1;
        } else {
            step = quotient;
        }
        checkArgument(step > 0, "Computed step is equal to zero");
        return new Buckets(start, step, count);
    }

    /**
     * Force bucket step.
     *
     * @param start start time
     * @param end   end time
     * @param step  desired step
     *
     * @return an instance of {@link Buckets}, starting at {@code start}, separated by intervals of size {@code step}
     */
    public static Buckets fromStep(long start, long end, long step) {
        checkTimeRange(start, end);
        checkArgument(step > 0, "step is not positive: %s", step);
        if (step > (end - start)) {
            return new Buckets(start, step, 1);
        }
        long quotient = (end - start) / step;
        long remainder = (end - start) % step;
        long count;
        if (remainder == 0) {
            count = quotient;
        } else {
            count = quotient + 1;
        }
        checkArgument(count <= Integer.MAX_VALUE, "Computed number of buckets is too big: %s", count);
        return new Buckets(start, step, (int) count);
    }

    private static void checkTimeRange(long start, long end) {
        checkArgument(end > start, "Start is higher than end: %s, %s", start, end);
    }
}
