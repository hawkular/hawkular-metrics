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

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author jsanda
 */
public class TaggedBucketPoint {

    private Map<String, String> tags;
    private final double min;
    private final double avg;
    private final double median;
    private final double max;
    private final double sum;
    private int samples;
    private final List<Percentile> percentiles;

    public TaggedBucketPoint(Map<String, String> tags, double min, double avg, double median, double max, double sum,
            int samples, List<Percentile> percentiles) {
        this.tags = tags;
        this.min = min;
        this.avg = avg;
        this.median = median;
        this.max = max;
        this.sum = sum;
        this.samples = samples;
        this.percentiles = percentiles;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public double getMin() {
        return min;
    }

    public double getAvg() {
        return avg;
    }

    public double getMedian() {
        return median;
    }

    public double getMax() {
        return max;
    }

    public double getSum() {
        return sum;
    }

    public int getSamples() {
        return samples;
    }

    public List<Percentile> getPercentiles() {
        return percentiles;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaggedBucketPoint that = (TaggedBucketPoint) o;
        return Objects.equals(min, that.min) &&
                Objects.equals(avg, that.avg) &&
                Objects.equals(median, that.median) &&
                Objects.equals(max, that.max) &&
                Objects.equals(sum, that.sum) &&
                Objects.equals(samples, that.samples) &&
                Objects.equals(tags, that.tags) &&
                Objects.equals(percentiles, that.percentiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tags, min, avg, median, max, sum, samples, percentiles);
    }

    @Override public String toString() {
        return "TaggedBucketPoint{" +
                "tags=" + tags +
                ", min=" + min +
                ", avg=" + avg +
                ", median=" + median +
                ", max=" + max +
                ", sum=" + sum +
                ", samples=" + samples +
                ", percentiles=" + percentiles +
                '}';
    }
}
