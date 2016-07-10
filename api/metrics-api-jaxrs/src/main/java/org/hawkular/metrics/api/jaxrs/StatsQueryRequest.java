/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.api.jaxrs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author jsanda
 */
public class StatsQueryRequest {

    private Map<String, List<String>> metrics = new HashMap<>();

    private Long start;

    private Long end;

    private Integer buckets;

    private String bucketDuration;

    private String percentiles;

    private String tags;

    public Map<String, List<String>> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, List<String>> metrics) {
        this.metrics = metrics;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    public Integer getBuckets() {
        return buckets;
    }

    public void setBuckets(Integer buckets) {
        this.buckets = buckets;
    }

    public String getBucketDuration() {
        return bucketDuration;
    }

    public void setBucketDuration(String bucketDuration) {
        this.bucketDuration = bucketDuration;
    }

    public String getPercentiles() {
        return percentiles;
    }

    public void setPercentiles(String percentiles) {
        this.percentiles = percentiles;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsQueryRequest that = (StatsQueryRequest) o;
        return Objects.equals(metrics, that.metrics) &&
                Objects.equals(tags, that.tags) &&
                Objects.equals(start, that.start) &&
                Objects.equals(end, that.end) &&
                Objects.equals(buckets, that.buckets) &&
                Objects.equals(bucketDuration, that.bucketDuration) &&
                Objects.equals(percentiles, that.percentiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metrics, start, end, buckets, bucketDuration, percentiles);
    }

    @Override public String toString() {
        return "StatsQueryRequest{" +
                "metrics=" + metrics +
                "tags=" +
                ", start=" + start +
                ", end=" + end +
                ", buckets=" + buckets +
                ", bucketDuration=" + bucketDuration +
                ", percentiles=" + percentiles +
                '}';
    }
}
