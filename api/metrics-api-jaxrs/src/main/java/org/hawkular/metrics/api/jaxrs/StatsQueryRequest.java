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
package org.hawkular.metrics.api.jaxrs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;

/**
 * @author jsanda
 */
public class StatsQueryRequest {

    private Map<String, List<String>> metrics = new HashMap<>();

    private String start;

    private String end;

    private Integer buckets;

    private String bucketDuration;

    private String percentiles;

    private String tags;

    private List<String> types = new ArrayList<>();

    public Map<String, List<String>> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, List<String>> metrics) {
        this.metrics = metrics;
    }

    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
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

    public List<String> getTypes() {
        return types;
    }

    public void setTypes(List<String> types) {
        this.types = types;
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
                Objects.equals(percentiles, that.percentiles) &&
                Objects.equals(types, that.types);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metrics, start, end, buckets, bucketDuration, percentiles, types);
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("metrics", metrics)
                .add("tags", tags)
                .add("start", start)
                .add("end", end)
                .add("buckets", buckets)
                .add("bucketDuration", bucketDuration)
                .add("percentiles", percentiles)
                .add("types", types)
                .toString();
    }
}
