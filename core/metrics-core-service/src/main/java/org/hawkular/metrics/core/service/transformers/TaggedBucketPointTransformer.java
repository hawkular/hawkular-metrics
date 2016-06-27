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

package org.hawkular.metrics.core.service.transformers;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import static org.hawkular.metrics.core.service.PatternUtil.filterPattern;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.TaggedBucketPoint;

import rx.Observable;
import rx.Observable.Transformer;

/**
 * @author Thomas Segismont
 */
public class TaggedBucketPointTransformer
        implements Transformer<DataPoint<? extends Number>, Map<String, TaggedBucketPoint>> {

    private final Map<String, String> tags;
    private final List<Percentile> percentiles;

    public TaggedBucketPointTransformer(Map<String, String> tags, List<Percentile> percentiles) {
        this.tags = tags;
        this.percentiles = percentiles;
    }

    @Override
    public Observable<Map<String, TaggedBucketPoint>> call(Observable<DataPoint<? extends Number>> dataPoints) {
        Predicate<DataPoint<? extends Number>> filter = dataPoint -> true;
        for (Entry<String, String> entry : tags.entrySet()) {
            boolean positive = (!entry.getValue().startsWith("!"));
            Pattern pattern = filterPattern(entry.getValue());
            filter = filter.and(dataPoint -> {
                return dataPoint.getTags().containsKey(entry.getKey()) &&
                        (positive == pattern.matcher(dataPoint.getTags().get(entry.getKey())).matches());
            });
        }
        return dataPoints
                .filter(filter::test)
                .groupBy(dataPoint -> tags.entrySet().stream().collect(
                        toMap(Entry::getKey, e -> dataPoint.getTags().get(e.getKey()))))
                .flatMap(group -> group.collect(() -> new TaggedDataPointCollector(group.getKey(), percentiles),
                        TaggedDataPointCollector::increment))
                .map(TaggedDataPointCollector::toBucketPoint)
                .toMap(bucketPoint -> bucketPoint.getTags().entrySet().stream().map(e ->
                        e.getKey() + ":" + e.getValue()).collect(joining(",")));
    }
}
