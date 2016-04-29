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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import static org.hawkular.metrics.core.service.PatternUtil.filterPattern;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.TaggedBucketPoint;

import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func1;

/**
 * @author Thomas Segismont
 */
public class TaggedBucketPointTransformer
        implements Transformer<DataPoint<? extends Number>, Map<String, TaggedBucketPoint>> {

    private final Map<String, String> tags;
    private final List<Double> percentiles;

    public TaggedBucketPointTransformer(Map<String, String> tags, List<Double> percentiles) {
        this.tags = tags;
        this.percentiles = percentiles;
    }

    @Override
    public Observable<Map<String, TaggedBucketPoint>> call(Observable<DataPoint<? extends Number>> dataPoints) {
        List<Func1<DataPoint<? extends Number>, Boolean>> tagFilters = tags.entrySet().stream().map(e -> {
            boolean positive = (!e.getValue().startsWith("!"));
            Pattern pattern = filterPattern(e.getValue());
            Func1<DataPoint<? extends Number>, Boolean> filter = dataPoint ->
                    dataPoint.getTags().containsKey(e.getKey()) &&
                            (positive == pattern.matcher(dataPoint.getTags().get(e.getKey())).matches());
            return filter;
        }).collect(toList());

        // TODO refactor this to be more functional and replace java 8 streams with rx operators
        return dataPoints
                .filter(dataPoint -> {
                    for (Func1<DataPoint<? extends Number>, Boolean> tagFilter : tagFilters) {
                        if (!tagFilter.call(dataPoint)) {
                            return false;
                        }
                    }
                    return true;
                })
                .groupBy(dataPoint -> tags.entrySet().stream().collect(
                        toMap(Map.Entry::getKey, e -> dataPoint.getTags().get(e.getKey()))))
                .flatMap(group -> group.collect(() -> new TaggedDataPointCollector(group.getKey(), percentiles),
                        TaggedDataPointCollector::increment))
                .map(TaggedDataPointCollector::toBucketPoint)
                .toMap(bucketPoint -> bucketPoint.getTags().entrySet().stream().map(e ->
                        e.getKey() + ":" + e.getValue()).collect(joining(",")));
    }
}
