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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile;
import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.TaggedBucketPoint;

/**
 * @author jsanda
 */
public class TaggedDataPointCollector {

    // These are the tags that define this bucket.
    private Map<String, String> tags;

    private int samples = 0;
    private Min min = new Min();
    private Mean average = new Mean();
    private Max max = new Max();
    private Sum sum = new Sum();
    private PSquarePercentile median = new PSquarePercentile(50.0);
    private List<PSquarePercentile> percentiles;

    public TaggedDataPointCollector(Map<String, String> tags, List<Double> percentiles) {
        this.tags = tags;
        this.percentiles = new ArrayList<>();
        percentiles.stream().forEach(d -> this.percentiles.add(new PSquarePercentile(d)));
    }

    public void increment(DataPoint<? extends Number> dataPoint) {
        Number value = dataPoint.getValue();
        min.increment(value.doubleValue());
        average.increment(value.doubleValue());
        median.increment(value.doubleValue());
        max.increment(value.doubleValue());
        sum.increment(value.doubleValue());
        samples++;
        percentiles.stream().forEach(p -> p.increment(value.doubleValue()));
    }

    public TaggedBucketPoint toBucketPoint() {
        List<Percentile> results = percentiles.stream()
                .map(p -> new Percentile(p.quantile(), p.getResult())).collect(Collectors.toList());
        return new TaggedBucketPoint(tags, min.getResult(), average.getResult(), median.getResult(), max.getResult(),
                sum.getResult(), samples, results);
    }

}
