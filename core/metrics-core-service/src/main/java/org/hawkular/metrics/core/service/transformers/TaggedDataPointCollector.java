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

import static org.hawkular.metrics.core.service.transformers.NumericDataPointCollector.createPercentile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.hawkular.metrics.core.service.PercentileWrapper;
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
    private List<PercentileWrapper> percentiles;
    private List<Percentile> percentileList;

    public TaggedDataPointCollector(Map<String, String> tags, List<Percentile> percentilesList) {
        this.tags = tags;
        this.percentiles = new ArrayList<>(percentilesList.size() + 1);
        this.percentileList = percentilesList;
        percentilesList.stream().forEach(d -> percentiles.add(createPercentile.apply(d.getQuantile())));
        percentiles.add(createPercentile.apply(50.0)); // Important to be the last one
    }

    public void increment(DataPoint<? extends Number> dataPoint) {
        Number value = dataPoint.getValue();
        min.increment(value.doubleValue());
        average.increment(value.doubleValue());
        max.increment(value.doubleValue());
        sum.increment(value.doubleValue());
        samples++;
        percentiles.stream().forEach(p -> p.addValue(value.doubleValue()));
    }

    public TaggedBucketPoint toBucketPoint() {

        List<Percentile> percentileReturns = new ArrayList<>(percentileList.size());

        if(percentileList.size() > 0) {
            for(int i = 0; i < percentileList.size(); i++) {
                Percentile p = percentileList.get(i);
                PercentileWrapper pw = percentiles.get(i);
                percentileReturns.add(new Percentile(p.getOriginalQuantile(), pw.getResult()));
            }
        }

        return new TaggedBucketPoint(tags, min.getResult(), average.getResult(),
                this.percentiles.get(this.percentiles.size() - 1).getResult(), max.getResult(), sum.getResult(),
                samples, percentileReturns);
    }

}
