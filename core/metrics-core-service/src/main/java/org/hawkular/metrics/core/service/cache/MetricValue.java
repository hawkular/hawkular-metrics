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
package org.hawkular.metrics.core.service.cache;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Percentile;

/**
 * @author jsanda
 */
public class MetricValue implements Serializable {

    private static final long serialVersionUID = 1285278451312746327L;

    private Buckets buckets;

    private int samples;

    private double min = Double.NaN;

    private double max = Double.NaN;

    private double sum;

    private PSquarePercentile median;

    private PSquarePercentile[] percentiles;

    private MetricValue() {}

    public MetricValue(Buckets buckets, List<Percentile> percentiles) {
        this.buckets = buckets;
        this.percentiles = percentiles.stream().map(p -> new PSquarePercentile(p.getQuantile()))
                .toArray(PSquarePercentile[]::new);
//        this.percentiles = new PSquarePercentile[percentiles.size()];
        median = new PSquarePercentile(50.0);
    }

    public Buckets getBuckets() {
        return buckets;
    }

    public void reset(Buckets buckets) {
        this.buckets = buckets;
        samples = 0;
        min = Double.NaN;
        max = Double.NaN;
        sum = Double.NaN;
        median.clear();
        Arrays.stream(percentiles).forEach(PSquarePercentile::clear);
    }

    public void increment(DataPoint<? extends Number> dataPoint) {
        double value = (Double) dataPoint.getValue();
        if (Double.isNaN(min)) {
            min = value;
            max = value;
        } else {
            if (value < min) {
                min = value;
            } else if (value > max) {
                max = value;
            }
        }
        sum += value;
        ++samples;
        median.increment(value);
        for (PSquarePercentile p : percentiles) {
            p.increment(value);
        }
    }
}
