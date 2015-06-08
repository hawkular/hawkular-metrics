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
package org.hawkular.metrics.core.impl;

import java.util.List;
import java.util.ListIterator;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.GaugeBucketDataPoint;
import org.hawkular.metrics.core.api.MetricId;

/**
 * A {@link BucketedOutputMapper} for {@link org.hawkular.metrics.core.api
 * .Gauge}.
 *
 * @author Thomas Segismont
 */
public class GaugeBucketedOutputMapper
        extends BucketedOutputMapper<Double, GaugeBucketDataPoint> {

    /**
     * @param buckets the bucket configuration
     */
    public GaugeBucketedOutputMapper(String tenantId, MetricId id, Buckets buckets) {
        super(tenantId, id, buckets, true);
    }

    @Override
    protected GaugeBucketDataPoint newEmptyPointInstance(long from, long to) {
        return new GaugeBucketDataPoint.Builder(from, to).build();
    }

    @Override
    protected GaugeBucketDataPoint newPointInstance(long from, long to, List<DataPoint<Double>> dataPoints) {
        double[] values = new double[dataPoints.size()];
        for (ListIterator<DataPoint<Double>> iterator = dataPoints.listIterator(); iterator.hasNext(); ) {
            DataPoint<Double> gaugeData = iterator.next();
            values[iterator.previousIndex()] = gaugeData.getValue();
        }

        Percentile percentile = new Percentile();
        percentile.setData(values);

        return new GaugeBucketDataPoint.Builder(from, to)
                .setMin(new Min().evaluate(values))
                .setAvg(new Mean().evaluate(values))
                .setMedian(percentile.evaluate(50.0))
                .setMax(new Max().evaluate(values))
                .setPercentile95th(percentile.evaluate(95.0))
                .build();
    }
}
