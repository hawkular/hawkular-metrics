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
package org.hawkular.metrics.core.impl.cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.hawkular.metrics.core.api.BucketedOutput;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.NumericBucketDataPoint;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;

import com.google.common.base.Function;

import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;

/**
 * @author Thomas Segismont
 */
public class NumericBucketedOutputMapper implements Function<NumericMetric, BucketedOutput<NumericBucketDataPoint>> {
    private final Buckets buckets;

    public NumericBucketedOutputMapper(Buckets buckets) {
        this.buckets = buckets;
    }

    @Override
    public BucketedOutput<NumericBucketDataPoint> apply(NumericMetric input) {
        if (input == null) {
            return null;
        }

        BucketedOutput<NumericBucketDataPoint> output = new BucketedOutput<>(
                input.getTenantId(), input.getId().getName(), input.getTags()
        );
        output.setData(new ArrayList<>(buckets.getCount()));

        List<NumericData> numericDataList = input.getData();
        NumericData[] numericDatas = numericDataList.toArray(new NumericData[numericDataList.size()]);
        Arrays.sort(numericDatas, NumericData.TIME_UUID_COMPARATOR);

        int dataIndex = 0;
        for (int bucketIndex = 0; bucketIndex < buckets.getCount(); bucketIndex++) {
            long from = buckets.getStart() + bucketIndex * buckets.getStep();
            long to = buckets.getStart() + (bucketIndex + 1) * buckets.getStep();

            if (dataIndex >= numericDatas.length) {
                // Reached end of data points
                output.getData().add(NumericBucketDataPoint.newEmptyInstance(from));
                continue;
            }

            NumericData current = numericDatas[dataIndex];
            if (current.getTimestamp() >= to) {
                // Current data point does not belong to this bucket
                output.getData().add(NumericBucketDataPoint.newEmptyInstance(from));
                continue;
            }

            TDoubleList valueList = new TDoubleArrayList();
            do {
                // Add current value to this bucket's summary
                valueList.add(current.getValue());

                // Move to next data point
                dataIndex++;
                current = dataIndex < numericDatas.length ? numericDatas[dataIndex] : null;

                // Continue until end of data points is reached or data point does not belong to this bucket
            } while (current != null && current.getTimestamp() < to);

            double[] values = valueList.toArray();

            Percentile percentile = new Percentile();
            percentile.setData(values);

            NumericBucketDataPoint numericBucketDataPoint = new NumericBucketDataPoint();
            numericBucketDataPoint.setTimestamp(from);
            numericBucketDataPoint.setMin(new Min().evaluate(values));
            numericBucketDataPoint.setAvg(new Mean().evaluate(values));
            numericBucketDataPoint.setMedian(percentile.evaluate(50.0));
            numericBucketDataPoint.setMax(new Max().evaluate(values));
            numericBucketDataPoint.setPercentile95th(percentile.evaluate(95.0));

            output.getData().add(numericBucketDataPoint);
        }

        return output;
    }
}
