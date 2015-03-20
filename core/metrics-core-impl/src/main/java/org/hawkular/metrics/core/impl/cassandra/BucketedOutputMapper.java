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
import java.util.List;
import java.util.RandomAccess;

import org.hawkular.metrics.core.api.BucketedOutput;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricData;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Transform a {@link org.hawkular.metrics.core.api.Metric} with its {@link org.hawkular.metrics.core.api.MetricData}
 * into a {@link org.hawkular.metrics.core.api.BucketedOutput}.
 *
 * @param <DATA>   type of metric data, like {@link org.hawkular.metrics.core.api.NumericData}
 * @param <METRIC> type of metric, like {@link org.hawkular.metrics.core.api.NumericMetric}
 * @param <POINT>  type of bucket points, like {@link org.hawkular.metrics.core.api.NumericBucketDataPoint}
 *
 * @author Thomas Segismont
 */
public abstract class BucketedOutputMapper<DATA extends MetricData, METRIC extends Metric<DATA>, POINT>
        implements Function<METRIC, BucketedOutput<POINT>> {

    protected final Buckets buckets;

    /**
     * @param buckets the bucket configuration
     */
    public BucketedOutputMapper(Buckets buckets) {
        this.buckets = buckets;
    }

    @Override
    public BucketedOutput<POINT> apply(METRIC input) {
        if (input == null) {
            return null;
        }

        BucketedOutput<POINT> output = new BucketedOutput<>(
                input.getTenantId(), input.getId().getName(), input.getTags()
        );
        output.setData(new ArrayList<>(buckets.getCount()));

        List<DATA> dataList = input.getData();
        if (!(dataList instanceof RandomAccess)) {
            dataList = new ArrayList<>(dataList);
        }
        dataList = Lists.reverse(dataList); // We expect input data to be sorted in descending order

        int dataIndex = 0;
        DATA previous;
        for (int bucketIndex = 0; bucketIndex < buckets.getCount(); bucketIndex++) {
            long from = buckets.getStart() + bucketIndex * buckets.getStep();
            long to = buckets.getStart() + (bucketIndex + 1) * buckets.getStep();

            if (dataIndex >= dataList.size()) {
                // Reached end of data points
                output.getData().add(newEmptyPointInstance(from, to));
                continue;
            }

            DATA current = dataList.get(dataIndex);
            if (current.getTimestamp() >= to) {
                // Current data point does not belong to this bucket
                output.getData().add(newEmptyPointInstance(from, to));
                continue;
            }

            List<DATA> metricDatas = new ArrayList<>();
            do {
                // Add current value to this bucket's summary
                metricDatas.add(current);

                // Move to next data point
                previous = current;
                dataIndex++;
                current = dataIndex < dataList.size() ? dataList.get(dataIndex) : null;

                checkOrder(previous, current);

                // Continue until end of data points is reached or data point does not belong to this bucket
            } while (current != null && current.getTimestamp() < to);

            output.getData().add(newPointInstance(from, to, metricDatas));
        }

        return output;
    }

    private void checkOrder(DATA previous, DATA current) {
        if (previous != null && current != null && MetricData.TIME_UUID_COMPARATOR.compare(previous, current) > 0) {
            throw new IllegalArgumentException("Expected to iterate over data sorted by time ascending");
        }
    }

    /**
     * Create an empty bucket data point instance.
     *
     * @param from        start timestamp of the bucket
     * @param to          end timestamp of the bucket
     *
     * @return an empty bucket data point
     */
    protected abstract POINT newEmptyPointInstance(long from, long to);

    /**
     * Create a bucket data point from the metric data in this bucket.
     *
     * @param from        start timestamp of the bucket
     * @param to          end timestamp of the bucket
     * @param metricDatas metric data in this bucket, ordered by {@link MetricData#TIME_UUID_COMPARATOR}
     *
     * @return a bucket data point summurazing the metric data
     */
    protected abstract POINT newPointInstance(long from, long to, List<DATA> metricDatas);
}
