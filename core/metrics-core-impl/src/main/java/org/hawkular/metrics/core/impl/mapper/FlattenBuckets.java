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
package org.hawkular.metrics.core.impl.mapper;

import static java.lang.Double.NaN;
import gnu.trove.map.TLongObjectMap;

import java.util.List;

import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;

import com.google.common.base.Function;

public class FlattenBuckets implements Function<List<? extends Object>, BucketedOutput> {

    private int numberOfBuckets;
    private boolean skipEmpty;
    private int bucketWidthSeconds;

    public FlattenBuckets(int numberOfBuckets, int bucketWidthSeconds, boolean skipEmpty) {
        this.numberOfBuckets = numberOfBuckets;
        this.bucketWidthSeconds = bucketWidthSeconds;
        this.skipEmpty = skipEmpty;
    }

    @Override
    public BucketedOutput apply(List<? extends Object> args) {
        // Now that stuff is in buckets - we need to "flatten" them out.
        // As we collapse stuff from a lot of input timestamps into some
        // buckets, we only use a relative time for the bucket timestamps.
        NumericMetric metric = (NumericMetric) args.get(0);
        TLongObjectMap<List<NumericData>> buckets = (TLongObjectMap<List<NumericData>>) args.get(1);
        BucketedOutput output = new BucketedOutput(metric.getTenantId(), metric.getId().getName(), metric.getTags());
        for (int i = 0; i < numberOfBuckets; ++i) {
            List<NumericData> tmpList = buckets.get(i);
            if (tmpList == null) {
                if (!skipEmpty) {
                    output.add(new BucketDataPoint(metric.getId().getName(), 1000 * i * bucketWidthSeconds, NaN,
                        NaN, NaN));
                }
            } else {
                output.add(getBucketDataPoint(tmpList.get(0).getMetric().getId().getName(),
                    1000L * i * bucketWidthSeconds, tmpList));
            }
        }
        return output;
    }

    private static BucketDataPoint getBucketDataPoint(String id, long startTime, List<NumericData> bucketMetrics) {
        Double min = null;
        Double max = null;
        double sum = 0;
        for (NumericData raw : bucketMetrics) {
            if (max==null || raw.getValue() > max) {
                max = raw.getValue();
            }
            if (min==null || raw.getValue() < min) {
                min = raw.getValue();
            }
            sum += raw.getValue();
        }
        double avg = bucketMetrics.size()>0 ? sum / bucketMetrics.size() : NaN;
        if (min == null) {
            min = NaN;
        }
        if (max == null) {
            max = NaN;
        }
        BucketDataPoint result = new BucketDataPoint(id,startTime,min, avg,max);

        return result;
    }
}