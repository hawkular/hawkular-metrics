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
import org.hawkular.metrics.core.impl.cassandra.MetricUtils;

import com.google.common.base.Function;

public class ClusterBucketData implements Function<List<? extends Object>, BucketedOutput> {

    private int numberOfBuckets;
    private int bucketWidthSeconds;

    public ClusterBucketData(int numberOfBuckets, int bucketWidthSeconds) {
        this.numberOfBuckets = numberOfBuckets;
        this.bucketWidthSeconds = bucketWidthSeconds;
    }

    @Override
    public BucketedOutput apply(List<? extends Object> args) {
        // We want to keep the raw values, but put them into clusters anyway
        // without collapsing them into a single min/avg/max tuple
        NumericMetric metric = (NumericMetric) args.get(0);
        TLongObjectMap<List<NumericData>> buckets = (TLongObjectMap<List<NumericData>>) args.get(1);
        BucketedOutput output = new BucketedOutput(metric.getTenantId(), metric.getId().getName(),
            MetricUtils.flattenTags(metric.getTags()));
        for (int i = 0; i < numberOfBuckets; ++i) {
            List<NumericData> tmpList = buckets.get(i);
            if (tmpList != null) {
                for (NumericData d : tmpList) {
                    BucketDataPoint p = new BucketDataPoint(metric.getId().getName(),
                        1000L * i * bucketWidthSeconds, NaN, d.getValue(), NaN);
                    p.setValue(d.getValue());
                    output.add(p);
                }
            }
        }
        return output;
    }
}