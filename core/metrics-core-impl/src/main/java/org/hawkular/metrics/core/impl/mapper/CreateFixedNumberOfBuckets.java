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

import static java.util.Arrays.asList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.List;

import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;

public class CreateFixedNumberOfBuckets extends MetricMapper<List<? extends Object>> {

    private int numberOfBuckets;
    private int bucketWidthSeconds;

    public CreateFixedNumberOfBuckets(int numberOfBuckets, int bucketWidthSeconds) {
        this.numberOfBuckets = numberOfBuckets;
        this.bucketWidthSeconds = bucketWidthSeconds;
    }

    @Override
    public List<? extends Object> doApply(NumericMetric metric) {
        long totalLength = (long) numberOfBuckets * bucketWidthSeconds * 1000L;
        long minTs = Long.MAX_VALUE;
        for (NumericData d : metric.getData()) {
            if (d.getTimestamp() < minTs) {
                minTs = d.getTimestamp();
            }
        }
        TLongObjectMap<List<NumericData>> buckets = new TLongObjectHashMap<>(numberOfBuckets);
        for (NumericData d : metric.getData()) {
            long bucket = d.getTimestamp() - minTs;
            bucket = bucket % totalLength;
            bucket = bucket / (bucketWidthSeconds * 1000L);
            List<NumericData> tmpList = buckets.get(bucket);
            if (tmpList == null) {
                tmpList = new ArrayList<>();
                buckets.put(bucket, tmpList);
            }
            tmpList.add(d);
        }
        return asList(metric, buckets);
    }
}