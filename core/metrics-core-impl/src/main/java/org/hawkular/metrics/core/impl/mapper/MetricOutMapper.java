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

import java.util.ArrayList;
import java.util.List;

import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;
import org.hawkular.metrics.core.impl.cassandra.MetricUtils;

public class MetricOutMapper extends MetricMapper<MetricOut> {

    @Override
    public MetricOut doApply(NumericMetric metric) {
        MetricOut output = new MetricOut(metric.getTenantId(), metric.getId().getName(),
            MetricUtils.flattenTags(metric.getTags()), metric.getDataRetention());
        List<DataPointOut> dataPoints = new ArrayList<>();
        for (NumericData d : metric.getData()) {
            dataPoints.add(new DataPointOut(d.getTimestamp(), d.getValue(), MetricUtils.flattenTags(d.getTags())));
        }
        output.setData(dataPoints);

        return output;
    }
}