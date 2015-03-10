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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

/**
 * @author John Sanda
 */
public class TaggedNumericDataMapper implements Function<ResultSet, Map<MetricId, Set<NumericData>>> {

    @Override
    public Map<MetricId, Set<NumericData>> apply(ResultSet resultSet) {
        Map<MetricId, Set<NumericData>> taggedData = new HashMap<>();
        NumericMetric metric = null;
        LinkedHashSet<NumericData> set = new LinkedHashSet<>();
        for (Row row : resultSet) {
            if (metric == null) {
                metric = createMetric(row);
                set.add(createNumericData(row));
            } else {
                NumericMetric nextMetric = createMetric(row);
                if (metric.equals(nextMetric)) {
                    set.add(createNumericData(row));
                } else {
                    taggedData.put(metric.getId(), set);
                    metric = nextMetric;
                    set = new LinkedHashSet<>();
                    set.add(createNumericData(row));
                }
            }
        }
        if (!(metric == null || set.isEmpty())) {
            taggedData.put(metric.getId(), set);
        }
        return taggedData;
    }

    private NumericMetric createMetric(Row row) {
        return new NumericMetric(row.getString(0), new MetricId(row.getString(4), Interval.parse(row.getString(5))));
    }

    private NumericData createNumericData(Row row) {
        return new NumericData(row.getUUID(6), row.getDouble(7));
    }

}
