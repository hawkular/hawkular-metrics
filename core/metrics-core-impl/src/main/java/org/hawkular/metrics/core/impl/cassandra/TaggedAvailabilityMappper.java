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

import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityMetric;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.MetricId;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

/**
 * @author John Sanda
 */
public class TaggedAvailabilityMappper implements Function<ResultSet, Map<MetricId, Set<Availability>>> {

    @Override
    public Map<MetricId, Set<Availability>> apply(ResultSet resultSet) {
        Map<MetricId, Set<Availability>> taggedData = new HashMap<>();
        AvailabilityMetric metric = null;
        LinkedHashSet<Availability> set = new LinkedHashSet<>();
        for (Row row : resultSet) {
            if (metric == null) {
                metric = createMetric(row);
                set.add(createAvailability(row));
            } else {
                AvailabilityMetric nextMetric = createMetric(row);
                if (metric.equals(nextMetric)) {
                    set.add(createAvailability(row));
                } else {
                    taggedData.put(metric.getId(), set);
                    metric = nextMetric;
                    set = new LinkedHashSet<>();
                    set.add(createAvailability(row));
                }
            }
        }
        if (!(metric == null || set.isEmpty())) {
            taggedData.put(metric.getId(), set);
        }
        return taggedData;
    }

    private AvailabilityMetric createMetric(Row row) {
        return new AvailabilityMetric(row.getString(0), new MetricId(row.getString(4),
            Interval.parse(row.getString(5))));
    }

    private Availability createAvailability(Row row) {
        return new Availability(row.getUUID(6), row.getBytes(7));
    }

}
