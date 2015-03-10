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

import java.util.Map;

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
public class AvailabilityMetricMapper implements Function<ResultSet, AvailabilityMetric> {

    private enum ColumnIndex {
        TENANT_ID,
        METRIC_NAME,
        INTERVAL,
        DPART,
        TIME,
        METRIC_TAGS,
        DATA_RETENTION,
        AVAILABILITY,
        TAGS
    }

    @Override
    public AvailabilityMetric apply(ResultSet resultSet) {
        if (resultSet.isExhausted()) {
            return null;
        }
        Row firstRow = resultSet.one();
        AvailabilityMetric metric = getMetric(firstRow);
        metric.addData(new Availability(firstRow.getUUID(ColumnIndex.TIME.ordinal()), firstRow
                .getBytes(ColumnIndex.AVAILABILITY.ordinal()), getTags(firstRow)));

        for (Row row : resultSet) {
            metric.addData(new Availability(row.getUUID(ColumnIndex.TIME.ordinal()), row
                    .getBytes(ColumnIndex.AVAILABILITY.ordinal()), getTags(row)));
        }

        return metric;
    }

    private AvailabilityMetric getMetric(Row row) {
        AvailabilityMetric metric = new AvailabilityMetric(row.getString(ColumnIndex.TENANT_ID.ordinal()), getId(row),
                row.getMap(ColumnIndex.METRIC_TAGS.ordinal(), String.class, String.class),
            row.getInt(ColumnIndex.DATA_RETENTION.ordinal()));
        metric.setDpart(row.getLong(ColumnIndex.DPART.ordinal()));

        return metric;
    }

    private MetricId getId(Row row) {
        return new MetricId(row.getString(ColumnIndex.METRIC_NAME.ordinal()), Interval.parse(row.getString(
            ColumnIndex.INTERVAL.ordinal())));
    }

    private Map<String, String> getTags(Row row) {
        return row.getMap(ColumnIndex.TAGS.ordinal(), String.class, String.class);
    }
}
