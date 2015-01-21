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
package org.rhq.metrics.impl.cassandra;

import static java.util.stream.Collectors.toSet;

import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.rhq.metrics.core.Availability;
import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.Tag;

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
        META_DATA,
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
        metric.addData(new Availability(metric, firstRow.getUUID(ColumnIndex.TIME.ordinal()), firstRow.getBytes(
            ColumnIndex.AVAILABILITY.ordinal()), getTags(firstRow)));

        for (Row row : resultSet) {
            metric.addData(new Availability(metric, row.getUUID(ColumnIndex.TIME.ordinal()), row.getBytes(
                ColumnIndex.AVAILABILITY.ordinal()), getTags(row)));
        }

        return metric;
    }

    private AvailabilityMetric getMetric(Row row) {
        AvailabilityMetric metric = new AvailabilityMetric(row.getString(ColumnIndex.TENANT_ID.ordinal()), getId(row),
            row.getMap(ColumnIndex.META_DATA.ordinal(), String.class, String.class), row.getInt(
            ColumnIndex.DATA_RETENTION.ordinal()));
        metric.setDpart(row.getLong(ColumnIndex.DPART.ordinal()));

        return metric;
    }

    private MetricId getId(Row row) {
        return new MetricId(row.getString(ColumnIndex.METRIC_NAME.ordinal()), Interval.parse(row.getString(
            ColumnIndex.INTERVAL.ordinal())));
    }

    private Set<Tag> getTags(Row row) {
        Map<String, String> map = row.getMap(ColumnIndex.TAGS.ordinal(), String.class, String.class);
        return map.entrySet().stream().map(entry -> new Tag(entry.getKey(), entry.getValue())).collect(toSet());
    }
}
