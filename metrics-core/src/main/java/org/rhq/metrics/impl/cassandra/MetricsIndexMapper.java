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

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.NumericMetric;

/**
 * @author John Sanda
 */
public class MetricsIndexMapper implements Function<ResultSet, List<Metric>> {

    private enum ColumnIndex {
        METRIC_NAME,
        INTERVAL,
        META_DATA,
        DATA_RETENTION
    }

    private String tenantId;

    private MetricType type;

    public MetricsIndexMapper(String tenantId, MetricType type) {
        if (type == MetricType.LOG_EVENT) {
            throw new IllegalArgumentException(type + " is not supported");
        }
        this.tenantId = tenantId;
        this.type = type;
    }

    @Override
    public List<Metric> apply(ResultSet resultSet) {
        if (type == MetricType.NUMERIC) {
            return getNumericMetrics(resultSet);
        } else {
            return getAvailabilityMetrics(resultSet);
        }
    }

    private List<Metric> getNumericMetrics(ResultSet resultSet) {
        List<Metric> metrics = new ArrayList<>();
        for (Row row : resultSet) {
            metrics.add(new NumericMetric(tenantId, new MetricId(row.getString(ColumnIndex.METRIC_NAME.ordinal()),
                Interval.parse(row.getString(ColumnIndex.INTERVAL.ordinal()))), row.getMap(
                ColumnIndex.META_DATA.ordinal(), String.class, String.class), row.getInt(
                ColumnIndex.DATA_RETENTION.ordinal())));
        }
        return metrics;
    }

    private List<Metric> getAvailabilityMetrics(ResultSet resultSet) {
        List<Metric> metrics = new ArrayList<>();
        for (Row row : resultSet) {
            metrics.add(new AvailabilityMetric(tenantId, new MetricId(row.getString(ColumnIndex.METRIC_NAME.ordinal()),
                Interval.parse(row.getString(ColumnIndex.INTERVAL.ordinal()))), row.getMap(
                ColumnIndex.META_DATA.ordinal(), String.class, String.class), row.getInt(
                ColumnIndex.DATA_RETENTION.ordinal())));
        }
        return metrics;
    }
}
