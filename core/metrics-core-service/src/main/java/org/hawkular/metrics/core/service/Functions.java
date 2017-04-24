/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.service;

import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;

import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.Tenant;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Function;

import rx.Observable;

/**
 * @author jsanda
 */
public class Functions {

    private enum GAUGE_COLS {
        TIME,
        VALUE,
        TAGS
    }

    private enum COUNTER_COLS {
        TIME,
        VALUE,
        TAGS
    }

    private enum AVAILABILITY_COLS {
        TIME,
        AVAILABILITY,
        TAGS
    }

    private enum STRING_COLS {
        TIME,
        VALUE,
        TAGS
    }

    private Functions() {
    }

    public static final Function<List<ResultSet>, Void> TO_VOID = resultSets -> null;

    public static DataPoint<Double> getGaugeDataPoint(Row row) {
        return new DataPoint<>(
                UUIDs.unixTimestamp(row.getUUID(GAUGE_COLS.TIME.ordinal())),
                row.getDouble(GAUGE_COLS.VALUE.ordinal()),
                row.getMap(GAUGE_COLS.TAGS.ordinal(), String.class, String.class));
    }

    public static DataPoint<Double> getTempGaugeDataPoint(Row row) {
        return new DataPoint<>(
                row.getTimestamp(GAUGE_COLS.TIME.ordinal()).toInstant().toEpochMilli(),
                row.getDouble(GAUGE_COLS.VALUE.ordinal()),
                row.getMap(GAUGE_COLS.TAGS.ordinal(), String.class, String.class));
    }

    public static DataPoint<Long> getCounterDataPoint(Row row) {
        return new DataPoint<>(
                UUIDs.unixTimestamp(row.getUUID(COUNTER_COLS.TIME.ordinal())),
                row.getLong(COUNTER_COLS.VALUE.ordinal()),
                row.getMap(COUNTER_COLS.TAGS.ordinal(), String.class, String.class));
    }

    public static DataPoint<Long> getTempCounterDataPoint(Row row) {
        return new DataPoint<>(
                row.getTimestamp(COUNTER_COLS.TIME.ordinal()).toInstant().toEpochMilli(),
                row.getLong(COUNTER_COLS.VALUE.ordinal()),
                row.getMap(COUNTER_COLS.TAGS.ordinal(), String.class, String.class));
    }

    public static DataPoint<String> getStringDataPoint(Row row) {
        return new DataPoint<>(
                UUIDs.unixTimestamp(row.getUUID(STRING_COLS.TIME.ordinal())),
                row.getString(STRING_COLS.VALUE.ordinal()),
                row.getMap(STRING_COLS.TAGS.ordinal(), String.class, String.class));
    }

    public static DataPoint<AvailabilityType> getAvailabilityDataPoint(Row row) {
        return new DataPoint<>(
                UUIDs.unixTimestamp(row.getUUID(AVAILABILITY_COLS.TIME.ordinal())),
                AvailabilityType.fromBytes(row.getBytes(AVAILABILITY_COLS.AVAILABILITY.ordinal())),
                row.getMap(COUNTER_COLS.TAGS.ordinal(), String.class, String.class));
    }

    public static DataPoint<AvailabilityType> getTempAvailabilityDataPoint(Row row) {
        return new DataPoint<>(
                row.getTimestamp(AVAILABILITY_COLS.TIME.ordinal()).toInstant().toEpochMilli(),
                AvailabilityType.fromBytes(row.getBytes(AVAILABILITY_COLS.AVAILABILITY.ordinal())),
                row.getMap(COUNTER_COLS.TAGS.ordinal(), String.class, String.class));
    }

    public static Tenant getTenant(Row row) {
        String tenantId = row.getString(0);
        Map<MetricType<?>, Integer> retentions = row.getMap(1, String.class, Integer.class).entrySet().stream()
                .collect(toMap(entry -> MetricType.fromTextCode(entry.getKey()), Map.Entry::getValue));

        return new Tenant(tenantId, retentions);
    }

    public static <S> Observable<Metric<S>> metricToObservable(
            String tenantId, List<Metric<S>> metrics, MetricType<S> type) {
        return Observable.from(metrics).map(g -> {
            return new Metric<>(new MetricId<>(tenantId, type, g.getMetricId().getName()), g.getDataPoints());
        });
    }

    public static <T> Observable<Metric<T>> dataPointToObservable(String tenantId, String metricId,
            List<DataPoint<T>> points, MetricType<T> type) {
        Metric<T> metric = new Metric<T>(new MetricId<>(tenantId, type, metricId), points);
        return Observable.just(metric);
    }

    /**
     * Makes the string safe by prepending it with a reserved character that is not allowed
     * in user-defined names for things such as tenant names, metric names, and tag names.
     *
     * @param s The string to wrap.*
     * @return The string prepended with a $
     */
    public static String makeSafe(String s) {
        return "$" + s;
    }

    public static boolean isValidTagMap(Map<String, String> map) {
        for (String value : map.keySet()) {
            if (value == null || value.trim().isEmpty()) {
                return false;
            }
        }

        return true;
    }

}
