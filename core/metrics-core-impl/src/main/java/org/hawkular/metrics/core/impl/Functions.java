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
package org.hawkular.metrics.core.impl;

import static java.util.stream.Collectors.toMap;
import static org.joda.time.DateTime.now;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Function;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Tenant;
import org.joda.time.Duration;

/**
 * @author jsanda
 */
public class Functions {

    private enum GAUGE_COLS {
        TIME,
        METRIC_TAGS,
        DATA_RETENTION,
        VALUE,
        TAGS,
        WRITE_TIME
    }

    private enum COUNTER_COLS {
        TIME,
        METRIC_TAGS,
        DATA_RETENTION,
        VALUE,
        TAGS,
        WRITE_TIME
    }

    private enum AVAILABILITY_COLS {
        TIME,
        METRIC_TAGS,
        DATA_RETENTION,
        AVAILABILITY,
        TAGS,
        WRITE_TIME
    }

    private Functions() {
    }

    public static final Function<List<ResultSet>, Void> TO_VOID = resultSets -> null;

    public static DataPoint<Double> getGaugeDataPoint(Row row) {
        return new DataPoint<>(
                UUIDs.unixTimestamp(row.getUUID(GAUGE_COLS.TIME.ordinal())),
                row.getDouble(GAUGE_COLS.VALUE.ordinal()),
                row.getMap(GAUGE_COLS.TAGS.ordinal(), String.class, String.class)
        );
    }

    public static DataPoint<Long> getCounterDataPoint(Row row) {
        return new DataPoint<>(
                UUIDs.unixTimestamp(row.getUUID(COUNTER_COLS.TIME.ordinal())),
                row.getLong(COUNTER_COLS.VALUE.ordinal()),
                row.getMap(COUNTER_COLS.TAGS.ordinal(), String.class, String.class)
        );
    }

    public static TTLDataPoint<Double> getTTLGaugeDataPoint(Row row, int originalTTL) {
        long writeTime = row.getLong(GAUGE_COLS.WRITE_TIME.ordinal()) / 1000;
        DataPoint<Double> dataPoint = getGaugeDataPoint(row);
        Duration duration = new Duration(now().minus(writeTime).getMillis());
        int newTTL = originalTTL - duration.toStandardSeconds().getSeconds();
        return new TTLDataPoint<>(dataPoint, newTTL);
    }

    public static TTLDataPoint<AvailabilityType> getTTLAvailabilityDataPoint(Row row, int originalTTL) {
        long writeTime = row.getLong(GAUGE_COLS.WRITE_TIME.ordinal()) / 1000;
        DataPoint<AvailabilityType> dataPoint = getAvailabilityDataPoint(row);
        Duration duration = new Duration(now().minus(writeTime).getMillis());
        int newTTL = originalTTL - duration.toStandardSeconds().getSeconds();
        return new TTLDataPoint<>(dataPoint, newTTL);
    }

    public static DataPoint<AvailabilityType> getAvailabilityDataPoint(Row row) {
        return new DataPoint<>(
                UUIDs.unixTimestamp(row.getUUID(AVAILABILITY_COLS.TIME.ordinal())),
                AvailabilityType.fromBytes(row.getBytes(AVAILABILITY_COLS.AVAILABILITY.ordinal())),
                row.getMap(AVAILABILITY_COLS.TAGS.ordinal(), String.class, String.class)
        );
    }

    public static Tenant getTenant(Row row) {
        String tenantId = row.getString(0);
        Map<MetricType, Integer> retentions = row.getMap(1, String.class, Integer.class).entrySet().stream().collect(
                toMap(entry -> MetricType.fromTextCode(entry.getKey()), Map.Entry::getValue));

        return new Tenant(tenantId, retentions);
    }

}
