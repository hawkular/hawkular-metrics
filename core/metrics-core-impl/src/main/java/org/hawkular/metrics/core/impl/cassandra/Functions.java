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

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.hawkular.metrics.core.api.AggregationTemplate;
import org.hawkular.metrics.core.api.AvailabilityData;
import org.hawkular.metrics.core.api.GaugeData;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Tenant;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.google.common.base.Function;

/**
 * @author jsanda
 */
public class Functions {

    private static enum GAUGE_COLS {
        TIME,
        METRIC_TAGS,
        DATA_RETENTION,
        VALUE,
        TAGS,
        WRITE_TIME
    }

    private static enum AVAILABILITY_COLS {
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

    public static final Function<ResultSet, List<GaugeData>> MAP_GAUGE_DATA = resultSet ->
            StreamSupport.stream(resultSet.spliterator(), false).map(Functions::getGaugeData).collect(toList());

    public static GaugeData getGaugeData(Row row) {
        return new GaugeData(
                row.getUUID(GAUGE_COLS.TIME.ordinal()), row.getDouble(GAUGE_COLS.VALUE.ordinal()),
                row.getMap(GAUGE_COLS.TAGS.ordinal(), String.class, String.class)
        );
    }

    public static final Function<ResultSet, List<GaugeData>> MAP_GAUGE_DATA_WITH_WRITE_TIME = resultSet ->
        StreamSupport.stream(resultSet.spliterator(), false).map(Functions::getGaugeDataAndWriteTime)
                .collect(toList());

    public static GaugeData getGaugeDataAndWriteTime(Row row) {
        return new GaugeData(
                row.getUUID(GAUGE_COLS.TIME.ordinal()),
                row.getDouble(GAUGE_COLS.VALUE.ordinal()),
                row.getMap(GAUGE_COLS.TAGS.ordinal(), String.class, String.class),
                row.getLong(GAUGE_COLS.WRITE_TIME.ordinal()) / 1000);
    }

    public static final Function<ResultSet, List<AvailabilityData>> MAP_AVAILABILITY_DATA = resultSet ->
            StreamSupport.stream(resultSet.spliterator(), false).map(Functions::getAvailability).collect(toList());

    private static AvailabilityData getAvailability(Row row) {
        return new AvailabilityData(
                row.getUUID(AVAILABILITY_COLS.TIME.ordinal()), row.getBytes(AVAILABILITY_COLS.AVAILABILITY.ordinal()),
                row.getMap(AVAILABILITY_COLS.TAGS.ordinal(), String.class, String.class));
    }

    public static final Function<ResultSet, List<AvailabilityData>> MAP_AVAILABILITY_WITH_WRITE_TIME = resultSet ->
            StreamSupport.stream(resultSet.spliterator(), false).map(Functions::getAvailabilityAndWriteTime)
                    .collect(toList());

    public static AvailabilityData getAvailabilityAndWriteTime(Row row) {
        return new AvailabilityData(
                row.getUUID(AVAILABILITY_COLS.TIME.ordinal()),
                row.getBytes(AVAILABILITY_COLS.AVAILABILITY.ordinal()),
                row.getMap(AVAILABILITY_COLS.TAGS.ordinal(), String.class, String.class),
                row.getLong(AVAILABILITY_COLS.WRITE_TIME.ordinal()) / 1000
        );
    }

    public static Tenant getTenant(Row row) {
        Tenant tenant = new Tenant().setId(row.getString(0));
        Map<TupleValue, Integer> retentions = row.getMap(1, TupleValue.class, Integer.class);
        for (Map.Entry<TupleValue, Integer> entry : retentions.entrySet()) {
            MetricType metricType = MetricType.fromCode(entry.getKey().getInt(0));
            if (entry.getKey().isNull(1)) {
                tenant.setRetention(metricType, entry.getValue());
            } else {
                Interval interval = Interval.parse(entry.getKey().getString(1));
                tenant.setRetention(metricType, interval, entry.getValue());
            }
        }

        List<UDTValue> templateValues = row.getList(2, UDTValue.class);
        for (UDTValue value : templateValues) {
            tenant.addAggregationTemplate(new AggregationTemplate()
                    .setType(MetricType.fromCode(value.getInt("type")))
                    .setInterval(Interval.parse(value.getString("interval")))
                    .setFunctions(value.getSet("fns", String.class)));
        }

        return tenant;
    }

}
