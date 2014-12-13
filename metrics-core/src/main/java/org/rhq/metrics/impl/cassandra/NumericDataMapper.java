package org.rhq.metrics.impl.cassandra;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric2;
import org.rhq.metrics.core.Tag;

/**
 * @author John Sanda
 */
public class NumericDataMapper implements Function<ResultSet, List<NumericData>> {

    private enum ColumnIndex {
        TENANT_ID,
        METRIC_NAME,
        INTERVAL,
        DPART,
        TIME,
        META_DATA,
        DATA_RETENTION,
        VALUE,
        TAGS,
        WRITE_TIME
    }

    private interface RowConverter {
        NumericData getData(Row row);
    }

    private final RowConverter DEFAULT_CONVERTER = new RowConverter() {
        @Override
        public NumericData getData(Row row) {
            return new NumericData(row.getUUID(ColumnIndex.TIME.ordinal()), row.getDouble(ColumnIndex.VALUE.ordinal()),
                getTags(row));
        }
    };

    private final RowConverter WRITE_TIME_CONVERTER = new RowConverter() {
        @Override
        public NumericData getData(Row row) {
            return new NumericData(row.getUUID(ColumnIndex.TIME.ordinal()), row.getDouble(ColumnIndex.VALUE.ordinal()),
                getTags(row), row.getLong(ColumnIndex.WRITE_TIME.ordinal()) / 1000);
        }
    };

    private RowConverter rowConverter;

    public NumericDataMapper() {
        this(false);
    }

    public NumericDataMapper(boolean includeWriteTime) {
        if (includeWriteTime) {
            rowConverter = WRITE_TIME_CONVERTER;
        } else {
            rowConverter = DEFAULT_CONVERTER;
        }
    }

    @Override
    public List<NumericData> apply(ResultSet resultSet) {
        if (resultSet.isExhausted()) {
            return Collections.emptyList();
        }
        Row firstRow = resultSet.one();
        NumericMetric2 metric = getMetric(firstRow);
        metric.addData(rowConverter.getData(firstRow));

        for (Row row : resultSet) {
            metric.addData(rowConverter.getData(row));
        }

        return metric.getData();
    }

    private NumericMetric2 getMetric(Row row) {
        NumericMetric2 metric = new NumericMetric2(row.getString(ColumnIndex.TENANT_ID.ordinal()), getId(row),
            row.getMap(ColumnIndex.META_DATA.ordinal(), String.class, String.class),
            row.getInt(ColumnIndex.DATA_RETENTION.ordinal()));
        metric.setDpart(row.getLong(ColumnIndex.DPART.ordinal()));

        return metric;
    }

    private MetricId getId(Row row) {
        return new MetricId(row.getString(1), Interval.parse(row.getString(2)));
    }

    private Set<Tag> getTags(Row row) {
        Map<String, String> map = row.getMap(8, String.class, String.class);
        Set<Tag> tags;
        if (map.isEmpty()) {
            tags = Collections.emptySet();
        } else {
            tags = new HashSet<>();
            for (String tag : map.keySet()) {
                tags.add(new Tag(tag, map.get(tag)));
            }
        }
        return tags;
    }
}
