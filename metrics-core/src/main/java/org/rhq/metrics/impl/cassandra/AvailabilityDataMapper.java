package org.rhq.metrics.impl.cassandra;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
public class AvailabilityDataMapper implements Function<ResultSet, List<Availability>> {

    private interface RowConverter {
        Availability getData(Row row);
    }

    private final RowConverter DEFAULT_CONVERTER = new RowConverter() {
        @Override
        public Availability getData(Row row) {
            return new Availability(row.getUUID(4), row.getBytes(6), getTags(row));
        }
    };

    private final RowConverter WRITE_TIME_CONVERTER = new RowConverter() {
        @Override
        public Availability getData(Row row) {
            return new Availability(row.getUUID(4), row.getBytes(6), getTags(row), row.getLong(8) / 1000);
        }
    };

    private RowConverter rowConverter;

    public AvailabilityDataMapper() {
        this(false);
    }

    public AvailabilityDataMapper(boolean includeWriteTime) {
        if (includeWriteTime) {
            rowConverter = WRITE_TIME_CONVERTER;
        } else {
            rowConverter = DEFAULT_CONVERTER;
        }
    }

    @Override
    public List<Availability> apply(ResultSet resultSet) {
        if (resultSet.isExhausted()) {
            return Collections.emptyList();
        }
        Row firstRow = resultSet.one();
        AvailabilityMetric metric = getMetric(firstRow);
        metric.addData(rowConverter.getData(firstRow));

        for (Row row : resultSet) {
            metric.addData(rowConverter.getData(row));
        }

        return metric.getData();
    }

    private AvailabilityMetric getMetric(Row row) {
        AvailabilityMetric metric = new AvailabilityMetric(row.getString(0), getId(row),
            row.getMap(5, String.class, String.class));
        metric.setDpart(row.getLong(3));

        return metric;
    }

    private MetricId getId(Row row) {
        return new MetricId(row.getString(1), Interval.parse(row.getString(2)));
    }

    private Set<Tag> getTags(Row row) {
        Map<String, String> map = row.getMap(7, String.class, String.class);
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
