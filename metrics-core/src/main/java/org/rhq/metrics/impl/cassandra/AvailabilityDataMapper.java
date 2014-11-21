package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
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

    @Override
    public List<Availability> apply(ResultSet resultSet) {
        if (resultSet.isExhausted()) {
            return Collections.emptyList();
        }
        Row firstRow = resultSet.one();
        AvailabilityMetric metric = getMetric(firstRow);
        List<Availability> data = new ArrayList<>();
        data.add(new Availability(metric, firstRow.getUUID(4), firstRow.getBytes(6), getTags(firstRow)));

        for (Row row : resultSet) {
            data.add(new Availability(metric, row.getUUID(4), row.getBytes(6), getTags(row)));
        }

        return data;
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
