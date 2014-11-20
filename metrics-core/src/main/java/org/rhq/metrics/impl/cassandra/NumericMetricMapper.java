package org.rhq.metrics.impl.cassandra;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.NumericMetric2;
import org.rhq.metrics.core.Tag;

/**
 * @author John Sanda
 */
public class NumericMetricMapper implements Function<ResultSet, NumericMetric2> {

    @Override
    public NumericMetric2 apply(ResultSet resultSet) {
        if (resultSet.isExhausted()) {
            return null;
        }

        Row firstRow = resultSet.one();
        NumericMetric2 metric = getMetric(firstRow);
        metric.addData(firstRow.getUUID(4), firstRow.getDouble(6), getTags(firstRow));

        for (Row row : resultSet) {
            metric.addData(row.getUUID(4), row.getDouble(6), getTags(row));
        }

        return metric;
    }

    private NumericMetric2 getMetric(Row row) {
        return new NumericMetric2(row.getString(0), getId(row), row.getMap(5, String.class, String.class));
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
