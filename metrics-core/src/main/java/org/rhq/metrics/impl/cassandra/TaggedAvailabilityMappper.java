package org.rhq.metrics.impl.cassandra;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.rhq.metrics.core.Availability;
import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricId;

/**
 * @author John Sanda
 */
public class TaggedAvailabilityMappper implements Function<ResultSet, Map<MetricId, Set<Availability>>> {

    @Override
    public Map<MetricId, Set<Availability>> apply(ResultSet resultSet) {
        Map<MetricId, Set<Availability>> taggedData = new HashMap<>();
        AvailabilityMetric metric = null;
        LinkedHashSet<Availability> set = new LinkedHashSet<>();
        for (Row row : resultSet) {
            if (metric == null) {
                metric = createMetric(row);
                set.add(createAvailability(row, metric));
            } else {
                AvailabilityMetric nextMetric = createMetric(row);
                if (metric.equals(nextMetric)) {
                    set.add(createAvailability(row, metric));
                } else {
                    taggedData.put(metric.getId(), set);
                    metric = nextMetric;
                    set = new LinkedHashSet<>();
                    set.add(createAvailability(row, metric));
                }
            }
        }
        if (!(metric == null || set.isEmpty())) {
            taggedData.put(metric.getId(), set);
        }
        return taggedData;
    }

    private AvailabilityMetric createMetric(Row row) {
        return new AvailabilityMetric(row.getString(0), new MetricId(row.getString(3),
            Interval.parse(row.getString(4))));
    }

    private Availability createAvailability(Row row, AvailabilityMetric metric) {
        return new Availability(metric, row.getUUID(5), row.getBytes(6));
    }

}
