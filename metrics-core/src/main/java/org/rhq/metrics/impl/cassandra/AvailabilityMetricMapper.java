package org.rhq.metrics.impl.cassandra;

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
public class AvailabilityMetricMapper implements Function<ResultSet, AvailabilityMetric> {

    @Override
    public AvailabilityMetric apply(ResultSet resultSet) {
        if (resultSet.isExhausted()) {
            return null;
        }
        Row firstRow = resultSet.one();
        AvailabilityMetric metric = getMetric(firstRow);
        metric.addData(new Availability(metric, firstRow.getUUID(4), firstRow.getBytes(6)));

        for (Row row : resultSet) {
            metric.addData(new Availability(metric, row.getUUID(4), row.getBytes(6)));
        }

        return metric;
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
}
