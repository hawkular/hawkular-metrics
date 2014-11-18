package org.rhq.metrics.impl.cassandra;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.NumericData;

/**
 * @author John Sanda
 */
public class TaggedDataMapper implements Function<ResultSet, Map<MetricId, Set<NumericData>>> {

    @Override
    public Map<MetricId, Set<NumericData>> apply(ResultSet resultSet) {
        Map<MetricId, Set<NumericData>> taggedData = new HashMap<>();
        Metric metric = null;
        LinkedHashSet<NumericData> set = new LinkedHashSet<>();
        for (Row row : resultSet) {
            if (metric == null) {
                metric = createMetric(row);
                set.add(createNumericData(row, metric));
            } else {
                Metric nextMetric = createMetric(row);
                if (metric.equals(nextMetric)) {
                    set.add(createNumericData(row, metric));
                } else {
                    taggedData.put(metric.getId(), set);
                    metric = nextMetric;
                    set = new LinkedHashSet<>();
                    set.add(createNumericData(row, metric));
                }
            }
        }
        if (!(metric == null || set.isEmpty())) {
            taggedData.put(metric.getId(), set);
        }
        return taggedData;
    }

    private Metric createMetric(Row row) {
        return new Metric().setTenantId(row.getString(0)).setId(new MetricId(row.getString(3),
            Interval.parse(row.getString(4))));
    }

    private NumericData createNumericData(Row row, Metric metric) {
        return new NumericData(metric, row.getUUID(5), row.getDouble(6));
    }
}
