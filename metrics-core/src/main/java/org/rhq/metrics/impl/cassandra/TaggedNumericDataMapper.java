package org.rhq.metrics.impl.cassandra;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric2;

/**
 * @author John Sanda
 */
public class TaggedNumericDataMapper implements Function<ResultSet, Map<MetricId, Set<NumericData>>> {

    @Override
    public Map<MetricId, Set<NumericData>> apply(ResultSet resultSet) {
        Map<MetricId, Set<NumericData>> taggedData = new HashMap<>();
        NumericMetric2 metric = null;
        LinkedHashSet<NumericData> set = new LinkedHashSet<>();
        for (Row row : resultSet) {
            if (metric == null) {
                metric = createMetric(row);
                set.add(createNumericData(row, metric));
            } else {
                NumericMetric2 nextMetric = createMetric(row);
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

    private NumericMetric2 createMetric(Row row) {
        return new NumericMetric2(row.getString(0), new MetricId(row.getString(3), Interval.parse(row.getString(4))));
    }

    private NumericData createNumericData(Row row, NumericMetric2 metric) {
        return new NumericData(metric, row.getUUID(5), row.getDouble(6));
    }

}
