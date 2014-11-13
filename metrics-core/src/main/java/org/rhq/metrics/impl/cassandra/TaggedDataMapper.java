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

/**
 * @author John Sanda
 */
public class TaggedDataMapper implements Function<ResultSet, Map<MetricId, Set<NumericData>>> {

    @Override
    public Map<MetricId, Set<NumericData>> apply(ResultSet resultSet) {
        Map<MetricId, Set<NumericData>> taggedData = new HashMap<>();
        MetricId id = null;
        LinkedHashSet<NumericData> set = new LinkedHashSet<>();
        for (Row row : resultSet) {
            if (id == null) {
                id = new MetricId(row.getString(3), Interval.parse(row.getString(4)));
                set.add(createNumericData(row, id));
            } else {
                MetricId nextId = new MetricId(row.getString(3), Interval.parse(row.getString(4)));
                if (id.equals(nextId)) {
                    set.add(createNumericData(row, id));
                } else {
                    taggedData.put(id, set);
                    id = nextId;
                    set = new LinkedHashSet<>();
                    set.add(createNumericData(row, id));
                }
            }
        }
        if (!set.isEmpty()) {
            taggedData.put(id, set);
        }
        return taggedData;
    }

    private NumericData createNumericData(Row row, MetricId id) {
        return new NumericData()
            .setTenantId(row.getString(0))
            .setId(id)
            .setTimeUUID(row.getUUID(5))
            .setValue(row.getDouble(6));
    }
}
