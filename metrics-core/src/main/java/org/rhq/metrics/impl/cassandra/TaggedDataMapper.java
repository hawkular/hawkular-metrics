package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.NumericData;

/**
 * @author John Sanda
 */
public class TaggedDataMapper implements Function<ResultSet, List<NumericData>> {

    @Override
    public List<NumericData> apply(ResultSet resultSet) {
        List<NumericData> list = new ArrayList<>();
        for (Row row : resultSet) {
            NumericData d = new NumericData()
                .setTenantId(row.getString(0))
                .setMetric(row.getString(3))
                .setInterval(Interval.parse(row.getString(4)))
                .setTimeUUID(row.getUUID(5))
                .setValue(row.getDouble(6));
            list.add(d);
        }
        return list;
    }
}
