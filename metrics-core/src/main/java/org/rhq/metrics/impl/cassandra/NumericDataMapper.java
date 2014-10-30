package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import org.rhq.metrics.core.AggregatedValue;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.NumericData;

/**
 * @author John Sanda
 */
public class NumericDataMapper implements Function<ResultSet, List<NumericData>> {

    @Override
    public List<NumericData> apply(ResultSet resultSet) {
        List<NumericData> list = new ArrayList<>();
        for (Row row : resultSet) {
            NumericData d = new NumericData()
                .setTenantId(row.getString(0))
                .setMetric(row.getString(1))
                .setInterval(getInterval(row.getString(2)))
                .setDpart(row.getLong(3))
                .setTimeUUID(row.getUUID(4))
                .putAttributes(row.getMap(5, String.class, String.class))
                .setValue(row.getDouble(6));

            Set<UDTValue> udtValues = row.getSet(7, UDTValue.class);
            for (UDTValue udtValue : udtValues) {
                d.addAggregatedValue(new AggregatedValue(udtValue.getString("type"), udtValue.getDouble("value"),
                    udtValue.getString("src_metric"), getInterval(udtValue.getString("src_metric_interval")),
                    udtValue.getUUID("time")));
            }

            list.add(d);
        }

        return list;
    }

    private Interval getInterval(String s) {
        Preconditions.checkArgument(s != null, "The interval in the database should not be null");
        return s.isEmpty() ? null : Interval.parse(s);
    }
}
