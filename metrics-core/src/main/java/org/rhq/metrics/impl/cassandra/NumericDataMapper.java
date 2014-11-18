package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.NumericData;

/**
 * @author John Sanda
 */
public class NumericDataMapper implements Function<ResultSet, List<NumericData>> {

    private boolean includeAttributes;

    public NumericDataMapper() {
        this(true);
    }

    public NumericDataMapper(boolean includeAttributes) {
        this.includeAttributes = includeAttributes;
    }

    @Override
    public List<NumericData> apply(ResultSet resultSet) {
        if (resultSet.isExhausted()) {
            return Collections.emptyList();
        }
        Row firstRow = resultSet.one();
        Metric metric = getMetric(firstRow);
        List<NumericData> data = new ArrayList<>();
        data.add(new NumericData(metric, firstRow.getUUID(4), firstRow.getDouble(6)));

        for (Row row : resultSet) {
            data.add(new NumericData(metric, row.getUUID(4), row.getDouble(6)));
        }

        return data;


//        if (includeAttributes) {
//            for (Row row : resultSet) {
//                NumericData d = new NumericData()
//                    .setTenantId(row.getString(0))
//                    .setId(new MetricId(row.getString(1), getInterval(row.getString(2))))
//                    .setDpart(row.getLong(3))
//                    .setTimeUUID(row.getUUID(4))
//                    .putAttributes(row.getMap(5, String.class, String.class))
//                    .setValue(row.getDouble(6));
//
//                Set<UDTValue> udtValues = row.getSet(7, UDTValue.class);
//                for (UDTValue udtValue : udtValues) {
//                    d.addAggregatedValue(new AggregatedValue(udtValue.getString("type"), udtValue.getDouble("value"),
//                        udtValue.getString("src_metric"), getInterval(udtValue.getString("src_metric_interval")),
//                        udtValue.getUUID("time")));
//                }
//
//                list.add(d);
//            }
//        } else {
//            for (Row row : resultSet) {
//                NumericData d = new NumericData()
//                    .setTenantId(row.getString(0))
//                    .setId(new MetricId(row.getString(1), getInterval(row.getString(2))))
//                    .setDpart(row.getLong(3))
//                    .setTimeUUID(row.getUUID(4))
//                    .setValue(row.getDouble(5));
//
//                Set<UDTValue> udtValues = row.getSet(6, UDTValue.class);
//                for (UDTValue udtValue : udtValues) {
//                    d.addAggregatedValue(new AggregatedValue(udtValue.getString("type"), udtValue.getDouble("value"),
//                        udtValue.getString("src_metric"), getInterval(udtValue.getString("src_metric_interval")),
//                        udtValue.getUUID("time")));
//                }
//
//                list.add(d);
//            }
//        }
    }

    private Metric getMetric(Row row) {
        return new Metric()
            .setTenantId(row.getString(0))
            .setId(getId(row))
            .setDpart(row.getLong(3))
            .setAttributes(row.getMap(5, String.class, String.class));
    }

    private MetricId getId(Row row) {
        return new MetricId(row.getString(1), Interval.parse(row.getString(2)));
    }

    private Interval getInterval(String s) {
        Preconditions.checkArgument(s != null, "The interval in the database should not be null");
        return Interval.parse(s);
    }
}
