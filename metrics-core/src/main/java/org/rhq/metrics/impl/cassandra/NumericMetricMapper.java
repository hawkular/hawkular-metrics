package org.rhq.metrics.impl.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.NumericMetric2;

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
        metric.addData(firstRow.getUUID(4), firstRow.getDouble(6));

        for (Row row : resultSet) {
            metric.addData(row.getUUID(4), row.getDouble(6));
        }

        return metric;

//        List<Metric> metrics = new ArrayList<>();
//        Metric metric = null;
//        List<NumericData> data = new ArrayList<>();
//
//        for (Row row : resultSet) {
//            if (metric == null) {
//                metric = getMetric(row);
//                data.add(new NumericData(row.getUUID(4), row.getDouble(6)));
//            } else {
//                MetricId id = getId(row);
//                if (metric.getId().equals(id)) {
//                    data.add(new NumericData(row.getUUID(4), row.getDouble(6)));
//                } else {
//                    data.add(new NumericData(row.getUUID(4), row.getDouble(6)));
//                    metric.setData(data);
//                    metrics.add(metric);
//                    metric = getMetric(row);
//                    data = new ArrayList<>();
//                }
//            }
//        }
//        metric.setData(data);
//        metrics.add(metric);
//
//        return metrics;
    }

    private NumericMetric2 getMetric(Row row) {
        return new NumericMetric2(row.getString(0), getId(row), row.getMap(5, String.class, String.class));
    }

    private MetricId getId(Row row) {
        return new MetricId(row.getString(1), Interval.parse(row.getString(2)));
    }
}
