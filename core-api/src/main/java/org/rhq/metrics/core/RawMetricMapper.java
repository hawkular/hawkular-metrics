package org.rhq.metrics.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * @author John Sanda
 */
public class RawMetricMapper {

    public RawNumericMetric map(Row row) {
        Map<Integer, Double> map = row.getMap(2, Integer.class, Double.class);
        return new RawNumericMetric(row.getString(0), map.get(DataType.RAW.ordinal()), row.getDate(1).getTime());
    }

    public List<RawNumericMetric> map(ResultSet resultSet) {
        List<RawNumericMetric> metrics = new ArrayList<RawNumericMetric>();
        for (Row row : resultSet) {
            metrics.add(map(row));
        }
        return metrics;
    }

}
