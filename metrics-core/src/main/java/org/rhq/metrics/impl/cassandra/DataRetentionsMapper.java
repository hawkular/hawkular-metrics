package org.rhq.metrics.impl.cassandra;

import java.util.HashSet;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.Retention;

/**
 * @author John Sanda
 */
public class DataRetentionsMapper implements Function<ResultSet, Set<Retention>> {

    @Override
    public Set<Retention> apply(ResultSet resultSet) {
        Set<Retention> dataRetentions = new HashSet<>();
        for (Row row : resultSet) {
            dataRetentions.add(new Retention(new MetricId(row.getString(3), Interval.parse(row.getString(2))),
                row.getInt(4)));
        }
        return dataRetentions;
    }
}
