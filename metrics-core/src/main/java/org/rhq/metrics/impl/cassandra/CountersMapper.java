package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.rhq.metrics.core.Counter;

/**
 * @author John Sanda
 */
public class CountersMapper implements Function<ResultSet, List<Counter>> {

    @Override
    public List<Counter> apply(ResultSet resultSet) {
        List<Counter> counters = new ArrayList<>();
        for (Row row : resultSet) {
            counters.add(new Counter(row.getString(0), row.getString(1), row.getString(2), row.getLong(3)));
        }
        return counters;
    }
}
