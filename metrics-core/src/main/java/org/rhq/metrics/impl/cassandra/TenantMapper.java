package org.rhq.metrics.impl.cassandra;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.google.common.base.Function;

import org.rhq.metrics.core.AggregationTemplate;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.Tenant;

/**
 * @author John Sanda
 */
public class TenantMapper implements Function<ResultSet, Tenant> {

    @Override
    public Tenant apply(ResultSet resultSet) {
        if (resultSet.isExhausted()) {
            return null;
        }
        Row row = resultSet.one();
        Tenant tenant = new Tenant().setId(row.getString(0));
        Map<TupleValue, Integer> retentions = row.getMap(1, TupleValue.class, Integer.class);
        for (Map.Entry<TupleValue, Integer> entry : retentions.entrySet()) {
            MetricType metricType = MetricType.fromCode(entry.getKey().getInt(0));
            if (entry.getKey().isNull(1)) {
                tenant.setRetention(metricType, entry.getValue());
            } else {
                Interval interval = Interval.parse(entry.getKey().getString(1));
                tenant.setRetention(metricType, interval, entry.getValue());
            }
        }

        List<UDTValue> templateValues = row.getList(2, UDTValue.class);
        for (UDTValue value : templateValues) {
            tenant.addAggregationTemplate(new AggregationTemplate()
                .setType(MetricType.fromCode(value.getInt("type")))
                .setInterval(Interval.parse(value.getString("interval")))
                .setFunctions(value.getSet("fns", String.class)));
        }

        return tenant;
    }
}
