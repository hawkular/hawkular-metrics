package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

import org.rhq.metrics.core.AggregatedValue;
import org.rhq.metrics.core.AggregationTemplate;
import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.RetentionSettings;
import org.rhq.metrics.core.Tenant;
import org.rhq.metrics.util.TimeUUIDUtils;

/**
 *
 * @author John Sanda
 */
public class DataAccess {

    private Session session;

    private PreparedStatement insertTenant;

    private PreparedStatement findTenants;

    private PreparedStatement addNumericAttributes;

    private PreparedStatement insertNumericData;

    private PreparedStatement findNumericData;

    private PreparedStatement deleteNumericMetric;

    private PreparedStatement findNumericMetrics;

    private PreparedStatement updateCounter;

    private PreparedStatement findCountersByGroup;

    private PreparedStatement findCountersByGroupAndName;

    private PreparedStatement insertTags;

    private PreparedStatement findDataByTag;

    public DataAccess(Session session) {
        this.session = session;
        initPreparedStatements();
    }

    private void initPreparedStatements() {
        insertTenant = session.prepare(
            "INSERT INTO tenants (id, retentions, aggregation_templates) " +
            "VALUES (?, ?, ?) " +
            "IF NOT EXISTS");

        findTenants = session.prepare("SELECT id, retentions, aggregation_templates FROM tenants");

        addNumericAttributes = session.prepare(
            "UPDATE numeric_data " +
            "SET attributes = attributes + ? " +
            "WHERE tenant_id = ? AND metric = ? AND interval = ? AND dpart = ?");

        insertNumericData = session.prepare(
            "UPDATE numeric_data " +
            "SET attributes = attributes + ?, raw = ?, aggregates = ? " +
            "WHERE tenant_id = ? AND metric = ? AND interval = ? AND dpart = ? AND time = ?");

        findNumericData = session.prepare(
            "SELECT tenant_id, metric, interval, dpart, time, attributes, raw, aggregates " +
            "FROM numeric_data " +
            "WHERE tenant_id = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ? AND time < ?");

        deleteNumericMetric = session.prepare(
            "DELETE FROM numeric_data " +
            "WHERE tenant_id = ? AND metric = ? AND interval = ? AND dpart = ?");

        findNumericMetrics = session.prepare(
            "SELECT DISTINCT tenant_id, metric, interval, dpart FROM numeric_data;");

        updateCounter = session.prepare(
            "UPDATE counters " +
            "SET c_value = c_value + ? " +
            "WHERE tenant_id = ? AND group = ? AND c_name = ?");

        findCountersByGroup = session.prepare(
            "SELECT tenant_id, group, c_name, c_value FROM counters WHERE tenant_id = ? AND group = ?");

        findCountersByGroupAndName = session.prepare(
            "SELECT tenant_id, group, c_name, c_value FROM counters WHERE tenant_id = ? AND group = ? AND c_name IN ?");

        insertTags = session.prepare(
            "INSERT INTO tags (tenant_id, tag, type, metric, interval, time, raw_data) VALUES (?, ?, ?, ?, ?, ?, ?)");

        findDataByTag = session.prepare(
            "SELECT tenant_id, tag, type, metric, interval, time, raw_data " +
            "FROM tags " +
            "WHERE tenant_id = ? AND tag = ?");
    }

    public ResultSetFuture insertTenant(Tenant tenant) {
        UserType aggregationTemplateType = getKeyspace().getUserType("aggregation_template");
        List<UDTValue> templateValues = new ArrayList<>(tenant.getAggregationTemplates().size());
        for (AggregationTemplate template : tenant.getAggregationTemplates()) {
            UDTValue value = aggregationTemplateType.newValue();
            value.setString("type", template.getType().getCode());
            value.setString("interval", template.getInterval().toString());
            value.setSet("fns", template.getFunctions());
            templateValues.add(value);
        }

        Map<TupleValue, Integer> retentions = new HashMap<>();
        for (RetentionSettings.RetentionKey key : tenant.getRetentionSettings().keySet()) {
            TupleType metricType = TupleType.of(DataType.text(), DataType.text());
            TupleValue tuple = metricType.newValue();
            tuple.setString(0, key.metricType.getCode());
            if (key.interval == null) {
                tuple.setString(1, null);
            } else {
                tuple.setString(1, key.interval.toString());
            }
            retentions.put(tuple, tenant.getRetentionSettings().get(key));
        }

        return session.executeAsync(insertTenant.bind(tenant.getId(), retentions, templateValues));
    }

    public ResultSetFuture findTenants() {
        return session.executeAsync(findTenants.bind());
    }

    public ResultSetFuture addNumericAttributes(String tenantId, MetricId id, long dpart,
        Map<String, String> attributes) {
        return session.executeAsync(addNumericAttributes.bind(attributes, tenantId, id.getName(),
            id.getInterval().toString(), dpart));
    }

//    public ResultSetFuture insert(Metric metric) {
//        UserType aggregateDataType = getKeyspace().getUserType("aggregate_data");
//
//        for (NumericDataPoint d : metric.getData()) {
//            Set<UDTValue> aggregateDataValues = new HashSet<>();
//            for (AggregatedValue v : d.getAggregatedValues()) {
//
//            }
//        }
//
//        if (metric.getData().size() > 1) {
//            BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
//            for (NumericDataPoint d : metric.getData()) {
//                batchStatement.add(insertNumericData.bind(metric.getAttributes(), d.getValue(), null,
//                    metric.getTenantId(), metric.getName(), getInterval(metric.getInterval()), 0L, d.getTimeUUID()));
//            }
//            return session.executeAsync(batchStatement);
//        } else {
//
//            statement = insertNumericData.bind(metric.getAttributes(), d.getValue(), null,
//                metric.getTenantId(), metric.getName(), getInterval(metric.getInterval()), 0L, d.getTimeUUID());
//        }
//    }

    public ResultSetFuture insertNumericData(NumericData data) {
        // TODO determine if we should use separate queries/methods for raw vs aggregated data

        UserType aggregateDataType = getKeyspace().getUserType("aggregate_data");
        Set<UDTValue> aggregateDataValues = new HashSet<>();

        for (AggregatedValue v : data.getAggregatedValues()) {
            aggregateDataValues.add(aggregateDataType.newValue()
                .setString("type", v.getType())
                .setDouble("value", v.getValue())
                .setUUID("time", v.getTimeUUID())
                .setString("src_metric", v.getSrcMetric())
                .setString("src_metric_interval", getInterval(v.getSrcMetricInterval())));
        }

        return session.executeAsync(insertNumericData.bind(data.getAttributes(), data.getValue(), aggregateDataValues,
            data.getTenantId(), data.getId().getName(), data.getId().getInterval().toString(), 0L, data.getTimeUUID()));
    }

    private final String getInterval(Interval interval) {
        return interval == null ? "" : interval.toString();
    }

    public ResultSetFuture findNumericData(String tenantId, MetricId id, long dpart, long startTime, long endTime) {
        return session.executeAsync(findNumericData.bind(tenantId, id.getName(), id.getInterval().toString(), dpart,
            TimeUUIDUtils.getTimeUUID(startTime), TimeUUIDUtils.getTimeUUID(endTime)));
    }

    public ResultSetFuture deleteNumericMetric(String tenantId, String metric, Interval interval, long dpart) {
        return session.executeAsync(deleteNumericMetric.bind(tenantId, metric, interval.toString(), dpart));
    }

    public ResultSetFuture findAllNumericMetrics() {
        return session.executeAsync(findNumericMetrics.bind());
    }

    public ResultSetFuture insertTag(String tag, List<NumericData> data) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (NumericData d : data) {
            batchStatement.add(insertTags.bind(d.getTenantId(), tag, MetricType.NUMERIC.getCode(), d.getId().getName(),
                d.getId().getInterval().toString(), d.getTimeUUID(), d.getValue()));
        }
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture findData(String tenantId, String tag) {
        return session.executeAsync(findDataByTag.bind(tenantId, tag));
    }

    public ResultSetFuture updateCounter(Counter counter) {
        BoundStatement statement = updateCounter.bind(counter.getValue(), counter.getTenantId(), counter.getGroup(),
            counter.getName());
        return session.executeAsync(statement);
    }

    public ResultSetFuture updateCounters(Collection<Counter> counters) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.COUNTER);
        for (Counter counter : counters) {
            batchStatement.add(updateCounter.bind(counter.getValue(), counter.getTenantId(), counter.getGroup(),
                counter.getName()));
        }
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture findCounters(String tenantId, String group) {
        BoundStatement statement = findCountersByGroup.bind(tenantId, group);
        return session.executeAsync(statement);
    }

    public ResultSetFuture findCounters(String tenantId, String group, List<String> names) {
        BoundStatement statement = findCountersByGroupAndName.bind(tenantId, group, names);
        return session.executeAsync(statement);
    }

    private KeyspaceMetadata getKeyspace() {
        return session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
    }

}
