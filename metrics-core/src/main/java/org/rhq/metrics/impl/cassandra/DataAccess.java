package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.datastax.driver.core.utils.UUIDs;

import org.rhq.metrics.core.AggregationTemplate;
import org.rhq.metrics.core.Availability;
import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric2;
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

    private PreparedStatement addAttributes;

    private PreparedStatement insertNumericData;

    private PreparedStatement findNumericDataByDateRangeExclusive;

    private PreparedStatement findNumericDataByDateRangeInclusive;

    private PreparedStatement deleteNumericMetric;

    private PreparedStatement findNumericMetrics;

    private PreparedStatement updateCounter;

    private PreparedStatement findCountersByGroup;

    private PreparedStatement findCountersByGroupAndName;

    private PreparedStatement insertTags;

    private PreparedStatement findDataByTag;

    private PreparedStatement insertAvailability;

    private PreparedStatement findAvailabilities;

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

        addAttributes = session.prepare(
            "UPDATE data " +
            "SET attributes = attributes + ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ?");

        insertNumericData = session.prepare(
            "UPDATE data " +
            "SET attributes = attributes + ?, n_value = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time = ?");

        findNumericDataByDateRangeExclusive = session.prepare(
            "SELECT tenant_id, metric, interval, dpart, time, attributes, n_value, aggregates " +
            "FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ? AND time < ?");

        findNumericDataByDateRangeInclusive = session.prepare(
            "SELECT tenant_id, metric, interval, dpart, time, attributes, n_value, aggregates " +
            " FROM data " +
            " WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ? AND time <= ?");

        deleteNumericMetric = session.prepare(
            "DELETE FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ?");

        findNumericMetrics = session.prepare(
            "SELECT DISTINCT tenant_id, type, metric, interval, dpart FROM data;");

        updateCounter = session.prepare(
            "UPDATE counters " +
            "SET c_value = c_value + ? " +
            "WHERE tenant_id = ? AND group = ? AND c_name = ?");

        findCountersByGroup = session.prepare(
            "SELECT tenant_id, group, c_name, c_value FROM counters WHERE tenant_id = ? AND group = ?");

        findCountersByGroupAndName = session.prepare(
            "SELECT tenant_id, group, c_name, c_value FROM counters WHERE tenant_id = ? AND group = ? AND c_name IN ?");

        insertTags = session.prepare(
            "INSERT INTO tags (tenant_id, tag, type, metric, interval, time, n_value) VALUES (?, ?, ?, ?, ?, ?, ?)");

        findDataByTag = session.prepare(
            "SELECT tenant_id, tag, type, metric, interval, time, n_value " +
            "FROM tags " +
            "WHERE tenant_id = ? AND tag = ? AND type = ?");

//        insertAvailability = session.prepare(
//            "INSERT INTO data (tenant_id, metric, interval, dpart, time, availability) " +
//            "VALUES (?, ?, ?, ?, ?, ?)");

        insertAvailability = session.prepare(
            "UPDATE data " +
            "SET attributes = attributes + ?, availability = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time = ?");

        findAvailabilities = session.prepare(
            "SELECT tenant_id, metric, interval, dpart, time, attributes, availability " +
            "FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ? AND time < ?");
    }

    public ResultSetFuture insertTenant(Tenant tenant) {
        UserType aggregationTemplateType = getKeyspace().getUserType("aggregation_template");
        List<UDTValue> templateValues = new ArrayList<>(tenant.getAggregationTemplates().size());
        for (AggregationTemplate template : tenant.getAggregationTemplates()) {
            UDTValue value = aggregationTemplateType.newValue();
            value.setInt("type", template.getType().getCode());
            value.setString("interval", template.getInterval().toString());
            value.setSet("fns", template.getFunctions());
            templateValues.add(value);
        }

        Map<TupleValue, Integer> retentions = new HashMap<>();
        for (RetentionSettings.RetentionKey key : tenant.getRetentionSettings().keySet()) {
            TupleType metricType = TupleType.of(DataType.cint(), DataType.text());
            TupleValue tuple = metricType.newValue();
            tuple.setInt(0, key.metricType.getCode());
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

    public ResultSetFuture addAttributes(Metric metric) {
        return session.executeAsync(addAttributes.bind(metric.getAttributes(), metric.getTenantId(),
            metric.getType().getCode(), metric.getId().getName(), metric.getId().getInterval().toString(),
            metric.getDpart()));
    }

//    public ResultSetFuture insertNumericData(NumericData data) {
//        UserType aggregateDataType = getKeyspace().getUserType("aggregate_data");
//        Set<UDTValue> aggregateDataValues = new HashSet<>();
//
//        for (AggregatedValue v : data.getAggregatedValues()) {
//            aggregateDataValues.add(aggregateDataType.newValue()
//                .setString("type", v.getType())
//                .setDouble("value", v.getValue())
//                .setUUID("time", v.getTimeUUID())
//                .setString("src_metric", v.getSrcMetric())
//                .setString("src_metric_interval", getInterval(v.getSrcMetricInterval())));
//        }
//
//        return session.executeAsync(insertNumericData.bind(data.getAttributes(), data.getValue(), aggregateDataValues,
//            data.getTenantId(), data.getId().getName(), data.getId().getInterval().toString(), 0L, data.getTimeUUID()));
//    }

    public ResultSetFuture insertNumericData(List<NumericData> data) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (NumericData d : data) {
            // TODO Determine what if there is any performance overhead for adding an empty map
            // If there is some overhead, then we will want to use a different prepared
            // statement when there are no attributes.
            batchStatement.add(insertNumericData.bind(d.getMetric().getAttributes(), d.getValue(),
                d.getMetric().getTenantId(), d.getMetric().getType().getCode(), d.getMetric().getId().getName(),
                d.getMetric().getId().getInterval().toString(), d.getMetric().getDpart(), d.getTimeUUID()));
        }
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture insertData(NumericMetric2 metric) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (NumericData d : metric.getData()) {
            batchStatement.add(insertNumericData.bind(metric.getAttributes(), d.getValue(), metric.getTenantId(),
                metric.getType().getCode(), metric.getId().getName(), metric.getId().getInterval().toString(),
                metric.getDpart(), d.getTimeUUID()));
        }
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture findData(NumericMetric2 metric, long startTime, long endTime) {
        return session.executeAsync(
            findNumericDataByDateRangeExclusive.bind(metric.getTenantId(), MetricType.NUMERIC.getCode(),
                metric.getId().getName(), metric.getId().getInterval().toString(), metric.getDpart(),
                TimeUUIDUtils.getTimeUUID(startTime), TimeUUIDUtils.getTimeUUID(endTime)));
    }

    public ResultSetFuture findData(NumericMetric2 metric, long timestamp) {
        return session.executeAsync(findNumericDataByDateRangeInclusive.bind(metric.getTenantId(),
            MetricType.NUMERIC.getCode(), metric.getId().getName(), metric.getId().getInterval().toString(),
            metric.getDpart(), UUIDs.startOf(timestamp), UUIDs.endOf(timestamp)));
    }

    public ResultSetFuture deleteNumericMetric(String tenantId, String metric, Interval interval, long dpart) {
        return session.executeAsync(deleteNumericMetric.bind(tenantId, MetricType.NUMERIC.getCode(), metric,
            interval.toString(), dpart));
    }

    public ResultSetFuture findAllNumericMetrics() {
        return session.executeAsync(findNumericMetrics.bind());
    }

    public ResultSetFuture insertTag(String tag, List<NumericData> data) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (NumericData d : data) {
            batchStatement.add(insertTags.bind(d.getMetric().getTenantId(), tag, MetricType.NUMERIC.getCode(),
                d.getMetric().getId().getName(), d.getMetric().getId().getInterval().toString(), d.getTimeUUID(),
                d.getValue()));
        }
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture findData(String tenantId, String tag, MetricType type) {
        return session.executeAsync(findDataByTag.bind(tenantId, tag, type.getCode()));
    }

    public ResultSetFuture insertAvailability(Availability a) {
        return session.executeAsync(insertAvailability.bind(a.getMetric().getAttributes(), a.getBytes(),
            a.getMetric().getTenantId(), a.getMetric().getType().getCode(), a.getMetric().getId().getName(),
            a.getMetric().getId().getInterval().toString(), a.getMetric().getDpart(), a.getTimeUUID()));
    }

    public ResultSetFuture insertData(AvailabilityMetric metric) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (Availability a : metric.getData()) {
            batchStatement.add(insertAvailability.bind(metric.getAttributes(), a.getBytes(), metric.getTenantId(),
                metric.getType().getCode(), metric.getId().getName(), metric.getId().getInterval().toString(),
                metric.getDpart(), a.getTimeUUID()));
        }
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture findAvailabilityData(AvailabilityMetric metric, long startTime, long endTime) {
        return session.executeAsync(findAvailabilities.bind(metric.getTenantId(), MetricType.AVAILABILITY.getCode(),
            metric.getId().getName(), metric.getId().getInterval().toString(), metric.getDpart(),
            TimeUUIDUtils.getTimeUUID(startTime), TimeUUIDUtils.getTimeUUID(endTime)));
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
