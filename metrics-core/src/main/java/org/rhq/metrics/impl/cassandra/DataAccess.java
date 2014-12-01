package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import com.datastax.driver.core.utils.UUIDs;

import org.rhq.metrics.core.AggregationTemplate;
import org.rhq.metrics.core.Availability;
import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricData;
import org.rhq.metrics.core.MetricId;
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

    private PreparedStatement insertMetric;

    private PreparedStatement findMetric;

    private PreparedStatement addMetadata;

    private PreparedStatement deleteMetadata;

    private PreparedStatement insertNumericData;

    private PreparedStatement findNumericDataByDateRangeExclusive;

    private PreparedStatement findNumericDataByDateRangeInclusive;

    private PreparedStatement findAvailabilityByDateRangeInclusive;

    private PreparedStatement deleteNumericMetric;

    private PreparedStatement findNumericMetrics;

    private PreparedStatement updateCounter;

    private PreparedStatement findCountersByGroup;

    private PreparedStatement findCountersByGroupAndName;

    private PreparedStatement insertNumericTags;

    private PreparedStatement insertAvailabilityTags;

    private PreparedStatement updateDataWithTags;

    private PreparedStatement findNumericDataByTag;

    private PreparedStatement findAvailabilityByTag;

    private PreparedStatement insertAvailability;

    private PreparedStatement findAvailabilities;

    private PreparedStatement updateMetricsIndex;

    private PreparedStatement addMetadataToMetricsIndex;

    private PreparedStatement deleteMetadataFromMetricsIndex;

    private PreparedStatement readMetricsIndex;

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

        findMetric = session.prepare(
            "SELECT tenant_id, type, metric, interval, dpart, meta_data " +
            "FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ?");

        addMetadata = session.prepare(
            "UPDATE data " +
            "SET meta_data = meta_data + ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ?");

        deleteMetadata = session.prepare(
            "UPDATE data " +
            "SET meta_data = meta_data - ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ?");

        insertMetric = session.prepare(
            "INSERT INTO metrics_idx (tenant_id, type, interval, metric, meta_data) " +
            "VALUES (?, ?, ?, ?, ?) " +
            "IF NOT EXISTS");

        updateMetricsIndex = session.prepare(
            "INSERT INTO metrics_idx (tenant_id, type, interval, metric) VALUES (?, ?, ?, ?)");

        addMetadataToMetricsIndex = session.prepare(
            "UPDATE metrics_idx " +
            "SET meta_data = meta_data + ? " +
            "WHERE tenant_id = ? AND type = ? AND interval = ? AND metric = ?");

        deleteMetadataFromMetricsIndex = session.prepare(
            "UPDATE metrics_idx " +
            "SET meta_data = meta_data - ?" +
            "WHERE tenant_id = ? AND type = ? AND interval = ? AND metric = ?");

        readMetricsIndex = session.prepare(
            "SELECT metric, interval, meta_data " +
            "FROM metrics_idx " +
            "WHERE tenant_id = ? AND type = ?");

        insertNumericData = session.prepare(
            "UPDATE data " +
            "SET meta_data = meta_data + ?, n_value = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time = ?");

        findNumericDataByDateRangeExclusive = session.prepare(
            "SELECT tenant_id, metric, interval, dpart, time, meta_data, n_value, aggregates, tags " +
            "FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ? AND time < ?");

        findNumericDataByDateRangeInclusive = session.prepare(
            "SELECT tenant_id, metric, interval, dpart, time, meta_data, n_value, aggregates, tags " +
            "FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ? AND time <= ?");

        findAvailabilityByDateRangeInclusive = session.prepare(
            "SELECT tenant_id, metric, interval, dpart, time, meta_data, availability, tags " +
            "FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ? AND time <= ?");

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

        insertNumericTags = session.prepare(
            "INSERT INTO tags (tenant_id, tag, type, metric, interval, time, n_value) VALUES (?, ?, ?, ?, ?, ?, ?)");

        insertAvailabilityTags = session.prepare(
            "INSERT INTO tags (tenant_id, tag, type, metric, interval, time, availability) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)");

        updateDataWithTags = session.prepare(
            "UPDATE data " +
            "SET tags = tags + ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time = ?");

        findNumericDataByTag = session.prepare(
            "SELECT tenant_id, tag, type, metric, interval, time, n_value " +
            "FROM tags " +
            "WHERE tenant_id = ? AND tag = ? AND type = ?");

        findAvailabilityByTag = session.prepare(
            "SELECT tenant_id, tag, type, metric, interval, time, availability " +
            "FROM tags " +
            "WHERE tenant_id = ? AND tag = ? AND type = ?");

//        insertAvailability = session.prepare(
//            "INSERT INTO data (tenant_id, metric, interval, dpart, time, availability) " +
//            "VALUES (?, ?, ?, ?, ?, ?)");

        insertAvailability = session.prepare(
            "UPDATE data " +
            "SET meta_data = meta_data + ?, availability = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time = ?");

        findAvailabilities = session.prepare(
            "SELECT tenant_id, metric, interval, dpart, time, meta_data, availability, tags " +
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

    public ResultSetFuture insertMetric(Metric metric) {
        return session.executeAsync(insertMetric.bind(metric.getTenantId(), metric.getType().getCode(),
            metric.getId().getInterval().toString(), metric.getId().getName(), metric.getMetadata()));
    }

    public ResultSetFuture findMetric(String tenantId, MetricType type, MetricId id, long dpart) {
        return session.executeAsync(findMetric.bind(tenantId, type.getCode(), id.getName(),
            id.getInterval().toString(), dpart));
    }

    public ResultSetFuture addMetadata(Metric metric) {
        return session.executeAsync(addMetadata.bind(metric.getMetadata(), metric.getTenantId(),
            metric.getType().getCode(), metric.getId().getName(), metric.getId().getInterval().toString(),
            metric.getDpart()));
    }

    public ResultSetFuture updateMetadata(Metric metric, Map<String, String> additions, Set<String> removals) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED)
            .add(addMetadata.bind(additions, metric.getTenantId(), metric.getType().getCode(), metric.getId().getName(),
                metric.getId().getInterval().toString(), metric.getDpart()))
            .add(deleteMetadata.bind(removals, metric.getTenantId(), metric.getType().getCode(),
                metric.getId().getName(), metric.getId().getInterval().toString(), metric.getDpart()));
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture updateMetadataInMetricsIndex(Metric metric, Map<String, String> additions,
        Set<String> deletions) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED)
            .add(addMetadataToMetricsIndex.bind(additions, metric.getTenantId(),
                metric.getType().getCode(), metric.getId().getInterval().toString(), metric.getId().getName()))
            .add(deleteMetadataFromMetricsIndex.bind(deletions, metric.getTenantId(), metric.getType().getCode(),
                metric.getId().getInterval().toString(), metric.getId().getName()));
        return session.executeAsync(batchStatement);
    }

    public <T extends Metric> ResultSetFuture updateMetricsIndex(List<T> metrics) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (T metric : metrics) {
            batchStatement.add(updateMetricsIndex.bind(metric.getTenantId(), metric.getType().getCode(),
                metric.getId().getInterval().toString(), metric.getId().getName()));
        }
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture findMetricsInMetricsIndex(String tenantId, MetricType type) {
        return session.executeAsync(readMetricsIndex.bind(tenantId, type.getCode()));
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
            // statement when there are is meta data.
            batchStatement.add(insertNumericData.bind(d.getMetric().getMetadata(), d.getValue(),
                d.getMetric().getTenantId(), d.getMetric().getType().getCode(), d.getMetric().getId().getName(),
                d.getMetric().getId().getInterval().toString(), d.getMetric().getDpart(), d.getTimeUUID()));
        }
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture insertData(NumericMetric2 metric) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (NumericData d : metric.getData()) {
            batchStatement.add(insertNumericData.bind(metric.getMetadata(), d.getValue(), metric.getTenantId(),
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

    public ResultSetFuture findData(AvailabilityMetric metric, long timestamp) {
        return session.executeAsync(findAvailabilityByDateRangeInclusive.bind(metric.getTenantId(),
            MetricType.AVAILABILITY.getCode(), metric.getId().getName(), metric.getId().getInterval().toString(),
            metric.getDpart(), UUIDs.startOf(timestamp), UUIDs.endOf(timestamp)));
    }

    public ResultSetFuture deleteNumericMetric(String tenantId, String metric, Interval interval, long dpart) {
        return session.executeAsync(deleteNumericMetric.bind(tenantId, MetricType.NUMERIC.getCode(), metric,
            interval.toString(), dpart));
    }

    public ResultSetFuture findAllNumericMetrics() {
        return session.executeAsync(findNumericMetrics.bind());
    }

    public ResultSetFuture insertNumericTag(String tag, List<NumericData> data) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (NumericData d : data) {
            batchStatement.add(insertNumericTags.bind(d.getMetric().getTenantId(), tag, MetricType.NUMERIC.getCode(),
                d.getMetric().getId().getName(), d.getMetric().getId().getInterval().toString(), d.getTimeUUID(),
                d.getValue()));
        }
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture insertAvailabilityTag(String tag, List<Availability> data) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (Availability a : data) {
            batchStatement.add(insertAvailabilityTags.bind(a.getMetric().getTenantId(), tag,
                MetricType.AVAILABILITY.getCode(), a.getMetric().getId().getName(),
                a.getMetric().getId().getInterval().toString(), a.getTimeUUID(), a.getBytes()));
        }
        return session.executeAsync(batchStatement);
    }

    public ResultSetFuture updateDataWithTag(MetricData data, Set<String> tags) {
        Map<String, String> tagMap = new HashMap<>();
        for (String tag : tags) {
            tagMap.put(tag, "");
        }
        return session.executeAsync(updateDataWithTags.bind(tagMap, data.getMetric().getTenantId(),
            data.getMetric().getType().getCode(), data.getMetric().getId().getName(),
            data.getMetric().getId().getInterval().toString(), data.getMetric().getDpart(), data.getTimeUUID()));
    }

    public ResultSetFuture findNumericDataByTag(String tenantId, String tag) {
        return session.executeAsync(findNumericDataByTag.bind(tenantId, tag, MetricType.NUMERIC.getCode()));
    }

    public ResultSetFuture findAvailabilityByTag(String tenantId, String tag) {
        return session.executeAsync(findAvailabilityByTag.bind(tenantId, tag, MetricType.AVAILABILITY.getCode()));
    }

    public ResultSetFuture insertAvailability(Availability a) {
        return session.executeAsync(insertAvailability.bind(a.getMetric().getMetadata(), a.getBytes(),
            a.getMetric().getTenantId(), a.getMetric().getType().getCode(), a.getMetric().getId().getName(),
            a.getMetric().getId().getInterval().toString(), a.getMetric().getDpart(), a.getTimeUUID()));
    }

    public ResultSetFuture insertData(AvailabilityMetric metric) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (Availability a : metric.getData()) {
            batchStatement.add(insertAvailability.bind(metric.getMetadata(), a.getBytes(), metric.getTenantId(),
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
