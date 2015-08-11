/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.core.impl;

import static java.util.stream.Collectors.toMap;

import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.hawkular.metrics.core.impl.TimeUUIDUtils.getTimeUUID;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

import rx.Observable;

/**
 *
 * @author John Sanda
 */
public class DataAccessImpl implements DataAccess {

    public static final long DPART = 0;
    private Session session;

    private RxSession rxSession;

    private PreparedStatement insertTenant;

    private PreparedStatement findAllTenantIds;

    private PreparedStatement findTenant;

    private PreparedStatement insertIntoMetricsIndex;

    private PreparedStatement findMetric;

    private PreparedStatement getMetricTags;

    private PreparedStatement addMetricTagsToDataTable;

    private PreparedStatement addMetadataAndDataRetention;

    private PreparedStatement deleteMetricTagsFromDataTable;

    private PreparedStatement insertGaugeData;

    private PreparedStatement insertCounterData;

    private PreparedStatement findCounterDataExclusive;

    private PreparedStatement findGaugeDataByDateRangeExclusive;

    private PreparedStatement findGaugeDataByDateRangeExclusiveASC;

    private PreparedStatement findGaugeDataWithWriteTimeByDateRangeExclusive;

    private PreparedStatement findGaugeDataByDateRangeInclusive;

    private PreparedStatement findGaugeDataWithWriteTimeByDateRangeInclusive;

    private PreparedStatement findAvailabilityByDateRangeInclusive;

    private PreparedStatement deleteGaugeMetric;

    private PreparedStatement findGaugeMetrics;

    private PreparedStatement insertGaugeTags;

    private PreparedStatement insertAvailabilityTags;

    private PreparedStatement updateDataWithTags;

    private PreparedStatement findGaugeDataByTag;

    private PreparedStatement findAvailabilityByTag;

    private PreparedStatement insertAvailability;

    private PreparedStatement findAvailabilities;

    private PreparedStatement updateMetricsIndex;

    private PreparedStatement addTagsToMetricsIndex;

    private PreparedStatement deleteTagsFromMetricsIndex;

    private PreparedStatement readMetricsIndex;

    private PreparedStatement findAvailabilitiesWithWriteTime;

    private PreparedStatement updateRetentionsIndex;

    private PreparedStatement findDataRetentions;

    private PreparedStatement insertMetricsTagsIndex;

    private PreparedStatement deleteMetricsTagsIndex;

    private PreparedStatement findMetricsByTagName;

    private PreparedStatement findMetricsByTagNameValue;

    public DataAccessImpl(Session session) {
        this.session = session;
        rxSession = new RxSessionImpl(session);
        initPreparedStatements();
    }

    protected void initPreparedStatements() {
        insertTenant = session.prepare(
            "INSERT INTO tenants (id, retentions) VALUES (?, ?) IF NOT EXISTS");

        findAllTenantIds = session.prepare("SELECT DISTINCT id FROM tenants");

        findTenant = session.prepare("SELECT id, retentions  FROM tenants WHERE id = ?");

        findMetric = session.prepare(
            "SELECT metric, interval, tags, data_retention " +
            "FROM metrics_idx " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ?");

        getMetricTags = session.prepare(
                "SELECT m_tags " +
                "FROM data " +
                "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ?");

        addMetricTagsToDataTable = session.prepare(
            "UPDATE data " +
            "SET m_tags = m_tags + ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ?");

        // TODO I am not sure if we want the m_tags and data_retention columns in the data table
        // Everything else in a partition will have a TTL set on it, so I fear that these columns
        // might cause problems with compaction.
        addMetadataAndDataRetention = session.prepare(
            "UPDATE data " +
            "SET m_tags = m_tags + ?, data_retention = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ?");

        deleteMetricTagsFromDataTable = session.prepare(
            "UPDATE data " +
            "SET m_tags = m_tags - ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ?");

        insertIntoMetricsIndex = session.prepare(
            "INSERT INTO metrics_idx (tenant_id, type, interval, metric, data_retention, tags) " +
            "VALUES (?, ?, ?, ?, ?, ?) " +
            "IF NOT EXISTS");

        updateMetricsIndex = session.prepare(
            "INSERT INTO metrics_idx (tenant_id, type, interval, metric) VALUES (?, ?, ?, ?)");

        addTagsToMetricsIndex = session.prepare(
            "UPDATE metrics_idx " +
            "SET tags = tags + ? " +
            "WHERE tenant_id = ? AND type = ? AND interval = ? AND metric = ?");

        deleteTagsFromMetricsIndex = session.prepare(
            "UPDATE metrics_idx " +
            "SET tags = tags - ?" +
            "WHERE tenant_id = ? AND type = ? AND interval = ? AND metric = ?");

        readMetricsIndex = session.prepare(
            "SELECT metric, interval, tags, data_retention " +
            "FROM metrics_idx " +
            "WHERE tenant_id = ? AND type = ?");

        insertGaugeData = session.prepare(
            "UPDATE data " +
            "USING TTL ?" +
            "SET m_tags = m_tags + ?, n_value = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time = ? ");

        insertCounterData = session.prepare(
            "UPDATE data " +
            "USING TTL ?" +
            "SET m_tags = m_tags + ?, l_value = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time = ? ");

        findGaugeDataByDateRangeExclusive = session.prepare(
            "SELECT time, m_tags, data_retention, n_value, tags FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ?"
                + " AND time < ?");

        findGaugeDataByDateRangeExclusiveASC = session.prepare(
            "SELECT time, m_tags, data_retention, n_value, tags FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ?" +
            " AND time < ? ORDER BY time ASC");

        findCounterDataExclusive = session.prepare(
            "SELECT time, m_tags, data_retention, l_value, tags FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ? " +
            "AND time < ?");

        findGaugeDataWithWriteTimeByDateRangeExclusive = session.prepare(
            "SELECT time, m_tags, data_retention, n_value, tags, WRITETIME(n_value) FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ?"
                + " AND time < ?");

        findGaugeDataByDateRangeInclusive = session.prepare(
            "SELECT tenant_id, metric, interval, dpart, time, m_tags, data_retention, n_value, tags " +
            "FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ?"
                + " AND time <= ?");

        findGaugeDataWithWriteTimeByDateRangeInclusive = session.prepare(
            "SELECT time, m_tags, data_retention, n_value, tags, WRITETIME(n_value) FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ?"
                + " AND time <= ?");

        findAvailabilityByDateRangeInclusive = session.prepare(
            "SELECT time, m_tags, data_retention, availability, tags, WRITETIME(availability) FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ?"
                + " AND time <= ?");

        deleteGaugeMetric = session.prepare(
            "DELETE FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ?");

        findGaugeMetrics = session.prepare(
            "SELECT DISTINCT tenant_id, type, metric, interval, dpart FROM data;");

        insertGaugeTags = session.prepare(
            "INSERT INTO tags (tenant_id, tname, tvalue, type, metric, interval, time, n_value) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
            "USING TTL ?");

        insertAvailabilityTags = session.prepare(
            "INSERT INTO tags (tenant_id, tname, tvalue, type, metric, interval, time, availability) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
            "USING TTL ?");

        updateDataWithTags = session.prepare(
            "UPDATE data " +
            "SET tags = tags + ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time = ?");

        findGaugeDataByTag = session.prepare(
            "SELECT tenant_id, tname, tvalue, type, metric, interval, time, n_value " +
            "FROM tags " +
            "WHERE tenant_id = ? AND tname = ? AND tvalue = ?");

        findAvailabilityByTag = session.prepare(
            "SELECT tenant_id, tname, tvalue, type, metric, interval, time, availability " +
            "FROM tags " +
            "WHERE tenant_id = ? AND tname = ? AND tvalue = ?");

        insertAvailability = session.prepare(
            "UPDATE data " +
            "USING TTL ? " +
            "SET m_tags = m_tags + ?, availability = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time = ?");

        findAvailabilities = session.prepare(
            "SELECT time, m_tags, data_retention, availability, tags FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ?"
                + " AND time < ? " +
            "ORDER BY time ASC");

        findAvailabilitiesWithWriteTime = session.prepare(
            "SELECT time, m_tags, data_retention, availability, tags, WRITETIME(availability) FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND interval = ? AND dpart = ? AND time >= ?" +
                    " AND time < ?");

        updateRetentionsIndex = session.prepare(
            "INSERT INTO retentions_idx (tenant_id, type, metric, retention) VALUES (?, ?, ?, ?)");

        findDataRetentions = session.prepare(
            "SELECT tenant_id, type, metric, retention " +
            "FROM retentions_idx " +
            "WHERE tenant_id = ? AND type = ?");

        insertMetricsTagsIndex = session.prepare(
            "INSERT INTO metrics_tags_idx (tenant_id, tname, tvalue, type, metric, interval) VALUES " +
            "(?, ?, ?, ?, ?, ?)");

        deleteMetricsTagsIndex = session.prepare(
            "DELETE FROM metrics_tags_idx " +
            "WHERE tenant_id = ? AND tname = ? AND tvalue = ? AND type = ? AND metric = ? AND interval = ?");

        findMetricsByTagName = session.prepare(
            "SELECT type, metric, interval, tvalue " +
            "FROM metrics_tags_idx " +
            "WHERE tenant_id = ? AND tname = ?");

        findMetricsByTagNameValue = session.prepare(
                "SELECT type, metric, interval " +
                        "FROM metrics_tags_idx " +
                        "WHERE tenant_id = ? AND tname = ? AND tvalue = ?");
    }

    @Override
    public Observable<ResultSet> insertTenant(Tenant tenant) {
        Map<String, Integer> retentions = tenant.getRetentionSettings().entrySet().stream()
                .collect(toMap(entry -> entry.getKey().getText(), Map.Entry::getValue));
        return rxSession.execute(insertTenant.bind(tenant.getId(), retentions));
    }

    @Override
    public Observable<ResultSet> findAllTenantIds() {
        return rxSession.execute(findAllTenantIds.bind());
    }

    @Override
    public Observable<ResultSet> findTenant(String id) {
        return rxSession.execute(findTenant.bind(id));
    }

    @Override
    public ResultSetFuture insertMetricInMetricsIndex(Metric metric) {
        return session.executeAsync(insertIntoMetricsIndex.bind(metric.getTenantId(), metric.getType().getCode(),
                metric.getId().getInterval().toString(), metric.getId().getName(), metric.getDataRetention(),
                metric.getTags()));
    }

    @Override
    public Observable<ResultSet> findMetric(MetricId id) {
        return rxSession.execute(findMetric.bind(id.getTenantId(), id.getType().getCode(), id.getName(),
                                                 id.getInterval().toString()));
    }

    @Override
    public Observable<ResultSet> getMetricTags(MetricId id, long dpart) {
        return rxSession.execute(getMetricTags.bind(id.getTenantId(), id.getType().getCode(), id.getName(),
                                                    id.getInterval().toString(), dpart));
    }

    // This method updates the metric tags and data retention in the data table. In the
    // long term after we add support for bucketing/date partitioning I am not sure that we
    // will store metric tags and data retention in the data table. We would have to
    // determine when we start writing data to a new partition, e.g., the start of the next
    // day, and then add the tags and retention to the new partition.
    @Override
    public Observable<ResultSet> addTagsAndDataRetention(Metric metric) {
        return rxSession.execute(addMetadataAndDataRetention.bind(metric.getTags(), metric.getDataRetention(),
                metric.getTenantId(), metric.getType().getCode(), metric.getId().getName(), metric.getId().getInterval()
                        .toString(), DPART));
    }

    @Override
    public Observable<ResultSet> addTags(Metric metric, Map<String, String> tags) {
        BatchStatement batch = new BatchStatement(UNLOGGED);
        batch.add(addMetricTagsToDataTable.bind(tags, metric.getTenantId(), metric.getType().getCode(),
            metric.getId().getName(), metric.getId().getInterval().toString(), DPART));
        batch.add(addTagsToMetricsIndex.bind(tags, metric.getTenantId(), metric.getType().getCode(),
            metric.getId().getInterval().toString(), metric.getId().getName()));
        return rxSession.execute(batch);
    }

    @Override
    public Observable<ResultSet> deleteTags(Metric metric, Set<String> tags) {
        BatchStatement batch = new BatchStatement(UNLOGGED);
        batch.add(deleteMetricTagsFromDataTable.bind(tags, metric.getTenantId(), metric.getType().getCode(),
            metric.getId().getName(), metric.getId().getInterval().toString(), DPART));
        batch.add(deleteTagsFromMetricsIndex.bind(tags, metric.getTenantId(), metric.getType().getCode(),
            metric.getId().getInterval().toString(), metric.getId().getName()));
        return rxSession.execute(batch);
    }

    @Override
    public Observable<ResultSet> updateTagsInMetricsIndex(Metric metric, Map<String, String> additions,
            Set<String> deletions) {
        BatchStatement batchStatement = new BatchStatement(UNLOGGED)
            .add(addTagsToMetricsIndex.bind(additions, metric.getTenantId(),
                metric.getType().getCode(), metric.getId().getInterval().toString(), metric.getId().getName()))
            .add(deleteTagsFromMetricsIndex.bind(deletions, metric.getTenantId(), metric.getType().getCode(),
                    metric.getId().getInterval().toString(), metric.getId().getName()));
        return rxSession.execute(batchStatement);
    }

    @Override
    public <T> ResultSetFuture updateMetricsIndex(List<Metric<T>> metrics) {
        BatchStatement batchStatement = new BatchStatement(UNLOGGED);
        for (Metric metric : metrics) {
            batchStatement.add(updateMetricsIndex.bind(metric.getTenantId(), metric.getType().getCode(),
                    metric.getId().getInterval().toString(), metric.getId().getName()));
        }
        return session.executeAsync(batchStatement);
    }

    @Override
    public <T> Observable<Integer> updateMetricsIndexRx(Observable<Metric<T>> metrics) {
        return metrics.reduce(new BatchStatement(UNLOGGED), (batch, metric) -> {
            batch.add(updateMetricsIndex.bind(metric.getTenantId(), metric.getType().getCode(),
                    metric.getId().getInterval().toString(), metric.getId().getName()));
            return batch;
        }).flatMap(batch -> rxSession.execute(batch).map(resultSet -> batch.size()));
    }

    @Override
    public Observable<ResultSet> findMetricsInMetricsIndex(String tenantId, MetricType type) {
        return rxSession.execute(readMetricsIndex.bind(tenantId, type.getCode()));
    }

    @Override
    public Observable<Integer> insertData(Metric<Double> gauge, int ttl) {
        return Observable.from(gauge.getDataPoints())
                .map(dataPoint -> bindDataPoint(insertGaugeData, gauge, dataPoint, ttl))
                .reduce(new BatchStatement(UNLOGGED), BatchStatement::add)
                .flatMap(batch -> rxSession.execute(batch).map(resultSet -> batch.size()));
    }

    @Override
    public Observable<Integer> insertCounterData(Metric<Long> counter, int ttl) {
        return Observable.from(counter.getDataPoints())
                .map(dataPoint -> bindDataPoint(insertCounterData, counter, dataPoint, ttl))
                .reduce(new BatchStatement(UNLOGGED), BatchStatement::add)
                .flatMap(batch -> rxSession.execute(batch).map(resultSet -> batch.size()));
    }

    private BoundStatement bindDataPoint(PreparedStatement statement, Metric<?> metric, DataPoint<?> dataPoint,
            int ttl) {
        return statement.bind(ttl, metric.getTags(), dataPoint.getValue(), metric.getTenantId(),
                metric.getType().getCode(), metric.getId().getName(), metric.getId().getInterval().toString(), DPART,
                getTimeUUID(dataPoint.getTimestamp()));
    }

    @Override
    public Observable<ResultSet> findData(MetricId id, long startTime, long endTime) {
        return findData(id, startTime, endTime, false);
    }

    @Override
    public Observable<ResultSet> findCounterData(MetricId id, long startTime, long endTime) {
        return rxSession.execute(findCounterDataExclusive.bind(id.getTenantId(), COUNTER.getCode(), id.getName(),
                id.getInterval().toString(), DPART, getTimeUUID(startTime), getTimeUUID(endTime)));
    }

    @Override
    public Observable<ResultSet> findData(Metric<Double> metric, long startTime, long endTime, Order
            order) {
        if (order == Order.ASC) {
            return rxSession.execute(findGaugeDataByDateRangeExclusiveASC.bind(metric.getTenantId(),
                    MetricType.GAUGE.getCode(), metric.getId().getName(), metric.getId().getInterval().toString(),
                    DPART, getTimeUUID(startTime), getTimeUUID(endTime)));
        } else {
            return rxSession.execute(findGaugeDataByDateRangeExclusive.bind(metric.getTenantId(),
                    MetricType.GAUGE.getCode(), metric.getId().getName(), metric.getId().getInterval().toString(),
                    DPART, getTimeUUID(startTime), getTimeUUID(endTime)));
        }
    }

    @Override
    public Observable<ResultSet> findData(MetricId id, long startTime, long endTime,
            boolean includeWriteTime) {
        if (includeWriteTime) {
            return rxSession.execute(findGaugeDataWithWriteTimeByDateRangeExclusive.bind(id.getTenantId(),
                    id.getType().getCode(), id.getName(),
                    id.getInterval().toString(), DPART, getTimeUUID(startTime), getTimeUUID(endTime)));
        } else {
            return rxSession.execute(findGaugeDataByDateRangeExclusive.bind(id.getTenantId(),
                                     id.getType().getCode(), id.getName(),
                                     id.getInterval().toString(), DPART, getTimeUUID(startTime), getTimeUUID(endTime)));
        }
    }

    @Override
    public Observable<ResultSet> findData(Metric<Double> metric, long timestamp,
            boolean includeWriteTime) {
        if (includeWriteTime) {
            return rxSession.execute(findGaugeDataWithWriteTimeByDateRangeInclusive.bind(metric.getTenantId(),
                    metric.getType().getCode(), metric.getId().getName(), metric.getId().getInterval().toString(),
                    DPART, UUIDs.startOf(timestamp), UUIDs.endOf(timestamp)));
        } else {
            return rxSession.execute(findGaugeDataByDateRangeInclusive.bind(metric.getTenantId(),
                    metric.getType().getCode(), metric.getId().getName(), metric.getId().getInterval().toString(),
                    DPART, UUIDs.startOf(timestamp), UUIDs.endOf(timestamp)));
        }
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(Metric<AvailabilityType> metric, long startTime, long
            endTime) {
        return findAvailabilityData(metric, startTime, endTime, false);
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(Metric<AvailabilityType> metric, long startTime, long
            endTime, boolean
            includeWriteTime) {
        if (includeWriteTime) {
            return rxSession.execute(findAvailabilitiesWithWriteTime.bind(metric.getTenantId(),
                    MetricType.AVAILABILITY.getCode(), metric.getId().getName(),
                    metric.getId().getInterval().toString(), DPART, getTimeUUID(startTime), getTimeUUID(endTime)));
        } else {
            return rxSession.execute(findAvailabilities.bind(metric.getTenantId(), MetricType.AVAILABILITY.getCode(),
                    metric.getId().getName(), metric.getId().getInterval().toString(), DPART, getTimeUUID(startTime),
                    getTimeUUID(endTime)));
        }
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(Metric<AvailabilityType> metric, long timestamp) {
        return rxSession.execute(findAvailabilityByDateRangeInclusive.bind(metric.getTenantId(),
                MetricType.AVAILABILITY.getCode(), metric.getId().getName(), metric.getId().getInterval().toString(),
                DPART, UUIDs.startOf(timestamp), UUIDs.endOf(timestamp)));
    }

    @Override
    public Observable<ResultSet> deleteGaugeMetric(String tenantId, String metric, Interval interval, long dpart) {
        return rxSession.execute(deleteGaugeMetric.bind(tenantId, MetricType.GAUGE.getCode(), metric,
                interval.toString(), dpart));
    }

    @Override
    public Observable<ResultSet> findAllGaugeMetrics() {
        return rxSession.execute(findGaugeMetrics.bind());
    }

    @Override
    public Observable<ResultSet> insertGaugeTag(String tag, String tagValue, Metric<Double> metric,
            Observable<TTLDataPoint<Double>> data) {
        return data.reduce(
            new BatchStatement(UNLOGGED),
            (batch, d) -> {
                batch.add(insertGaugeTags.bind(metric.getTenantId(), tag, tagValue, MetricType.GAUGE.getCode(), metric
                    .getId().getName(), metric.getId().getInterval().toString(),
                        getTimeUUID(d.getDataPoint().getTimestamp()), d.getDataPoint().getValue(), d.getTTL()));
                return batch;
            }).flatMap(rxSession::execute);
    }

    @Override
    public Observable<ResultSet> insertAvailabilityTag(String tag, String tagValue,
            Metric<AvailabilityType> metric, Observable<TTLDataPoint<AvailabilityType>> data) {
        return data.reduce(
                new BatchStatement(UNLOGGED),
                (batch, a) -> {
                    batch.add(insertAvailabilityTags.bind(metric.getTenantId(), tag, tagValue,
                            MetricType.AVAILABILITY.getCode(), metric.getId().getName(),
                            metric.getId().getInterval().toString(), getTimeUUID(a.getDataPoint().getTimestamp()),
                            getBytes(a.getDataPoint()), a.getTTL()));
                    return batch;
                }).flatMap(rxSession::execute);
    }

    @Override
    public Observable<ResultSet> updateDataWithTag(Metric metric, DataPoint dataPoint, Map<String, String> tags) {
        return rxSession.execute(updateDataWithTags.bind(tags, metric.getTenantId(), metric.getType().getCode(), metric
                        .getId().getName(), metric.getId().getInterval().toString(), DPART,
                getTimeUUID(dataPoint.getTimestamp())));
    }

    @Override
    public Observable<ResultSet> findGaugeDataByTag(String tenantId, String tag, String tagValue) {
        return rxSession.execute(findGaugeDataByTag.bind(tenantId, tag, tagValue));
    }

    @Override
    public Observable<ResultSet> findAvailabilityByTag(String tenantId, String tag, String tagValue) {
        return rxSession.execute(findAvailabilityByTag.bind(tenantId, tag, tagValue));
    }

    @Override
    public Observable<Integer> insertAvailabilityData(Metric<AvailabilityType> metric, int ttl) {
        return Observable.from(metric.getDataPoints())
                .reduce(new BatchStatement(UNLOGGED), (batchStatement, a) -> {
                    batchStatement.add(insertAvailability.bind(ttl, metric.getTags(), getBytes(a),
                        metric.getTenantId(), metric.getType().getCode(), metric.getId().getName(), metric.getId()
                            .getInterval().toString(), DPART, getTimeUUID(a.getTimestamp())));
                    return batchStatement;
                })
                .flatMap(batch -> rxSession.execute(batch).map(resultSet -> batch.size()));
    }

    private ByteBuffer getBytes(DataPoint<AvailabilityType> dataPoint) {
        return ByteBuffer.wrap(new byte[]{dataPoint.getValue().getCode()});
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(MetricId id, long startTime, long endTime) {
        return rxSession.execute(findAvailabilities.bind(id.getTenantId(), MetricType.AVAILABILITY.getCode(),
                id.getName(), id.getInterval().toString(), DPART, getTimeUUID(startTime), getTimeUUID(endTime)));
    }

    @Override
    public ResultSetFuture findDataRetentions(String tenantId, MetricType type) {
        return session.executeAsync(findDataRetentions.bind(tenantId, type.getCode()));
    }

    @Override
    public Observable<ResultSet> updateRetentionsIndex(String tenantId, MetricType type,
            Map<String, Integer> retentions) {

        return Observable.from(retentions.entrySet())
                .reduce(new BatchStatement(UNLOGGED), (batchStatement, entry) -> {
                    batchStatement.add(updateRetentionsIndex.bind(tenantId, type.getCode(), entry.getKey(),
                            entry.getValue()));
                    return batchStatement;
                })
                .flatMap(rxSession::execute);
    }

    @Override
    public Observable<ResultSet> insertIntoMetricsTagsIndex(Metric metric, Map<String, String> tags) {
        return executeTagsBatch(tags, (name, value) -> insertMetricsTagsIndex.bind(metric.getTenantId(), name, value,
                metric.getType().getCode(), metric.getId().getName(), metric.getId().getInterval().toString()));
    }

    @Override
    public Observable<ResultSet> deleteFromMetricsTagsIndex(Metric metric, Map<String, String> tags) {
        return executeTagsBatch(tags, (name, value) -> deleteMetricsTagsIndex.bind(metric.getTenantId(), name, value,
                metric.getType().getCode(), metric.getId().getName(), metric.getId().getInterval().toString()));
    }

    private Observable<ResultSet> executeTagsBatch(Map<String, String> tags,
                                                   BiFunction<String, String, BoundStatement> bindVars) {
        BatchStatement batchStatement = new BatchStatement(UNLOGGED);
        tags.entrySet().stream().forEach(entry -> batchStatement.add(bindVars.apply(entry.getKey(), entry.getValue())));
        return rxSession.execute(batchStatement);
    }

    @Override
    public Observable<ResultSet> findMetricsByTagName(String tenantId, String tag) {
        return rxSession.execute(findMetricsByTagName.bind(tenantId, tag));
    }

    @Override
    public Observable<ResultSet> findMetricsByTagNameValue(String tenantId, String tag, String tvalue) {
        return rxSession.execute(findMetricsByTagNameValue.bind(tenantId, tag, tvalue));
    }

    @Override
    public ResultSetFuture updateRetentionsIndex(Metric metric) {
        return session.executeAsync(updateRetentionsIndex.bind(metric.getId().getTenantId(),
                metric.getId().getType().getCode(), metric.getId().getName(), metric.getDataRetention()));
    }

    private KeyspaceMetadata getKeyspace() {
        return session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
    }

}
