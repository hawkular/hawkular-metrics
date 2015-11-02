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

import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;
import static org.hawkular.metrics.core.impl.TimeUUIDUtils.getTimeUUID;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.core.impl.transformers.BatchStatementTransformer;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.joda.time.Duration;

import com.datastax.driver.core.BoundStatement;
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

    private Session session;

    private RxSession rxSession;

    private CacheContainer cacheContainer;

    private DateTimeService dateTimeService;

    private Cache<MetricId<?>, Duration> bucketSizeCache;

    private PreparedStatement insertTenant;

    private PreparedStatement insertTenantId;

    private PreparedStatement findAllTenantIds;

    private PreparedStatement findAllTenantIdsFromMetricsIdx;

    private PreparedStatement insertTenantIdIntoBucket;

    private PreparedStatement findTenantIdsByTime;

    private PreparedStatement findTenant;

    private PreparedStatement deleteTenantsBucket;

    private PreparedStatement insertIntoMetricsIndex;

    private PreparedStatement findBucketSize;

    private PreparedStatement findMetric;

    private PreparedStatement getMetricTags;

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

    public DataAccessImpl(Session session, CacheContainer cacheContainer) {
        this.session = session;
        this.dateTimeService = new DateTimeService();
        rxSession = new RxSessionImpl(session);
        initPreparedStatements();
        this.cacheContainer = cacheContainer;
        this.bucketSizeCache = cacheContainer.getCache("bucket_size");
    }

    protected void initPreparedStatements() {
        insertTenantId = session.prepare("INSERT INTO tenants (id) VALUES (?)");

        insertTenant = session.prepare(
            "INSERT INTO tenants (id, retentions) VALUES (?, ?) IF NOT EXISTS");

        findAllTenantIds = session.prepare("SELECT DISTINCT id FROM tenants");

        findAllTenantIdsFromMetricsIdx = session.prepare("SELECT DISTINCT tenant_id, type FROM metrics_idx");

        findTenantIdsByTime = session.prepare("SELECT tenant FROM tenants_by_time WHERE bucket = ?");

        insertTenantIdIntoBucket = session.prepare("INSERT into tenants_by_time (bucket, tenant) VALUES (?, ?)");

        findTenant = session.prepare("SELECT id, retentions FROM tenants WHERE id = ?");

        deleteTenantsBucket = session.prepare("DELETE FROM tenants_by_time WHERE bucket = ?");

        findBucketSize = session.prepare(
                "SELECT bucket_size " +
                        "FROM metrics_idx " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ?");

        findMetric = session.prepare(
            "SELECT metric, tags, data_retention, bucket_size " +
            "FROM metrics_idx " +
            "WHERE tenant_id = ? AND type = ? AND metric = ?");

        getMetricTags = session.prepare(
            "SELECT tags " +
            "FROM metrics_idx " +
            "WHERE tenant_id = ? AND type = ? AND metric = ?");

        insertIntoMetricsIndex = session.prepare(
                "INSERT INTO metrics_idx (tenant_id, type, metric, data_retention, bucket_size, tags) " +
                        "VALUES (?, ?, ?, ?, ?, ?) " +
                        "IF NOT EXISTS");

        updateMetricsIndex = session.prepare(
            "INSERT INTO metrics_idx (tenant_id, type, metric) VALUES (?, ?, ?)");

        addTagsToMetricsIndex = session.prepare(
            "UPDATE metrics_idx " +
            "SET tags = tags + ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ?");

        deleteTagsFromMetricsIndex = session.prepare(
            "UPDATE metrics_idx " +
            "SET tags = tags - ?" +
            "WHERE tenant_id = ? AND type = ? AND metric = ?");

        readMetricsIndex = session.prepare(
            "SELECT metric, tags, data_retention, bucket_size " +
            "FROM metrics_idx " +
            "WHERE tenant_id = ? AND type = ?");

        insertGaugeData = session.prepare(
            "UPDATE data " +
            "SET n_value = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ? ");

        insertCounterData = session.prepare(
            "UPDATE data " +
            "SET l_value = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ? ");

        findGaugeDataByDateRangeExclusive = session.prepare(
            "SELECT time, n_value FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ?");

        findGaugeDataByDateRangeExclusiveASC = session.prepare(
            "SELECT time, n_value FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ?" +
            " AND time < ? ORDER BY time ASC");

        findCounterDataExclusive = session.prepare(
            "SELECT time, l_value FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ? " +
            "ORDER BY time ASC");

        findGaugeDataWithWriteTimeByDateRangeExclusive = session.prepare(
            "SELECT time, n_value, WRITETIME(n_value) FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ?");

        findGaugeDataByDateRangeInclusive = session.prepare(
            "SELECT tenant_id, metric, dpart, time, n_value " +
            "FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time <= ?");

        findGaugeDataWithWriteTimeByDateRangeInclusive = session.prepare(
            "SELECT time, n_value, WRITETIME(n_value) FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time <= ?");

        findAvailabilityByDateRangeInclusive = session.prepare(
            "SELECT time, availability, WRITETIME(availability) FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time <= ?");

        deleteGaugeMetric = session.prepare(
            "DELETE FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ?");

        findGaugeMetrics = session.prepare(
            "SELECT DISTINCT tenant_id, type, metric, dpart FROM data;");

        insertAvailability = session.prepare(
            "UPDATE data " +
            "SET availability = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ?");

        findAvailabilities = session.prepare(
            "SELECT time, availability " +
            "FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ?"
                + " AND time < ? " +
            "ORDER BY time ASC");

        findAvailabilitiesWithWriteTime = session.prepare(
            "SELECT time, availability, WRITETIME(availability) " +
            "FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ?");

        updateRetentionsIndex = session.prepare(
            "INSERT INTO retentions_idx (tenant_id, type, metric, retention) VALUES (?, ?, ?, ?)");

        findDataRetentions = session.prepare(
            "SELECT tenant_id, type, metric, retention " +
            "FROM retentions_idx " +
            "WHERE tenant_id = ? AND type = ?");

        insertMetricsTagsIndex = session.prepare(
            "INSERT INTO metrics_tags_idx (tenant_id, tname, tvalue, type, metric) VALUES (?, ?, ?, ?, ?)");

        deleteMetricsTagsIndex = session.prepare(
            "DELETE FROM metrics_tags_idx " +
            "WHERE tenant_id = ? AND tname = ? AND tvalue = ? AND type = ? AND metric = ?");

        findMetricsByTagName = session.prepare(
            "SELECT type, metric, tvalue " +
            "FROM metrics_tags_idx " +
            "WHERE tenant_id = ? AND tname = ?");

        findMetricsByTagNameValue = session.prepare(
                "SELECT tenant_id, type, metric " +
                "FROM metrics_tags_idx " +
                "WHERE tenant_id = ? AND tname = ? AND tvalue = ?");
    }

    @Override public Observable<ResultSet> insertTenant(String tenantId) {
        return rxSession.execute(insertTenantId.bind(tenantId));
    }

    @Override
    public Observable<ResultSet> insertTenant(Tenant tenant) {
        Map<String, Integer> retentions = tenant.getRetentionSettings().entrySet().stream()
                .collect(toMap(entry -> entry.getKey().getText(), Map.Entry::getValue));
        return rxSession.execute(insertTenant.bind(tenant.getId(), retentions));
    }

    @Override
    public Observable<ResultSet> findAllTenantIds() {
        return rxSession.execute(findAllTenantIds.bind())
                .concatWith(rxSession.execute(findAllTenantIdsFromMetricsIdx.bind()));
    }

    @Override public Observable<ResultSet> findTenantIds(long time) {
        return rxSession.execute(findTenantIdsByTime.bind(new Date(time)));
    }

    @Override public Observable<ResultSet> insertTenantId(long time, String id) {
        return rxSession.execute(insertTenantIdIntoBucket.bind(new Date(time), id));
    }

    @Override
    public Observable<ResultSet> findTenant(String id) {
        return rxSession.execute(findTenant.bind(id));
    }

    @Override public Observable<ResultSet> deleteTenantsBucket(long time) {
        return rxSession.execute(deleteTenantsBucket.bind(new Date(time)));
    }

    @Override
    public <T> Observable<ResultSet> insertMetricInMetricsIndex(Metric<T> metric) {
        MetricId<T> metricId = metric.getId();
        return rxSession.execute(insertIntoMetricsIndex.bind(metricId.getTenantId(), metricId.getType().getCode(),
                metricId.getName(), metric.getDataRetention(), metric.getBucketSize(), metric.getTags()))
                .doOnCompleted(() -> {
                    if(metric.getBucketSize() == null) {
                        this.bucketSizeCache.putAsync(metricId, DateTimeService.DEFAULT_SLICE);
                    } else {
                        this.bucketSizeCache.putAsync(metricId, Duration.standardSeconds(metric.getBucketSize()));
                    }
                });
    }

    @Override
    public <T> Observable<ResultSet> findMetric(MetricId<T> id) {
        return rxSession.execute(findMetric.bind(id.getTenantId(), id.getType().getCode(), id.getName()));
    }

    @Override
    public <T> Observable<ResultSet> getMetricTags(MetricId<T> id) {
        return rxSession.execute(getMetricTags.bind(id.getTenantId(), id.getType().getCode(), id.getName()));
    }

    @Override
    public <T> Observable<ResultSet> addTags(Metric<T> metric, Map<String, String> tags) {
        MetricId<T> metricId = metric.getId();
        BoundStatement stmt = addTagsToMetricsIndex.bind(tags, metricId.getTenantId(), metricId.getType().getCode(),
                metricId.getName());
        return rxSession.execute(stmt);
    }

    @Override
    public <T> Observable<ResultSet> deleteTags(Metric<T> metric, Set<String> tags) {
        MetricId<T> metricId = metric.getId();
        BoundStatement stmt = deleteTagsFromMetricsIndex.bind(tags, metricId.getTenantId(),
                metricId.getType().getCode(), metricId.getName());
        return rxSession.execute(stmt);
    }

    @Override
    public <T> Observable<Integer> updateMetricsIndex(Observable<Metric<T>> metrics) {
        return metrics.map(Metric::getId)
                .map(id -> updateMetricsIndex.bind(id.getTenantId(), id.getType().getCode(), id.getName()))
                .compose(new BatchStatementTransformer())
                .flatMap(batch -> rxSession.execute(batch).map(resultSet -> batch.size()));
    }

    @Override
    public <T> Observable<ResultSet> findMetricsInMetricsIndex(String tenantId, MetricType<T> type) {
        return rxSession.execute(readMetricsIndex.bind(tenantId, type.getCode()));
    }

    @Override
    public Observable<Integer> insertGaugeData(Metric<Double> gauge) {
        return Observable.from(gauge.getDataPoints())
                .flatMap(dataPoint -> bindDataPoint(insertGaugeData, gauge, dataPoint.getValue(),
                        dataPoint.getTimestamp()))
                .compose(new BatchStatementTransformer())
                .flatMap(batch -> rxSession.execute(batch).map(resultSet -> batch.size()));
    }

    @Override
    public Observable<Integer> insertCounterData(Metric<Long> counter) {
        return Observable.from(counter.getDataPoints())
                .flatMap(dataPoint -> bindDataPoint(insertCounterData, counter, dataPoint.getValue(),
                        dataPoint.getTimestamp()))
                .compose(new BatchStatementTransformer())
                .flatMap(batch -> rxSession.execute(batch).map(resultSet -> batch.size()));
    }

    private Observable<BoundStatement> bindDataPoint(PreparedStatement statement, Metric<?> metric, Object value,
            long timestamp) {
        MetricId<?> metricId = metric.getId();

        return getBucket(metricId, timestamp)
                .map(bucket -> statement.bind(value, metricId.getTenantId(), metricId.getType().getCode(),
                        metricId.getName(), bucket, getTimeUUID(timestamp)));
    }

    @Override
    public Observable<ResultSet> findGaugeData(MetricId<Double> id, long startTime, long endTime) {
        return findGaugeData(id, startTime, endTime, false);
    }

    @Override
    public Observable<ResultSet> findCounterData(MetricId<Long> id, long startTime, long endTime) {
        return getBuckets(id, startTime, endTime)
                .concatMap(b -> rxSession.execute(findCounterDataExclusive.bind(id.getTenantId(), COUNTER.getCode(), id
                        .getName(), b, getTimeUUID(startTime), getTimeUUID(endTime))));
    }

    @Override
    public Observable<ResultSet> findGaugeData(Metric<Double> metric, long startTime, long endTime, Order
            order) {
        MetricId<?> metricId = metric.getId();
        if (order == Order.ASC) {
            return getBuckets(metricId, startTime, endTime)
                    .concatMap(b -> rxSession.execute(findGaugeDataByDateRangeExclusiveASC.bind(metricId.getTenantId(),
                            GAUGE.getCode(), metricId.getName(), b, getTimeUUID(startTime), getTimeUUID(endTime))));
        } else {
            return getBuckets(metricId, startTime, endTime)
                    .concatMap(b -> rxSession.execute(findGaugeDataByDateRangeExclusive.bind(metricId.getTenantId(),
                            GAUGE.getCode(), metricId.getName(), b, getTimeUUID(startTime), getTimeUUID(endTime))));
        }
    }

    @Override
    public Observable<ResultSet> findGaugeData(MetricId<Double> id, long startTime, long endTime,
            boolean includeWriteTime) {
        if (includeWriteTime) {
            return getBuckets(id, startTime, endTime)
                    .concatMap(b -> rxSession.execute(findGaugeDataWithWriteTimeByDateRangeExclusive.bind(
                            id.getTenantId(), id.getType().getCode(), id.getName(), b, getTimeUUID(startTime),
                            getTimeUUID(endTime))));
        } else {
            return getBuckets(id, startTime, endTime)
                    .concatMap(b -> rxSession.execute(findGaugeDataByDateRangeExclusive.bind(id.getTenantId(),
                            id.getType().getCode(), id.getName(), b, getTimeUUID(startTime), getTimeUUID(endTime))));
        }
    }

    @Override
    public Observable<ResultSet> findGaugeData(Metric<Double> metric, long timestamp,
                                               boolean includeWriteTime) {
        MetricId<?> metricId = metric.getId();
        if (includeWriteTime) {
            return getBucket(metricId, timestamp)
                    .concatMap(b -> rxSession.execute(findGaugeDataWithWriteTimeByDateRangeInclusive.bind(metricId
                                    .getTenantId(), metricId.getType().getCode(), metricId.getName(), b,
                            UUIDs.startOf(timestamp), UUIDs.endOf(timestamp))));
        } else {
            return getBucket(metricId, timestamp)
                    .concatMap(b -> rxSession.execute(findGaugeDataByDateRangeInclusive.bind(metricId.getTenantId(),
                            metricId.getType().getCode(), metricId.getName(), b, UUIDs.startOf(timestamp),
                            UUIDs.endOf(timestamp))));
        }
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(Metric<AvailabilityType> metric, long startTime, long
            endTime) {
        return findAvailabilityData(metric, startTime, endTime, false);
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(Metric<AvailabilityType> metric, long startTime, long
            endTime, boolean includeWriteTime) {
        MetricId<?> metricId = metric.getId();
        if (includeWriteTime) {
            return getBuckets(metricId, startTime, endTime)
                    .concatMap(b -> rxSession.execute(findAvailabilitiesWithWriteTime.bind(metricId.getTenantId(),
                            AVAILABILITY.getCode(), metricId.getName(), b, getTimeUUID(startTime),
                            getTimeUUID(endTime))));
        } else {
            return getBuckets(metricId, startTime, endTime)
                    .concatMap(b -> rxSession.execute(findAvailabilities.bind(metricId.getTenantId(),
                            AVAILABILITY.getCode(), metricId.getName(), b, getTimeUUID(startTime),
                            getTimeUUID(endTime))));
        }
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(Metric<AvailabilityType> metric, long timestamp) {
        MetricId<?> metricId = metric.getId();
        return getBucket(metricId, timestamp)
                .concatMap(b -> rxSession.execute(findAvailabilityByDateRangeInclusive.bind(metricId.getTenantId(),
                        AVAILABILITY.getCode(), metricId.getName(), b, UUIDs.startOf
                                (timestamp), UUIDs.endOf(timestamp))));
    }

//    @Override
//    public Observable<ResultSet> deleteGaugeMetric(String tenantId, String metric, Interval interval, long dpart) {
//        return rxSession.execute(deleteGaugeMetric.bind(tenantId, GAUGE.getCode(), metric,
//                interval.toString(), dpart));
//    }

    @Override
    public Observable<ResultSet> findAllGaugeMetrics() {
        return rxSession.execute(findGaugeMetrics.bind());
    }

    @Override
    public Observable<Integer> insertAvailabilityData(Metric<AvailabilityType> metric) {
        return Observable.from(metric.getDataPoints())
                .flatMap(dataPoint -> bindDataPoint(insertAvailability, metric, getBytes(dataPoint),
                        dataPoint.getTimestamp()))
                .compose(new BatchStatementTransformer())
                .flatMap(batch -> rxSession.execute(batch).map(resultSet -> batch.size()));
    }

    private ByteBuffer getBytes(DataPoint<AvailabilityType> dataPoint) {
        return ByteBuffer.wrap(new byte[]{dataPoint.getValue().getCode()});
    }

    @Override
    public Observable<ResultSet> findAvailabilityData(MetricId<AvailabilityType> id, long startTime, long endTime) {
        return getBuckets(id, startTime, endTime)
                .concatMap(b -> rxSession.execute(findAvailabilities.bind(id.getTenantId(), AVAILABILITY.getCode(),
                        id.getName(), b, getTimeUUID(startTime), getTimeUUID(endTime))));
    }

    @Override
    public <T> ResultSetFuture findDataRetentions(String tenantId, MetricType<T> type) {
        return session.executeAsync(findDataRetentions.bind(tenantId, type.getCode()));
    }

    @Override
    public <T> Observable<ResultSet> updateRetentionsIndex(String tenantId, MetricType<T> type,
                                                       Map<String, Integer> retentions) {
        return Observable.from(retentions.entrySet())
                .map(entry -> updateRetentionsIndex.bind(tenantId, type.getCode(), entry.getKey(), entry.getValue()))
                .compose(new BatchStatementTransformer())
                .flatMap(rxSession::execute);
    }

    @Override
    public <T> Observable<ResultSet> insertIntoMetricsTagsIndex(Metric<T> metric, Map<String, String> tags) {
        MetricId<T> metricId = metric.getId();
        return tagsUpdates(tags, (name, value) -> insertMetricsTagsIndex.bind(metricId.getTenantId(), name, value,
                metricId.getType().getCode(), metricId.getName()));
    }

    @Override
    public <T> Observable<ResultSet> deleteFromMetricsTagsIndex(Metric<T> metric, Map<String, String> tags) {
        MetricId<T> metricId = metric.getId();
        return tagsUpdates(tags, (name, value) -> deleteMetricsTagsIndex.bind(metricId.getTenantId(), name, value,
                metricId.getType().getCode(), metricId.getName()));
    }

    private Observable<ResultSet> tagsUpdates(Map<String, String> tags,
                                              BiFunction<String, String, BoundStatement> bindVars) {
        return Observable.from(tags.entrySet())
                .map(entry -> bindVars.apply(entry.getKey(), entry.getValue()))
                .flatMap(rxSession::execute);
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
    public <T> ResultSetFuture updateRetentionsIndex(Metric<T> metric) {
        return session.executeAsync(updateRetentionsIndex.bind(metric.getId().getTenantId(),
                metric.getId().getType().getCode(), metric.getId().getName(), metric.getDataRetention()));
    }

    private Observable<Long> getBucket(MetricId id, long timestamp) {
        return getBucketSize(id)
                .map(b -> dateTimeService.getCurrentDpart(timestamp, b));
    }

    private Observable<Long> getBuckets(MetricId id, long startTime, long endTime) {
        return getBuckets(id, startTime, endTime, false);
    }

    private Observable<Long> getBuckets(MetricId id, long startTime, long endTime, boolean reverse) {
        // TODO These have to take into account the storage time, otherwise we might do fetches to a bucket
        //      that does not exist anymore
        Comparator<Long> sortComparator = reverse ? Comparator.reverseOrder() : Comparator.naturalOrder();

        Observable<Stream<Long>> longStream = getBucketSize(id)
                .map(b -> dateTimeService.getDparts(startTime, endTime, b))
                .map(dparts -> Arrays.stream(dparts).mapToObj(Long::valueOf).sorted(sortComparator));

        return longStream.concatMap(d -> Observable.from(d::iterator));
    }

    private Observable<Duration> getBucketSize(MetricId<?> id) {
        Duration slice = bucketSizeCache.get(id);
        if(slice == null) {
            Observable<Duration> cache = getBucketSizeFromCassandra(id)
                    .cache();
            cache.subscribe(d -> bucketSizeCache.putAsync(id, d));
            return cache;
        }
        return Observable.just(slice);
    }

    private Observable<Duration> getBucketSizeFromCassandra(MetricId<?> id) {
        return rxSession.execute(findBucketSize.bind(id.getTenantId(), id.getType().getCode(), id.getName()))
                .flatMap(Observable::from)
                .map(row -> (row.getInt(0) <= 0) ? DateTimeService.DEFAULT_SLICE : Duration.standardSeconds(row
                        .getInt(0)))
                .switchIfEmpty(Observable.just(DateTimeService.DEFAULT_SLICE));
    }
}
