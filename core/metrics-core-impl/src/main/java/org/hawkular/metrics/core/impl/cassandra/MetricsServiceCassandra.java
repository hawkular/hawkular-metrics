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
package org.hawkular.metrics.core.impl.cassandra;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.joda.time.Hours.hours;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityBucketDataPoint;
import org.hawkular.metrics.core.api.AvailabilityData;
import org.hawkular.metrics.core.api.BucketedOutput;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.Counter;
import org.hawkular.metrics.core.api.Gauge;
import org.hawkular.metrics.core.api.GaugeBucketDataPoint;
import org.hawkular.metrics.core.api.GaugeData;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricData;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.MetricsThreadFactory;
import org.hawkular.metrics.core.api.Retention;
import org.hawkular.metrics.core.api.RetentionSettings;
import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.core.api.TenantAlreadyExistsException;
import org.joda.time.Duration;
import org.joda.time.Hours;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * @author John Sanda
 */
public class MetricsServiceCassandra implements MetricsService {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceCassandra.class);

    public static final String REQUEST_LIMIT = "hawkular.metrics.request.limit";

    public static final int DEFAULT_TTL = Duration.standardDays(7).toStandardSeconds().getSeconds();

    public volatile State state;

    private static class DataRetentionKey {
        private final String tenantId;
        private final MetricId metricId;
        private final MetricType type;

        public DataRetentionKey(String tenantId, MetricType type) {
            this.tenantId = tenantId;
            this.type = type;
            metricId = new MetricId("[" + type.getText() + "]");
        }

        public DataRetentionKey(String tenantId, MetricId metricId, MetricType type) {
            this.tenantId = tenantId;
            this.metricId = metricId;
            this.type = type;
        }

        public DataRetentionKey(Metric metric) {
            this.tenantId = metric.getTenantId();
            this.metricId = metric.getId();
            this.type = metric.getType();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DataRetentionKey that = (DataRetentionKey) o;

            if (!metricId.equals(that.metricId)) return false;
            if (!tenantId.equals(that.tenantId)) return false;
            return type == that.type;

        }

        @Override
        public int hashCode() {
            int result = tenantId.hashCode();
            result = 31 * result + metricId.hashCode();
            result = 31 * result + type.hashCode();
            return result;
        }
    }

    private final RateLimiter permits = RateLimiter.create(Double.parseDouble(
        System.getProperty(REQUEST_LIMIT, "30000")), 3, TimeUnit.MINUTES);

    private DataAccess dataAccess;

    private final ListeningExecutorService metricsTasks = MoreExecutors
        .listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));

    /**
     * Note that while user specifies the durations in hours, we store them in seconds.
     */
    private final Map<DataRetentionKey, Integer> dataRetentions = new ConcurrentHashMap<>();

    public MetricsServiceCassandra() {
        this.state = State.STARTING;
    }

    @Override
    public void startUp(Session s) {
        this.dataAccess = new DataAccessImpl(s);
        loadDataRetentions();
        state = State.STARTED;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public void setState(State state) {
        this.state = state;
    }

    void loadDataRetentions() {
        DataRetentionsMapper mapper = new DataRetentionsMapper();
        List<String> tenantIds = loadTenantIds();
        CountDownLatch latch = new CountDownLatch(tenantIds.size() * 2);
        for (String tenantId : tenantIds) {
            ResultSetFuture gaugeFuture = dataAccess.findDataRetentions(tenantId, MetricType.GAUGE);
            ResultSetFuture availabilityFuture = dataAccess.findDataRetentions(tenantId, MetricType.AVAILABILITY);
            ListenableFuture<Set<Retention>> gaugeRetentions = Futures.transform(gaugeFuture, mapper, metricsTasks);
            ListenableFuture<Set<Retention>> availabilityRetentions = Futures.transform(availabilityFuture, mapper,
                    metricsTasks);
            Futures.addCallback(gaugeRetentions,
                    new DataRetentionsLoadedCallback(tenantId, MetricType.GAUGE, latch));
            Futures.addCallback(availabilityRetentions, new DataRetentionsLoadedCallback(tenantId,
                    MetricType.AVAILABILITY, latch));
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    void unloadDataRetentions() {
        dataRetentions.clear();
    }

    private static class MergeTagsFunction<T extends MetricData> implements
        Function<List<Map<MetricId, Set<T>>>, Map<MetricId, Set<T>>> {

        @Override
        public Map<MetricId, Set<T>> apply(List<Map<MetricId, Set<T>>> taggedDataMaps) {
            if (taggedDataMaps.isEmpty()) {
                return Collections.emptyMap();
            }
            if (taggedDataMaps.size() == 1) {
                return taggedDataMaps.get(0);
            }

            Set<MetricId> ids = new HashSet<>(taggedDataMaps.get(0).keySet());
            for (int i = 1; i < taggedDataMaps.size(); ++i) {
                ids.retainAll(taggedDataMaps.get(i).keySet());
            }

            Map<MetricId, Set<T>> mergedDataMap = new HashMap<>();
            for (MetricId id : ids) {
                TreeSet<T> set = new TreeSet<>(MetricData.TIME_UUID_COMPARATOR);
                for (Map<MetricId, Set<T>> taggedDataMap : taggedDataMaps) {
                    set.addAll(taggedDataMap.get(id));
                }
                mergedDataMap.put(id, set);
            }

            return mergedDataMap;
        }
    }

    private class DataRetentionsLoadedCallback implements FutureCallback<Set<Retention>> {

        private final String tenantId;

        private final MetricType type;

        private final CountDownLatch latch;

        public DataRetentionsLoadedCallback(String tenantId, MetricType type, CountDownLatch latch) {
            this.tenantId = tenantId;
            this.type = type;
            this.latch = latch;
        }

        @Override
        public void onSuccess(Set<Retention> dataRetentionsSet) {
            for (Retention r : dataRetentionsSet) {
                dataRetentions.put(new DataRetentionKey(tenantId, r.getId(), type), r.getValue());
            }
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable t) {
            logger.warn("Failed to load data retentions for {tenantId: " + tenantId + ", metricType: " +
                    type.getText() + "}", t);
            latch.countDown();
            // TODO We probably should not let initialization proceed on this error
        }
    }

    @Override
    public void shutdown() {
        state = State.STOPPED;
    }

    /**
     * This is a test hook.
     */
    DataAccess getDataAccess() {
        return dataAccess;
    }

    /**
     * This is a test hook.
     */
    void setDataAccess(DataAccess dataAccess) {
        this.dataAccess = dataAccess;
    }

    @Override
    public ListenableFuture<Void> createTenant(final Tenant tenant) {
        ResultSetFuture future = dataAccess.insertTenant(tenant);
        return Futures.transform(future, new AsyncFunction<ResultSet, Void>() {
            @Override
            public ListenableFuture<Void> apply(ResultSet resultSet) {
                if (!resultSet.wasApplied()) {
                    throw new TenantAlreadyExistsException(tenant.getId());
                }
                Map<MetricType, Set<Retention>> retentionsMap = new HashMap<>();
                for (RetentionSettings.RetentionKey key : tenant.getRetentionSettings().keySet()) {
                    Set<Retention> retentions = retentionsMap.get(key.metricType);
                    if (retentions == null) {
                        retentions = new HashSet<>();
                    }
                    Interval interval = key.interval == null ? Interval.NONE : key.interval;
                    Hours hours = hours(tenant.getRetentionSettings().get(key));
                    retentions.add(new Retention(new MetricId("[" + key.metricType.getText() + "]", interval),
                        hours.toStandardSeconds().getSeconds()));
                    retentionsMap.put(key.metricType, retentions);
                }
                if (retentionsMap.isEmpty()) {
                    return Futures.immediateFuture(null);
                } else {
                    List<ResultSetFuture> updateRetentionFutures = new ArrayList<>();

                    for (Map.Entry<MetricType, Set<Retention>> metricTypeSetEntry : retentionsMap.entrySet()) {
                        updateRetentionFutures.add(dataAccess.updateRetentionsIndex(tenant.getId(),
                                metricTypeSetEntry.getKey(),
                                metricTypeSetEntry.getValue()));

                        for (Retention r : metricTypeSetEntry.getValue()) {
                            dataRetentions.put(new DataRetentionKey(tenant.getId(), metricTypeSetEntry.getKey()),
                                    r.getValue());
                        }
                    }

                    ListenableFuture<List<ResultSet>> updateRetentionsFuture = Futures
                        .allAsList(updateRetentionFutures);
                    return Futures.transform(updateRetentionsFuture, Functions.TO_VOID, metricsTasks);
                }
            }
        }, metricsTasks);
    }

    @Override
    public ListenableFuture<List<Tenant>> getTenants() {
        ListenableFuture<Stream<String>> tenantIdsFuture = Futures.transform(
                dataAccess.findAllTenantIds(), (ResultSet input) ->
                    StreamSupport.stream(input.spliterator(), false).map(row -> row.getString(0)), metricsTasks);

        return Futures.transform(tenantIdsFuture, (Stream<String> input) ->
                Futures.allAsList(input.map(dataAccess::findTenant).map(Functions::getTenant).collect(toList())));
    }

    private List<String> loadTenantIds() {
        ResultSet resultSet = dataAccess.findAllTenantIds().getUninterruptibly();
        List<String> ids = new ArrayList<>();
        for (Row row : resultSet) {
            ids.add(row.getString(0));
        }
        return ids;
    }

    @Override
    public Observable<Void> createMetric(final Metric<?> metric) {
        ResultSetFuture future = dataAccess.insertMetricInMetricsIndex(metric);
        Observable<ResultSet> indexUpdated = RxUtil.from(future, metricsTasks);
        return Observable.create(subscriber ->
            indexUpdated.subscribe(
                    resultSet -> {
                        if (!resultSet.wasApplied()) {
                            subscriber.onError(new MetricAlreadyExistsException(metric));

                        } else {
                            // TODO Need error handling if either of the following updates fail
                            // If adding tags/retention fails, then we want to report the error to the
                            // client. Updating the retentions_idx table could also fail. We need to
                            // report that failure as well.
                            //
                            // The error handling is the same as it was with Guava futures. That is, if any
                            // future fails, we treat the entire client request as a failure. We probably
                            // eventually want to implement more fine-grained error handling where we can
                            // notify the subscriber of what exactly fails.
                            ResultSetFuture metadataFuture = dataAccess.addTagsAndDataRetention(metric);
                            Observable<ResultSet> metadataUpdated = RxUtil.from(metadataFuture, metricsTasks);
                            ResultSetFuture tagsFuture = dataAccess.insertIntoMetricsTagsIndex(metric,
                                    metric.getTags());
                            Observable<ResultSet> tagsUpdated = RxUtil.from(tagsFuture, metricsTasks);
                            Observable<ResultSet> metricUpdates;

                            if (metric.getDataRetention() != null) {
                                ResultSetFuture dataRetentionFuture = dataAccess.updateRetentionsIndex(metric);
                                Observable<ResultSet> dataRetentionUpdated = RxUtil.from(dataRetentionFuture,
                                        metricsTasks);
                                dataRetentions.put(new DataRetentionKey(metric), metric.getDataRetention());
                                metricUpdates = Observable.merge(metadataUpdated, tagsUpdated, dataRetentionUpdated);
                            } else {
                                metricUpdates = Observable.merge(metadataUpdated, tagsUpdated);
                            }

                            metricUpdates.subscribe(new VoidSubscriber<>(subscriber));
                        }
                    }));
    }

    @Override
    public ListenableFuture<Optional<Metric<?>>> findMetric(final String tenantId, final MetricType type,
            final MetricId id) {
        ResultSetFuture future = dataAccess.findMetric(tenantId, type, id, Metric.DPART);
        return Futures.transform(future, (ResultSet resultSet) -> {
            if (resultSet.isExhausted()) {
                return Optional.empty();
            }
            Row row = resultSet.one();
            if (type == MetricType.GAUGE) {
                return Optional.of(new Gauge(tenantId, id, row.getMap(5, String.class, String.class),
                        row.getInt(6)));
            } else {
                return Optional.of(new Availability(tenantId, id, row.getMap(5, String.class, String.class),
                        row.getInt(6)));
            }
        }, metricsTasks);
    }

    @Override
    public ListenableFuture<List<Metric<?>>> findMetrics(String tenantId, MetricType type) {
        ResultSetFuture future = dataAccess.findMetricsInMetricsIndex(tenantId, type);
        return Futures.transform(future, new MetricsIndexMapper(tenantId, type), metricsTasks);
    }

    @Override
    public ListenableFuture<Map<String, String>> getMetricTags(String tenantId, MetricType type, MetricId id) {
        ResultSetFuture metricTags = dataAccess.getMetricTags(tenantId, type, id, Metric.DPART);
        return Futures.transform(metricTags, (ResultSet input) -> {
            if (input.isExhausted()) {
                return Collections.EMPTY_MAP;
            }
            return input.one().getMap(0, String.class, String.class);
        }, metricsTasks);
    }

    // Adding/deleting metric tags currently involves writing to three tables - data,
    // metrics_idx, and metrics_tags_idx. It might make sense to refactor tag related
    // functionality into a separate class.
    @Override
    public ListenableFuture<Void> addTags(Metric metric, Map<String, String> tags) {
        List<ResultSetFuture> insertFutures = asList(
            dataAccess.addTags(metric, tags),
            dataAccess.insertIntoMetricsTagsIndex(metric, tags)
        );
        ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insertFutures);
        return Futures.transform(insertsFuture, Functions.TO_VOID, metricsTasks);
    }

    @Override
    public ListenableFuture<Void> deleteTags(Metric metric, Map<String, String> tags) {
        List<ResultSetFuture> deleteFutures = asList(
            dataAccess.deleteTags(metric, tags.keySet()),
            dataAccess.deleteFromMetricsTagsIndex(metric, tags)
        );
        ListenableFuture<List<ResultSet>> deletesFuture = Futures.allAsList(deleteFutures);
        return Futures.transform(deletesFuture, Functions.TO_VOID, metricsTasks);
    }

    @Override
    public Observable<Void> addGaugeData(Observable<Gauge> gaugeObservable) {
        // This is a first, rough cut at an RxJava implementation for storing metric data. This code will change but
        // for now the goal is 1) replace ListenableFuture in the API with Observable and 2) make sure tests still
        // pass. This means that the behavior basically remains the same as before. The idea here is to implement a
        // pub/sub workflow.

        return Observable.create(subscriber -> {
            Map<Gauge, ResultSetFuture> insertsMap = new HashMap<>();
            gaugeObservable.subscribe(
                    gauge -> {
                        int ttl = getTTL(gauge);
                        insertsMap.put(gauge, dataAccess.insertData(gauge, ttl));
                    },
                    t -> logger.warn("There was an error receive gauge data to insert", t),
                    () -> {
                        List<ResultSetFuture> inserts = Lists.newArrayList(insertsMap.values());
                        inserts.add(dataAccess.updateMetricsIndex(ImmutableList.copyOf(insertsMap.keySet())));
                        ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(inserts);
                        Observable<List<ResultSet>> dataInserted = RxUtil.from(insertsFuture, metricsTasks);
                        dataInserted.subscribe(new VoidSubscriber<>(subscriber));
                    });
        });
    }

    @Override
    public ListenableFuture<Void> addAvailabilityData(List<Availability> metrics) {
        List<ResultSetFuture> insertFutures = new ArrayList<>(metrics.size());
        for (Availability metric : metrics) {
            if (metric.getData().isEmpty()) {
                logger.warn("There is no data to insert for {}", metric);
            } else {
                int ttl = getTTL(metric);
                insertFutures.add(dataAccess.insertData(metric, ttl));
            }
        }
        insertFutures.add(dataAccess.updateMetricsIndex(metrics));
        ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insertFutures);
        return Futures.transform(insertsFuture, Functions.TO_VOID, metricsTasks);
    }

    @Override
    public ListenableFuture<Void> updateCounter(Counter counter) {
//        return Futures.transform(dataAccess.updateCounter(counter), TO_VOID);
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> updateCounters(Collection<Counter> counters) {
//        ResultSetFuture future = dataAccess.updateCounters(counters);
//        return Futures.transform(future, TO_VOID);
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<List<Counter>> findCounters(String group) {
//        ResultSetFuture future = dataAccess.findCounters(group);
//        return Futures.transform(future, mapCounters, metricsTasks);
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<List<Counter>> findCounters(String group, List<String> counterNames) {
//        ResultSetFuture future = dataAccess.findCounters(group, counterNames);
//        return Futures.transform(future, mapCounters, metricsTasks);
        throw new UnsupportedOperationException();
    }

    @Override
    public Observable<GaugeData> findGaugeData(String tenantId, MetricId id, Long start, Long end) {
        // When we implement date partitioning, dpart will have to be determined based on
        // the start and end params. And it is possible the the date range spans multiple
        // date partitions.
        ResultSetFuture future = dataAccess.findData(tenantId, id, start, end);
        return RxUtil.from(future, metricsTasks).flatMap(Observable::from).map(Functions::getGaugeData);
    }

    @Override
    public ListenableFuture<BucketedOutput<GaugeBucketDataPoint>> findGaugeStats(
            Gauge metric, long start, long end, Buckets buckets
    ) {
        // When we implement date partitioning, dpart will have to be determined based on
        // the start and end params. And it is possible the the date range spans multiple
        // date partitions.
        ResultSetFuture queryFuture = dataAccess.findData(metric.getTenantId(), metric.getId(), start, end);
        ListenableFuture<List<GaugeData>> raw = Futures.transform(queryFuture, Functions.MAP_GAUGE_DATA,
                metricsTasks);
        return Futures.transform(raw, new GaugeBucketedOutputMapper(metric.getTenantId(), metric.getId(), buckets));
    }

    @Override
    public ListenableFuture<List<AvailabilityData>> findAvailabilityData(String tenantId, MetricId id, long start,
            long end) {
        return findAvailabilityData(tenantId, id, start, end, false);
    }

    @Override
    public ListenableFuture<List<AvailabilityData>> findAvailabilityData(String tenantId, MetricId id, long start,
            long end, boolean distinct) {
        ResultSetFuture queryFuture = dataAccess.findAvailabilityData(tenantId, id, start, end);
        ListenableFuture<List<AvailabilityData>> availabilityFuture =  Futures.transform(queryFuture,
                Functions.MAP_AVAILABILITY_DATA, metricsTasks);
        if (distinct) {
            return Futures.transform(availabilityFuture, (List<AvailabilityData> availabilities) -> {
                if (availabilities.isEmpty()) {
                    return availabilities;
                }
                List<AvailabilityData> distinctAvailabilities = new ArrayList<>(availabilities.size());
                AvailabilityData previous = availabilities.get(0);
                distinctAvailabilities.add(previous);
                for (AvailabilityData availability : availabilities){
                    if (availability.getType() != previous.getType()) {
                        distinctAvailabilities.add(availability);
                    }
                    previous = availability;
                }
                return distinctAvailabilities;
            }, metricsTasks);
        } else {
            return availabilityFuture;
        }
    }

    @Override
    public ListenableFuture<BucketedOutput<AvailabilityBucketDataPoint>> findAvailabilityStats(
            Availability metric, long start, long end, Buckets buckets
    ) {
        ResultSetFuture queryFuture = dataAccess.findAvailabilityData(metric.getTenantId(), metric.getId(), start, end);
        ListenableFuture<List<AvailabilityData>> raw = Futures.transform(queryFuture, Functions.MAP_AVAILABILITY_DATA,
                metricsTasks);
        return Futures.transform(raw, new AvailabilityBucketedOutputMapper(metric.getTenantId(), metric.getId(),
                buckets));
    }

    @Override
    public ListenableFuture<Boolean> idExists(final String id) {
        ResultSetFuture future = dataAccess.findAllGuageMetrics();
        return Futures.transform(future, new Function<ResultSet, Boolean>() {
            @Override
            public Boolean apply(ResultSet resultSet) {
                for (Row row : resultSet) {
                    if (id.equals(row.getString(2))) {
                        return true;
                    }
                }
                return false;
            }
        }, metricsTasks);
    }

    @Override
    // TODO refactor to support multiple metrics
    // Data for different metrics and for the same tag are stored within the same partition
    // in the tags table; therefore, it makes sense for the API to support tagging multiple
    // metrics since they could efficiently be inserted in a single batch statement.
    public ListenableFuture<List<GaugeData>> tagGaugeData(Gauge metric, final Map<String, String> tags,
            long start, long end) {
        ResultSetFuture queryFuture = dataAccess.findData(metric.getTenantId(), metric.getId(), start, end, true);
        ListenableFuture<List<GaugeData>> dataFuture = Futures.transform(queryFuture,
                Functions.MAP_GAUGE_DATA_WITH_WRITE_TIME, metricsTasks);
        int ttl = getTTL(metric);
        ListenableFuture<List<GaugeData>> updatedDataFuture = Futures.transform(dataFuture, new ComputeTTL<>(ttl));
        return Futures.transform(updatedDataFuture, new TagAsyncFunction(tags, metric));
    }

    @Override
    public ListenableFuture<List<AvailabilityData>> tagAvailabilityData(Availability metric,
            Map<String, String> tags, long start, long end) {
        ResultSetFuture queryFuture = dataAccess.findData(metric, start, end, true);
        ListenableFuture<List<AvailabilityData>> dataFuture = Futures.transform(queryFuture,
            Functions.MAP_AVAILABILITY_WITH_WRITE_TIME, metricsTasks);
        int ttl = getTTL(metric);
        ListenableFuture<List<AvailabilityData>> updatedDataFuture = Futures.transform(dataFuture,
                new ComputeTTL<>(ttl));
        return Futures.transform(updatedDataFuture, new TagAsyncFunction(tags, metric));
    }

    @Override
    public ListenableFuture<List<GaugeData>> tagGaugeData(Gauge metric, final Map<String, String> tags,
            long timestamp) {
        ListenableFuture<ResultSet> queryFuture = dataAccess.findData(metric, timestamp, true);
        ListenableFuture<List<GaugeData>> dataFuture = Futures.transform(queryFuture,
                Functions.MAP_GAUGE_DATA_WITH_WRITE_TIME, metricsTasks);
        int ttl = getTTL(metric);
        ListenableFuture<List<GaugeData>> updatedDataFuture = Futures.transform(dataFuture, new ComputeTTL<>(ttl));
        return Futures.transform(updatedDataFuture, new TagAsyncFunction(tags, metric));
    }

    @Override
    public ListenableFuture<List<AvailabilityData>> tagAvailabilityData(Availability metric,
            final Map<String, String> tags, long timestamp) {
        ListenableFuture<ResultSet> queryFuture = dataAccess.findData(metric, timestamp);
        ListenableFuture<List<AvailabilityData>> dataFuture = Futures.transform(queryFuture,
            Functions.MAP_AVAILABILITY_WITH_WRITE_TIME, metricsTasks);
        int ttl = getTTL(metric);
        ListenableFuture<List<AvailabilityData>> updatedDataFuture = Futures.transform(dataFuture,
                new ComputeTTL<>(ttl));
        return Futures.transform(updatedDataFuture, new TagAsyncFunction(tags, metric));
    }

    @Override
    public ListenableFuture<Map<MetricId, Set<GaugeData>>> findGaugeDataByTags(String tenantId,
            Map<String, String> tags) {
        List<ListenableFuture<Map<MetricId, Set<GaugeData>>>> queryFutures = new ArrayList<>(tags.size());
        tags.forEach((k, v) -> queryFutures.add(Futures.transform(dataAccess.findGuageDataByTag(tenantId, k, v),
                new TaggedGaugeDataMapper(), metricsTasks)));
        ListenableFuture<List<Map<MetricId, Set<GaugeData>>>> queriesFuture = Futures.allAsList(queryFutures);
        return Futures.transform(queriesFuture, new MergeTagsFunction());

    }

    @Override
    public ListenableFuture<Map<MetricId, Set<AvailabilityData>>> findAvailabilityByTags(String tenantId,
            Map<String, String> tags) {
        List<ListenableFuture<Map<MetricId, Set<AvailabilityData>>>> queryFutures = new ArrayList<>(tags.size());
        tags.forEach((k, v) -> queryFutures.add(Futures.transform(dataAccess.findAvailabilityByTag(tenantId, k, v),
                new TaggedAvailabilityMappper(), metricsTasks)));
        ListenableFuture<List<Map<MetricId, Set<AvailabilityData>>>> queriesFuture = Futures.allAsList(queryFutures);
        return Futures.transform(queriesFuture, new MergeTagsFunction());
    }

    @Override
    public ListenableFuture<List<long[]>> getPeriods(String tenantId, MetricId id, Predicate<Double> predicate,
        long start, long end) {
        ResultSetFuture resultSetFuture = dataAccess.findData(new Gauge(tenantId, id), start, end, Order.ASC);
        ListenableFuture<List<GaugeData>> dataFuture = Futures.transform(resultSetFuture,
                Functions.MAP_GAUGE_DATA, metricsTasks);

        return Futures.transform(dataFuture, (List<GaugeData> data) -> {
            List<long[]> periods = new ArrayList<>(data.size());
            long[] period = null;
            GaugeData previous = null;
            for (GaugeData d : data) {
                if (predicate.test(d.getValue())) {
                    if (period == null) {
                        period = new long[2];
                        period[0] = d.getTimestamp();
                    }
                    previous = d;
                } else if (period != null) {
                    period[1] = previous.getTimestamp();
                    periods.add(period);
                    period = null;
                    previous = null;
                }
            }
            if (period != null) {
                period[1] = previous.getTimestamp();
                periods.add(period);
            }

            return periods;
        }, metricsTasks);
    }

    private int getTTL(Metric<?> metric) {
        Integer ttl = dataRetentions.get(new DataRetentionKey(metric.getTenantId(), metric.getId(), metric.getType()));
        if (ttl == null) {
            ttl = dataRetentions.get(new DataRetentionKey(metric.getTenantId(), metric.getType()));
            if (ttl == null) {
                ttl = DEFAULT_TTL;
            }
        }
        return ttl;
    }

    private class TagAsyncFunction<T extends MetricData> implements AsyncFunction<List<T>, List<T>> {
        private final Map<String, String> tags;
        private final Metric<?> metric;

        public TagAsyncFunction(Map<String, String> tags, Metric<?> metric) {
            this.tags = tags;
            this.metric = metric;
        }

        @Override
        public ListenableFuture<List<T>> apply(List<T> data) throws Exception {
            if (data.isEmpty()) {
                List<T> results = Collections.emptyList();
                return Futures.immediateFuture(results);
            }
            List<ResultSetFuture> insertFutures = new ArrayList<>(tags.size());
            tags.forEach((k, v) -> {
                if(metric instanceof Gauge) {
                    insertFutures.add(dataAccess.insertGuageTag(k, v, (Gauge) metric,
                                                                  (List<GaugeData>) data));
                } else if(metric instanceof Availability) {
                    insertFutures.add(dataAccess.insertAvailabilityTag(k, v, (Availability) metric,
                                                                       (List<AvailabilityData>) data));
                }
            });
            data.forEach(t -> insertFutures.add(dataAccess.updateDataWithTag(metric, t, tags)));
            ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insertFutures);
            return Futures.transform(insertsFuture, (List<ResultSet> resultSets) -> data);
        }
    }
}
