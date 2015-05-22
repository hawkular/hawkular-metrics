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
import org.hawkular.metrics.schema.SchemaManager;
import org.hawkular.rx.cassandra.driver.RxUtil;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Hours;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.subjects.PublishSubject;

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
    public Observable<Void> createTenant(final Tenant tenant) {
        return dataAccess.insertTenant(tenant).flatMap(
                resultSet -> {
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
                        retentions.add(
                                new Retention(
                                        new MetricId("[" + key.metricType.getText() + "]", interval),
                                        hours.toStandardSeconds().getSeconds()
                                )
                        );
                        retentionsMap.put(key.metricType, retentions);
                    }
                    if (retentionsMap.isEmpty()) {
                        return Observable.from(Collections.singleton(null));
                    }
                    List<ResultSetFuture> updateRetentionFutures = new ArrayList<>();

                    for (Map.Entry<MetricType, Set<Retention>> metricTypeSetEntry : retentionsMap.entrySet()) {
                        updateRetentionFutures.add(
                                dataAccess.updateRetentionsIndex(
                                        tenant.getId(),
                                        metricTypeSetEntry.getKey(),
                                        metricTypeSetEntry.getValue()
                                )
                        );

                        for (Retention r : metricTypeSetEntry.getValue()) {
                            dataRetentions.put(
                                    new DataRetentionKey(tenant.getId(), metricTypeSetEntry.getKey()),
                                    r.getValue()
                            );
                        }
                    }

                    ListenableFuture<List<ResultSet>> updateRetentionsFuture = Futures
                            .allAsList(updateRetentionFutures);
                    ListenableFuture<Void> transform = Futures.transform(
                            updateRetentionsFuture,
                            Functions.TO_VOID,
                            metricsTasks
                    );
                    return RxUtil.from(transform, metricsTasks);
                }
        );
    }

    @Override
    public Observable<Tenant> getTenants() {
        return dataAccess.findAllTenantIds()
                         .flatMap(Observable::from)
                         .map(row -> row.getString(0))
                         .flatMap(dataAccess::findTenant)
                         .flatMap(Observable::from)
                         .map(Functions::getTenant);
    }

    private List<String> loadTenantIds() {
        Iterable<String> tenantIds = dataAccess.findAllTenantIds()
                                               .flatMap(Observable::from)
                                               .map(row -> row.getString(0))
                                               .toBlocking()
                                               .toIterable();
        return ImmutableList.copyOf(tenantIds);
    }

    @Override
    public Observable<Void> createMetric(final Metric<?> metric) {
        ResultSetFuture future = dataAccess.insertMetricInMetricsIndex(metric);
        Observable<ResultSet> indexUpdated = RxUtil.from(future, metricsTasks);
        return Observable.create(subscriber -> indexUpdated.subscribe(resultSet -> {
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
            Observable<ResultSet> metadataUpdated = dataAccess.addTagsAndDataRetention(metric);
            Observable<ResultSet> tagsUpdated = dataAccess.insertIntoMetricsTagsIndex(metric, metric.getTags());
            Observable<ResultSet> metricUpdates;

            if (metric.getDataRetention() != null) {
                ResultSetFuture dataRetentionFuture = dataAccess.updateRetentionsIndex(metric);
                Observable<ResultSet> dataRetentionUpdated = RxUtil.from(dataRetentionFuture, metricsTasks);
                dataRetentions.put(new DataRetentionKey(metric), metric.getDataRetention());
                metricUpdates = Observable.merge(metadataUpdated, tagsUpdated, dataRetentionUpdated);
            } else {
                metricUpdates = Observable.merge(metadataUpdated, tagsUpdated);
            }

            metricUpdates.subscribe(new VoidSubscriber<>(subscriber));
        }
    })  );
    }

    @Override
    public Observable<Optional<? extends Metric<? extends MetricData>>> findMetric(final String tenantId,
            final MetricType type, final MetricId id) {

        return dataAccess.findMetric(tenantId, type, id, Metric.DPART)
                .flatMap(Observable::from)
                .map(row -> {
                    if (type == MetricType.GAUGE) {
                        return Optional.of(new Gauge(tenantId, id, row.getMap(5, String.class, String.class),
                                row.getInt(6)));
                    } else {
                        return Optional.of(new Availability(tenantId, id, row.getMap(5, String.class, String.class),
                                row.getInt(6)));
                    }
                })
                .defaultIfEmpty(Optional.empty());
    }

    @Override
    public Observable<Metric<?>> findMetrics(String tenantId, MetricType type) {
        Observable<ResultSet> observable = dataAccess.findMetricsInMetricsIndex(tenantId, type);
        if (type == MetricType.GAUGE) {
            return observable.flatMap(Observable::from).map(row -> toGauge(tenantId, row));
        } else {
            return observable.flatMap(Observable::from).map(row -> toAvailability(tenantId, row));
        }
    }

    private Gauge toGauge(String tenantId, Row row) {
        return new Gauge(
                tenantId,
                new MetricId(row.getString(0), Interval.parse(row.getString(1))),
                row.getMap(2, String.class, String.class),
                row.getInt(3)
        );
    }

    private Availability toAvailability(String tenantId, Row row) {
        return new Availability(
                tenantId,
                new MetricId(row.getString(0), Interval.parse(row.getString(1))),
                row.getMap(2, String.class, String.class),
                row.getInt(3)
        );
    }

    @Override
    public Observable<Optional<Map<String, String>>> getMetricTags(String tenantId, MetricType type, MetricId id) {
        Observable<ResultSet> metricTags = dataAccess.getMetricTags(tenantId, type, id, Metric.DPART);

        return metricTags.flatMap(Observable::from).take(1).map(row -> Optional.of(row.getMap(0, String.class, String
                .class)))
            .defaultIfEmpty(Optional.empty());
    }

    // Adding/deleting metric tags currently involves writing to three tables - data,
    // metrics_idx, and metrics_tags_idx. It might make sense to refactor tag related
    // functionality into a separate class.
    @Override
    public Observable<ResultSet> addTags(Metric metric, Map<String, String> tags) {
        return dataAccess.addTags(metric, tags).mergeWith(dataAccess.insertIntoMetricsTagsIndex(metric, tags));
    }

    @Override
    public Observable<ResultSet> deleteTags(Metric metric, Map<String, String> tags) {
        return dataAccess.deleteTags(metric, tags.keySet()).mergeWith(
                dataAccess.deleteFromMetricsTagsIndex(metric, tags));
    }

    @Override
    public Observable<Void> addGaugeData(Observable<Gauge> gaugeObservable) {
        // We write to both the data and the metrics_idx tables. Each Gauge can have one or more data points. We
        // currently write a separate batch statement for each gauge.
        //
        // TODO Is there additional overhead of using batch statement when there is only a single insert?
        //      If there is overhead, then we should avoid using batch statements when the metric has only a single
        //      data point which could be quite often.
        //
        // The metrics_idx table stores the metric id along with any tags, and data retention. The update we perform
        // here though only inserts the metric id (i.e., name and interval). We need to revisit this logic. The original
        // intent for updating metrics_idx here is that even if the client does not explicitly create the metric, we
        // still have it in metrics_idx. In reality, I think clients will be explicitly creating metrics. This will
        // certainly be the case with the full, integrated hawkular server.
        //
        // TODO Determine how much overhead is caused by updating metrics_idx on every write
        //      If there much overhead, then we might not want to update the index every time we insert data. Maybe
        //      we periodically update it in the background, so we will still be aware of metrics that have not been
        //      explicitly created, just not necessarily right away.

        PublishSubject<Void> results = PublishSubject.create();
        Observable<ResultSet> dataInserted = dataAccess.insertData(
                gaugeObservable.map(gauge -> new GaugeAndTTL(gauge, getTTL(gauge))));
        Observable<ResultSet> indexUpdated = dataAccess.updateMetricsIndexRx(gaugeObservable);
        dataInserted.concatWith(indexUpdated).subscribe(
                resultSet -> {},
                results::onError,
                () -> {
                    results.onNext(null);
                    results.onCompleted();
                });
        return results;
    }

    @Override
    public Observable<ResultSet> addAvailabilityData(List<Availability> metrics) {
        return Observable.from(metrics)
                .filter(a -> !a.getData().isEmpty())
                .flatMap(a -> dataAccess.insertData(a, getTTL(a)))
                .doOnCompleted(() -> dataAccess.updateMetricsIndex(metrics));
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
        return dataAccess.findData(tenantId, id, start, end).flatMap(Observable::from).map(Functions::getGaugeData);
    }

    @Override
    public ListenableFuture<BucketedOutput<GaugeBucketDataPoint>> findGaugeStats(
            Gauge metric, long start, long end, Buckets buckets
    ) {
        // When we implement date partitioning, dpart will have to be determined based on
        // the start and end params. And it is possible the the date range spans multiple
        // date partitions.

//
//
//        ResultSetFuture queryFuture = dataAccess.findData(metric.getTenantId(), metric.getId(), start, end);
//        ListenableFuture<List<GaugeData>> raw = Futures.transform(queryFuture, Functions.MAP_GAUGE_DATA,
//                metricsTasks);
//        return Futures.transform(raw, new GaugeBucketedOutputMapper(metric.getTenantId(), metric.getId(), buckets));

        List<GaugeData> data = ImmutableList.copyOf(
                dataAccess.findData(metric.getTenantId(), metric.getId(), start, end)
                        .flatMap(Observable::from)
                        .map(Functions::getGaugeData)
                        .toBlocking()
                        .toIterable());
        GaugeBucketedOutputMapper mapper = new GaugeBucketedOutputMapper(metric.getTenantId(), metric.getId(), buckets);
        return Futures.immediateFuture(mapper.apply(data));
    }

    @Override
    public Observable<AvailabilityData> findAvailabilityData(String tenantId, MetricId id, long start,
                                                             long end) {
        return findAvailabilityData(tenantId, id, start, end, false);
    }

    @Override
    public Observable<AvailabilityData> findAvailabilityData(String tenantId, MetricId id, long start,
                                                             long end, boolean distinct) {
        Observable<AvailabilityData> availabilityData = dataAccess.findAvailabilityData(tenantId, id, start, end)
                .flatMap(r -> Observable.from(r))
                .map(Functions::getAvailability);
        if(distinct) {
            return availabilityData.distinctUntilChanged((a) -> a.getType());
        } else {
            return availabilityData;
        }
    }

    @Override
    public Observable<BucketedOutput<AvailabilityBucketDataPoint>> findAvailabilityStats(Availability metric,
        long start, long end, Buckets buckets) {
        AvailabilityBucketedOutputMapper availabilityBucketedOutputMapper = new AvailabilityBucketedOutputMapper(
            metric.getTenantId(), metric.getId(), buckets);
        return dataAccess.findAvailabilityData(metric.getTenantId(), metric.getId(), start, end)
            .flatMap(r -> Observable.from(r)).map(Functions::getAvailability)
                .toList()
                .map(l -> availabilityBucketedOutputMapper.apply(l));
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
    public Observable<ResultSet> tagGaugeData(Gauge metric, final Map<String, String> tags,
                                              long start, long end) {
        Observable<ResultSet> findDataObservable = dataAccess.findData(metric.getTenantId(), metric.getId(), start, end,
                true);
        return tagGaugeData(findDataObservable, tags, metric);
    }

    private Observable<ResultSet> tagGaugeData(Observable<ResultSet> findDataObservable, Map<String, String> tags,
                                               Gauge metric) {
        int ttl = getTTL(metric);
        Observable<Map.Entry<String, String>> tagsObservable = Observable.from(tags.entrySet()).cache();
        Observable<GaugeData> gauges = findDataObservable.flatMap(Observable::from)
                .map(Functions::getGaugeDataAndWriteTime)
                .map(g -> (GaugeData) computeTTL(g, ttl))
                .cache();

        Observable<ResultSet> tagInsert = tagsObservable
                .flatMap(t -> dataAccess.insertGaugeTag(t.getKey(), t.getValue(), metric, gauges));

        Observable<ResultSet> tagsInsert = gauges
                .flatMap(g -> dataAccess.updateDataWithTag(metric, g, tags));

        return tagInsert.concatWith(tagsInsert);
    }

    private Observable<ResultSet> tagAvailabilityData(Observable<ResultSet> findDataObservable,
                                                      Map<String, String> tags, Availability metric) {
        int ttl = getTTL(metric);
        Observable<Map.Entry<String, String>> tagsObservable = Observable.from(tags.entrySet()).cache();
        Observable<AvailabilityData> availabilities = findDataObservable.flatMap(Observable::from)
                .map(Functions::getAvailabilityAndWriteTime)
                .map(a -> (AvailabilityData) computeTTL(a, ttl))
                .cache();

        Observable<ResultSet> tagInsert = tagsObservable
                .flatMap(t -> dataAccess.insertAvailabilityTag(t.getKey(), t.getValue(), metric, availabilities));

        Observable<ResultSet> tagsInsert = availabilities
                .flatMap(a -> dataAccess.updateDataWithTag(metric, a, tags));

        return tagInsert.concatWith(tagsInsert);
    }

    @Override
    public Observable<ResultSet> tagAvailabilityData(Availability metric,
                                                     Map<String, String> tags, long start, long end) {
        Observable<ResultSet> findDataObservable = dataAccess.findData(metric, start, end, true);
        return tagAvailabilityData(findDataObservable, tags, metric);
    }

    private MetricData computeTTL(MetricData d, int originalTTL) {
        Duration duration = new Duration(DateTime.now().minus(d.getWriteTime()).getMillis());
        d.setTTL(originalTTL - duration.toStandardSeconds().getSeconds());
        return d;
    }

    @Override
    public Observable<ResultSet> tagGaugeData(Gauge metric, final Map<String, String> tags,
                                              long timestamp) {
        Observable<ResultSet> findDataObservable = dataAccess.findData(metric, timestamp, true);
        return tagGaugeData(findDataObservable, tags, metric);
    }

    @Override
    public Observable<ResultSet> tagAvailabilityData(Availability metric,
                                                     final Map<String, String> tags, long timestamp) {
        Observable<ResultSet> findDataObservable = dataAccess.findData(metric, timestamp);
        return tagAvailabilityData(findDataObservable, tags, metric);
    }

    @Override
    public Observable<Map<MetricId, Set<GaugeData>>> findGaugeDataByTags(String tenantId, Map<String, String> tags) {
        MergeTagsFunction f = new MergeTagsFunction();

        return Observable.from(tags.entrySet())
                .flatMap(e -> dataAccess.findGaugeDataByTag(tenantId, e.getKey(), e.getValue()))
                .map(TaggedGaugeDataMapper::apply)
                .toList()
                .map(r -> f.apply(r));
    }

    @Override
    public Observable<Map<MetricId, Set<AvailabilityData>>> findAvailabilityByTags(String tenantId,
        Map<String, String> tags) {
        MergeTagsFunction f = new MergeTagsFunction();

        return Observable.from(tags.entrySet())
                .flatMap(e -> dataAccess.findAvailabilityByTag(tenantId, e.getKey(), e.getValue()))
                .map(TaggedAvailabilityMapper::apply)
                .toList()
                .map(r -> f.apply(r));
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

    private void updateSchemaIfNecessary(String schemaName) {
        try {
            SchemaManager schemaManager = new SchemaManager(session.get());
            schemaManager.createSchema(schemaName);
        } catch (IOException e) {
            throw new RuntimeException("Schema creation failed", e);
        }
    }
}
