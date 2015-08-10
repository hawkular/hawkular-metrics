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

import static java.util.Comparator.comparingLong;

import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.COUNTER_RATE;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;
import static org.hawkular.metrics.core.impl.Functions.getTTLAvailabilityDataPoint;
import static org.hawkular.metrics.core.impl.Functions.getTTLGaugeDataPoint;
import static org.hawkular.metrics.core.impl.Functions.makeSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.hawkular.metrics.core.api.AvailabilityBucketPoint;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.GaugeBucketPoint;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.MetricsThreadFactory;
import org.hawkular.metrics.core.api.Retention;
import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.core.api.TenantAlreadyExistsException;
import org.hawkular.metrics.core.impl.transformers.ItemsToSetTransformer;
import org.hawkular.metrics.core.impl.transformers.TagsIndexRowTransformer;
import org.hawkular.metrics.schema.SchemaManager;
import org.hawkular.metrics.tasks.api.RepeatingTrigger;
import org.hawkular.metrics.tasks.api.TaskScheduler;
import org.hawkular.metrics.tasks.api.Trigger;
import org.hawkular.rx.cassandra.driver.RxUtil;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import rx.Observable;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

/**
 * @author John Sanda
 */
public class MetricsServiceImpl implements MetricsService {

    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceImpl.class);

    /**
     * In seconds.
     */
    public static final int DEFAULT_TTL = Duration.standardDays(7).toStandardSeconds().getSeconds();

    private static class DataRetentionKey {
        private final MetricId metricId;

        public DataRetentionKey(String tenantId, MetricType type) {
            metricId = new MetricId(tenantId, type, makeSafe(type.getText()));
        }

        public DataRetentionKey(MetricId metricId) {
            this.metricId = metricId;
        }

        public DataRetentionKey(Metric metric) {
            this.metricId = metric.getId();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DataRetentionKey that = (DataRetentionKey) o;

            return metricId.equals(that.metricId);
        }

        @Override
        public int hashCode() {
            return metricId.hashCode();
        }
    }

    /**
     * Note that while user specifies the durations in hours, we store them in seconds.
     */
    private final Map<DataRetentionKey, Integer> dataRetentions = new ConcurrentHashMap<>();

    private ListeningExecutorService metricsTasks;

    private DataAccess dataAccess;

    private TaskScheduler taskScheduler;

    private MetricRegistry metricRegistry;

    /**
     * Measures the throughput of inserting gauge data points.
     */
    private Meter gaugeInserts;

    /**
     * Measures the throughput of inserting availability data points.
     */
    private Meter availabilityInserts;

    /**
     * Measures the throughput of inserting counter data points.
     */
    private Meter counterInserts;

    /**
     * Measures the latency of queries for gauge (raw) data.
     */
    private Timer gaugeReadLatency;

    /**
     * Measures the latency of queries for raw counter data.
     */
    private Timer counterReadLatency;

    /**
     * Measures the latency of queries for availability (raw) data.
     */
    private Timer availabilityReadLatency;

    public void startUp(Session session, String keyspace, boolean resetDb, MetricRegistry metricRegistry) {
        startUp(session, keyspace, resetDb, true, metricRegistry);
    }

    public void startUp(Session session, String keyspace, boolean resetDb, boolean createSchema,
            MetricRegistry metricRegistry) {
        SchemaManager schemaManager = new SchemaManager(session);
        if (resetDb) {
            schemaManager.dropKeyspace(keyspace);
        }
        if (createSchema) {
            // This creates/updates the keyspace + tables if needed
            schemaManager.createSchema(keyspace);
        }
        session.execute("USE " + keyspace);
        logger.info("Using a key space of '{}'", keyspace);
        metricsTasks = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));
        dataAccess = new DataAccessImpl(session);
        loadDataRetentions();

        this.metricRegistry = metricRegistry;
        initMetrics();

        GenerateRate generateRates = new GenerateRate(this);
        taskScheduler.subscribe(generateRates);
    }

    void loadDataRetentions() {
        List<String> tenantIds = loadTenantIds();
        CountDownLatch latch = new CountDownLatch(tenantIds.size() * 2);
        for (String tenantId : tenantIds) {
            DataRetentionsMapper gaugeMapper = new DataRetentionsMapper(tenantId, GAUGE);
            DataRetentionsMapper availMapper = new DataRetentionsMapper(tenantId, AVAILABILITY);
            ResultSetFuture gaugeFuture = dataAccess.findDataRetentions(tenantId, GAUGE);
            ResultSetFuture availabilityFuture = dataAccess.findDataRetentions(tenantId, MetricType.AVAILABILITY);
            ListenableFuture<Set<Retention>> gaugeRetentions = Futures.transform(gaugeFuture, gaugeMapper,
                    metricsTasks);
            ListenableFuture<Set<Retention>> availabilityRetentions = Futures.transform(availabilityFuture, availMapper,
                    metricsTasks);
            Futures.addCallback(gaugeRetentions,
                    new DataRetentionsLoadedCallback(tenantId, GAUGE, latch));
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

    private void initMetrics() {
        gaugeInserts = metricRegistry.meter("gauge-inserts");
        availabilityInserts = metricRegistry.meter("availability-inserts");
        counterInserts = metricRegistry.meter("counter-inserts");
        gaugeReadLatency = metricRegistry.timer("gauge-read-latency");
        availabilityReadLatency = metricRegistry.timer("availability-read-latency");
        counterReadLatency = metricRegistry.timer("counter-read-latency");
    }

    private static class MergeDataPointTagsFunction<T extends DataPoint> implements
            Func1<List<Map<MetricId, Set<T>>>, Map<MetricId, Set<T>>> {

        @Override
        public Map<MetricId, Set<T>> call(List<Map<MetricId, Set<T>>> taggedDataMaps) {
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
                TreeSet<T> set = new TreeSet<>(comparingLong(DataPoint::getTimestamp));
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
                dataRetentions.put(new DataRetentionKey(r.getId()), r.getValue());
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

    public void setTaskScheduler(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    @Override
    public Observable<Void> createTenant(final Tenant tenant) {
        return Observable.create(subscriber -> {
            Observable<ResultSet> updates = dataAccess.insertTenant(tenant).flatMap(resultSet -> {
                if (!resultSet.wasApplied()) {
                    throw new TenantAlreadyExistsException(tenant.getId());
                }

                Trigger trigger = new RepeatingTrigger.Builder()
                        .withDelay(1, TimeUnit.MINUTES)
                        .withInterval(1, TimeUnit.MINUTES)
                        .build();
                Map<String, String> params = ImmutableMap.of("tenant", tenant.getId());
                taskScheduler.scheduleTask("generate-rates", tenant.getId(), 100, params, trigger);

                return Observable.from(tenant.getRetentionSettings().entrySet())
                        .flatMap(entry -> dataAccess.updateRetentionsIndex(tenant.getId(), entry.getKey(),
                                ImmutableMap.of(makeSafe(entry.getKey().getText()), entry.getValue())));
            });
            updates.subscribe(resultSet -> {
            }, subscriber::onError, subscriber::onCompleted);
        });
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
    public Observable<Void> createMetric(Metric<?> metric) {
        if (metric.getType() == COUNTER_RATE) {
            throw new IllegalArgumentException(metric + " cannot be created. " + COUNTER_RATE + " metrics are " +
                    "internally generated metrics and cannot be created by clients.");
        }

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
                List<Observable<ResultSet>> updates = new ArrayList<>();
                updates.add(dataAccess.addTagsAndDataRetention(metric));
                updates.add(dataAccess.insertIntoMetricsTagsIndex(metric, metric.getTags()));

                if (metric.getDataRetention() != null) {
                    updates.add(updateRetentionsIndex(metric));
                }

                Observable.merge(updates).subscribe(new VoidSubscriber<>(subscriber));
            }
        }));
    }

    private Observable<ResultSet> updateRetentionsIndex(Metric metric) {
        ResultSetFuture dataRetentionFuture = dataAccess.updateRetentionsIndex(metric);
        Observable<ResultSet> dataRetentionUpdated = RxUtil.from(dataRetentionFuture, metricsTasks);
        // TODO Shouldn't we only update dataRetentions map when the retentions index update succeeds?
        dataRetentions.put(new DataRetentionKey(metric), metric.getDataRetention());

        return dataRetentionUpdated;
    }

    @Override
    public Observable<Metric> findMetric(final MetricId id) {
        return dataAccess.findMetric(id)
                .flatMap(Observable::from)
                .map(row -> new Metric(id, row.getMap(2, String.class, String.class),
                        row.getInt(3)));
    }

    @Override
    public Observable<Metric> findMetrics(String tenantId, MetricType type) {
        Observable<MetricType> typeObservable = (type == null) ? Observable.from(MetricType.userTypes()) :
                Observable.just(type);

        return typeObservable.flatMap(t -> dataAccess.findMetricsInMetricsIndex(tenantId, t))
                .flatMap(Observable::from)
                .map(row -> new Metric(new MetricId(tenantId, type, row.getString(0), Interval.parse(row.getString(1))),
                        row.getMap(2, String.class, String.class), row.getInt(3)));
    }

    @Override
    public Observable<Metric> findMetricsWithFilters(String tenantId, Map<String, String> tagsQueries, MetricType
            type) {

        // Fetch everything from the tagsQueries
        return Observable.from(tagsQueries.entrySet())
                .flatMap(entry -> {
                    // Special case "*" allowed and ! indicates match shouldn't happen
                    boolean positive = (!entry.getValue().startsWith("!"));
                    Pattern p = filterPattern(entry.getValue());

                    return Observable.just(entry)
                            .flatMap(e -> dataAccess.findMetricsByTagName(tenantId, e.getKey())
                                    .flatMap(Observable::from)
                                    .filter(r -> positive == p.matcher(r.getString(3)).matches()) // XNOR
                                    .compose(new TagsIndexRowTransformer(tenantId, type))
                                    .compose(new ItemsToSetTransformer<>())
                                    .reduce((s1, s2) -> {
                                        s1.addAll(s2);
                                        return s1;
                                    }));
                })
                .reduce((s1, s2) -> {
                    s1.retainAll(s2);
                    return s1;
                })
                .flatMap(Observable::from)
                .flatMap(this::findMetric);
    }

    /**
     * Allow special cases to Pattern matching, such as "*" -> ".*" and ! indicating the match shouldn't
     * happen. The first ! indicates the rest of the pattern should not match.
     *
     * @param inputRegexp Regexp given by the user
     * @return Pattern modified to allow special cases in the query language
     */
    private Pattern filterPattern(String inputRegexp) {
        if (inputRegexp.equals("*")) {
            inputRegexp = ".*";
        } else if (inputRegexp.startsWith("!")) {
            inputRegexp = inputRegexp.substring(1);
        }
        return Pattern.compile(inputRegexp); // Catch incorrect patterns..
    }

    @Override
    public Observable<Optional<Map<String, String>>> getMetricTags(MetricId id) {
        Observable<ResultSet> metricTags = dataAccess.getMetricTags(id, DataAccessImpl.DPART);

        return metricTags.flatMap(Observable::from).take(1).map(row -> Optional.of(row.getMap(0, String.class, String
                .class)))
                .defaultIfEmpty(Optional.empty());
    }

    // Adding/deleting metric tags currently involves writing to three tables - data,
    // metrics_idx, and metrics_tags_idx. It might make sense to refactor tag related
    // functionality into a separate class.
    @Override
    public Observable<Void> addTags(Metric metric, Map<String, String> tags) {
        return dataAccess.addTags(metric, tags).mergeWith(dataAccess.insertIntoMetricsTagsIndex(metric, tags))
                .toList().map(l -> null);
    }

    @Override
    public Observable<Void> deleteTags(Metric metric, Map<String, String> tags) {
        return dataAccess.deleteTags(metric, tags.keySet()).mergeWith(
                dataAccess.deleteFromMetricsTagsIndex(metric, tags)).toList().map(r -> null);
    }

    @Override
    public Observable<Void> addGaugeData(Observable<Metric<Double>> gaugeObservable) {
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
        Observable<Integer> updates = gaugeObservable.flatMap(g -> dataAccess.insertGaugeData(g, getTTL(g)));
        // I am intentionally return zero for the number index updates because I want to measure and compare the
        // throughput inserting data with and without the index updates. This will give us a better idea of how much
        // over there is with the index updates.
        Observable<Integer> indexUpdates = dataAccess.updateMetricsIndex(gaugeObservable).map(count -> 0);
        updates.concatWith(indexUpdates).subscribe(
                gaugeInserts::mark,
                results::onError,
                () -> {
                    results.onNext(null);
                    results.onCompleted();
                });
        return results;
    }

    @Override
    public Observable<Void> addAvailabilityData(Observable<Metric<AvailabilityType>> availabilities) {
        PublishSubject<Void> results = PublishSubject.create();
        Observable<Integer> updates = availabilities
                .filter(a -> !a.getDataPoints().isEmpty())
                .flatMap(a -> dataAccess.insertAvailabilityData(a, getTTL(a)));
        // I am intentionally return zero for the number index updates because I want to measure and compare the
        // throughput inserting data with and without the index updates. This will give us a better idea of how much
        // over there is with the index updates.
        Observable<Integer> indexUpdates = dataAccess.updateMetricsIndex(availabilities).map(count -> 0);
        updates.concatWith(indexUpdates).subscribe(
                availabilityInserts::mark,
                results::onError,
                () -> {
                    results.onNext(null);
                    results.onCompleted();
                });
        return results;
    }

    @Override
    public Observable<Void> addCounterData(Observable<Metric<Long>> counters) {
        PublishSubject<Void> results = PublishSubject.create();
        Observable<Integer> updates = counters.flatMap(c -> dataAccess.insertCounterData(c, getTTL(c)));
        // I am intentionally return zero for the number index updates because I want to measure and compare the
        // throughput inserting data with and without the index updates. This will give us a better idea of how much
        // over there is with the index updates.
        Observable<Integer> indexUpdates = dataAccess.updateMetricsIndex(counters).map(count -> 0);
        updates.concatWith(indexUpdates).subscribe(
                counterInserts::mark,
                results::onError,
                () -> {
                    results.onNext(null);
                    results.onCompleted();
                });
        return results;
    }

    @Override
    public Observable<DataPoint<Long>> findCounterData(MetricId id, long start, long end) {
        return time(counterReadLatency, () ->
                dataAccess.findCounterData(id, start, end)
                        .flatMap(Observable::from)
                        .map(Functions::getCounterDataPoint));
    }

    @Override
    public Observable<DataPoint<Double>> findRateData(MetricId id, long start, long end) {
        MetricId rateId = new MetricId(id.getTenantId(), COUNTER_RATE, id.getName(), id.getInterval());
        return time(gaugeReadLatency, () ->
                dataAccess.findData(rateId, start, end)
                        .flatMap(Observable::from)
                        .map(Functions::getGaugeDataPoint));
    }

    @Override
    public Observable<DataPoint<Double>> findGaugeData(MetricId id, Long start, Long end) {
        // When we implement date partitioning, dpart will have to be determined based on
        // the start and end params. And it is possible the the date range spans multiple
        // date partitions.
        return time(gaugeReadLatency, () ->
                dataAccess.findData(id, start, end)
                        .flatMap(Observable::from)
                        .map(Functions::getGaugeDataPoint));
    }

    @Override
    public <T> Observable<T> findGaugeData(MetricId id, Long start, Long end,
            Func1<Observable<DataPoint<Double>>, Observable<T>>... funcs) {

        Observable<DataPoint<Double>> dataCache = findGaugeData(id, start, end).cache();
        return Observable.from(funcs).flatMap(fn -> fn.call(dataCache));
    }

    @Override
    public Observable<List<GaugeBucketPoint>> findGaugeStats(MetricId metricId, long start, long end, Buckets buckets) {
        return findGaugeData(metricId, start, end)
                .groupBy(dataPoint -> buckets.getIndex(dataPoint.getTimestamp()))
                .flatMap(group -> group.collect(() -> new GaugeDataPointCollector(buckets, group.getKey()),
                        GaugeDataPointCollector::increment))
                .map(GaugeDataPointCollector::toBucketPoint)
                .toMap(GaugeBucketPoint::getStart)
                .map(pointMap -> GaugeBucketPoint.toList(pointMap, buckets));
    }

    @Override
    public Observable<DataPoint<AvailabilityType>> findAvailabilityData(MetricId id, long start,
            long end) {
        return findAvailabilityData(id, start, end, false);
    }

    @Override
    public Observable<DataPoint<AvailabilityType>> findAvailabilityData(MetricId id, long start,
            long end, boolean distinct) {
        return time(availabilityReadLatency, () -> {
            Observable<DataPoint<AvailabilityType>> availabilityData = dataAccess.findAvailabilityData(id,
                    start, end)
                    .flatMap(Observable::from)
                    .map(Functions::getAvailabilityDataPoint);
            if (distinct) {
                return availabilityData.distinctUntilChanged(DataPoint::getValue);
            } else {
                return availabilityData;
            }
        });
    }

    @Override
    public Observable<List<AvailabilityBucketPoint>> findAvailabilityStats(MetricId metricId, long start, long end,
                                                                           Buckets buckets) {
        return findAvailabilityData(metricId, start, end)
                .groupBy(dataPoint -> buckets.getIndex(dataPoint.getTimestamp()))
                .flatMap(group -> group.collect(() -> new AvailabilityDataPointCollector(buckets, group.getKey()),
                        AvailabilityDataPointCollector::increment))
                .map(AvailabilityDataPointCollector::toBucketPoint)
                .toMap(AvailabilityBucketPoint::getStart)
                .map(pointMap -> AvailabilityBucketPoint.toList(pointMap, buckets));
    }

    @Override
    public Observable<Boolean> idExists(final String id) {
        return dataAccess.findAllGaugeMetrics().flatMap(Observable::from)
                .filter(row -> id.equals(row.getString(2)))
                .take(1)
                .map(r -> Boolean.TRUE)
                .defaultIfEmpty(Boolean.FALSE);
    }

    @Override
    // TODO refactor to support multiple metrics
    // Data for different metrics and for the same tag are stored within the same partition
    // in the tags table; therefore, it makes sense for the API to support tagging multiple
    // metrics since they could efficiently be inserted in a single batch statement.
    public Observable<Void> tagGaugeData(Metric<Double> metric, final Map<String, String> tags, long start,
            long end) {
        Observable<ResultSet> findDataObservable = dataAccess.findData(metric.getId(), start, end, true);
        return tagGaugeData(findDataObservable, tags, metric);
    }

    private Observable<Void> tagGaugeData(Observable<ResultSet> findDataObservable, Map<String, String> tags,
            Metric<Double> metric) {
        int ttl = getTTL(metric);
        Observable<Map.Entry<String, String>> tagsObservable = Observable.from(tags.entrySet()).cache();
        Observable<TTLDataPoint<Double>> dataPoints = findDataObservable.flatMap(Observable::from)
                .map(row -> getTTLGaugeDataPoint(row, ttl))
                .cache();

        Observable<ResultSet> tagInsert = tagsObservable
                .flatMap(t -> dataAccess.insertGaugeTag(t.getKey(), t.getValue(), metric, dataPoints));

        Observable<ResultSet> tagsInsert = dataPoints
                .flatMap(g -> dataAccess.updateDataWithTag(metric, g.getDataPoint(), tags));

        return tagInsert.concatWith(tagsInsert).map(r -> null);
    }

    private Observable<Void> tagAvailabilityData(Observable<ResultSet> findDataObservable, Map<String, String> tags,
            Metric<AvailabilityType> metric) {
        int ttl = getTTL(metric);
        Observable<Map.Entry<String, String>> tagsObservable = Observable.from(tags.entrySet()).cache();
        Observable<TTLDataPoint<AvailabilityType>> dataPoints = findDataObservable.flatMap(Observable::from)
                .map(row -> getTTLAvailabilityDataPoint(row, ttl))
                .cache();

        Observable<ResultSet> tagInsert = tagsObservable
                .flatMap(t -> dataAccess.insertAvailabilityTag(t.getKey(), t.getValue(), metric, dataPoints));

        Observable<ResultSet> tagsInsert = dataPoints
                .flatMap(a -> dataAccess.updateDataWithTag(metric, a.getDataPoint(), tags));

        return tagInsert.concatWith(tagsInsert).map(r -> null);
    }

    @Override
    public Observable<Void> tagAvailabilityData(Metric<AvailabilityType> metric, Map<String, String> tags,
            long start, long end) {
        Observable<ResultSet> findDataObservable = dataAccess.findAvailabilityData(metric, start, end, true);
        return tagAvailabilityData(findDataObservable, tags, metric);
    }

    @Override
    public Observable<Void> tagGaugeData(Metric<Double> metric, final Map<String, String> tags,
            long timestamp) {
        Observable<ResultSet> findDataObservable = dataAccess.findData(metric, timestamp, true);
        return tagGaugeData(findDataObservable, tags, metric);
    }

    @Override
    public Observable<Void> tagAvailabilityData(Metric<AvailabilityType> metric, Map<String, String> tags,
            long timestamp) {
        Observable<ResultSet> findDataObservable = dataAccess.findAvailabilityData(metric, timestamp);
        return tagAvailabilityData(findDataObservable, tags, metric);
    }

    @Override
    public Observable<Map<MetricId, Set<DataPoint<Double>>>> findGaugeDataByTags(String tenantId,
            Map<String, String> tags) {
        return Observable.from(tags.entrySet())
                .flatMap(e -> dataAccess.findGaugeDataByTag(tenantId, e.getKey(), e.getValue()))
                .map(TaggedGaugeDataPointMapper::apply)
                .toList()
                .map(new MergeDataPointTagsFunction<>());
    }

    @Override
    public Observable<Map<MetricId, Set<DataPoint<AvailabilityType>>>> findAvailabilityByTags(String tenantId,
            Map<String, String> tags) {

        return Observable.from(tags.entrySet())
                .flatMap(e -> dataAccess.findAvailabilityByTag(tenantId, e.getKey(), e.getValue()))
                .map(TaggedAvailabilityDataPointMapper::apply)
                .toList()
                .map(new MergeDataPointTagsFunction<>());
    }

    @Override
    public Observable<List<long[]>> getPeriods(MetricId id, Predicate<Double> predicate,
            long start, long end) {
        return dataAccess.findData(new Metric<>(id), start, end,
                Order.ASC)
                .flatMap(Observable::from)
                .map(Functions::getGaugeDataPoint)
                .toList().map(data -> {
                    List<long[]> periods = new ArrayList<>(data.size());
                    long[] period = null;
                    DataPoint<Double> previous = null;
                    for (DataPoint<Double> d : data) {
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
                });
    }

    private int getTTL(Metric metric) {
        Integer ttl = dataRetentions.get(new DataRetentionKey(metric.getId()));
        if (ttl == null) {
            ttl = dataRetentions.get(new DataRetentionKey(metric.getTenantId(), metric.getType()));
            if (ttl == null) {
                ttl = DEFAULT_TTL;
            }
        }
        return ttl;
    }

    public void shutdown() {
        metricsTasks.shutdown();
        unloadDataRetentions();
    }

    private <T> T time(Timer timer, Callable<T> callable) {
        try {
            // TODO Should this method always return an observable?
            // If so, than we should return Observable.error(e) in the catch block
            return timer.time(callable);
        } catch (Exception e) {
            throw new RuntimeException("There was an error during a timed event", e);
        }
    }

}
