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

import static java.util.Collections.emptyMap;
import static java.util.Comparator.comparingLong;

import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.hawkular.metrics.core.api.MetricType.COUNTER_RATE;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;
import static org.hawkular.metrics.core.impl.Functions.isValidTagMap;
import static org.hawkular.metrics.core.impl.Functions.makeSafe;
import static org.joda.time.Duration.standardMinutes;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.hawkular.metrics.core.api.AvailabilityBucketPoint;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.GaugeBucketPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.api.Retention;
import org.hawkular.metrics.core.api.Tenant;
import org.hawkular.metrics.core.api.TenantAlreadyExistsException;
import org.hawkular.metrics.core.impl.log.CoreLogger;
import org.hawkular.metrics.core.impl.log.CoreLogging;
import org.hawkular.metrics.core.impl.transformers.ItemsToSetTransformer;
import org.hawkular.metrics.core.impl.transformers.MetricsIndexRowTransformer;
import org.hawkular.metrics.core.impl.transformers.TagsIndexRowTransformer;
import org.hawkular.metrics.schema.SchemaManager;
import org.hawkular.metrics.tasks.api.TaskScheduler;
import org.hawkular.rx.cassandra.driver.RxUtil;
import org.joda.time.Duration;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
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
import rx.functions.Func2;
import rx.functions.Func3;

/**
 * @author John Sanda
 */
public class MetricsServiceImpl implements MetricsService, TenantsService {
    private static final CoreLogger log = CoreLogging.getCoreLogger(MetricsServiceImpl.class);

    /**
     * In seconds.
     */
    public static final int DEFAULT_TTL = Duration.standardDays(7).toStandardSeconds().getSeconds();

    public static final String SYSTEM_TENANT_ID = makeSafe("system");

    private static class DataRetentionKey {
        private final MetricId<?> metricId;

        public DataRetentionKey(String tenantId, MetricType<?> type) {
            metricId = new MetricId<>(tenantId, type, makeSafe(type.getText()));
        }

        public DataRetentionKey(MetricId<?> metricId) {
            this.metricId = metricId;
        }

        public DataRetentionKey(Metric<?> metric) {
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

    private DateTimeService dateTimeService;

    private MetricRegistry metricRegistry;

    /**
     * Functions used to insert metric data points.
     */
    private Map<MetricType<?>, Func2<? extends Metric<?>, Integer, Observable<Integer>>> dataPointInserters;

    /**
     * Measurements of the throughput of inserting data points.
     */
    private Map<MetricType<?>, Meter> dataPointInsertMeters;

    /**
     * Measures the latency of queries for data points.
     */
    private Map<MetricType<?>, Timer> dataPointReadTimers;

    /**
     * Functions used to find metric data points.
     */
    private Map<MetricType<?>, Func3<? extends MetricId<?>, Long, Long, Observable<ResultSet>>> dataPointFinders;

    /**
     * Functions used to transform a row into a data point object.
     */
    private Map<MetricType<?>, Func1<Row, ? extends DataPoint<?>>> dataPointMappers;

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
        log.infoKeyspaceUsed(keyspace);
        metricsTasks = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));
        loadDataRetentions();

        this.metricRegistry = metricRegistry;

        dataPointInserters = ImmutableMap.<MetricType<?>, Func2<? extends Metric<?>, Integer,
                Observable<Integer>>>builder()
                .put(GAUGE, (metric, ttl) -> {
                    @SuppressWarnings("unchecked")
                    Metric<Double> gauge = (Metric<Double>) metric;
                    return dataAccess.insertGaugeData(gauge, ttl);
                })
                .put(AVAILABILITY, (metric, ttl) -> {
                    @SuppressWarnings("unchecked")
                    Metric<AvailabilityType> avail = (Metric<AvailabilityType>) metric;
                    return dataAccess.insertAvailabilityData(avail, ttl);
                })
                .put(COUNTER, (metric, ttl) -> {
                    @SuppressWarnings("unchecked")
                    Metric<Long> counter = (Metric<Long>) metric;
                    return dataAccess.insertCounterData(counter, ttl);
                })
                .put(COUNTER_RATE, (metric, ttl) -> {
                    @SuppressWarnings("unchecked")
                    Metric<Double> gauge = (Metric<Double>) metric;
                    return dataAccess.insertGaugeData(gauge, ttl);
                })
                .build();

        dataPointFinders = ImmutableMap.<MetricType<?>, Func3<? extends MetricId<?>, Long, Long,
                Observable<ResultSet>>>builder()
                .put(GAUGE, dataAccess::findData)
                .put(AVAILABILITY, dataAccess::findAvailabilityData)
                .put(COUNTER, dataAccess::findCounterData)
                .build();

        dataPointMappers = ImmutableMap.<MetricType<?>, Func1<Row, ? extends DataPoint<?>>>builder()
                .put(GAUGE, Functions::getGaugeDataPoint)
                .put(AVAILABILITY, Functions::getAvailabilityDataPoint)
                .put(COUNTER, Functions::getCounterDataPoint)
                .build();

        initMetrics();
    }

    void loadDataRetentions() {
        List<String> tenantIds = loadTenantIds();
        CountDownLatch latch = new CountDownLatch(tenantIds.size() * 2);
        for (String tenantId : tenantIds) {
            DataRetentionsMapper gaugeMapper = new DataRetentionsMapper(tenantId, GAUGE);
            DataRetentionsMapper availMapper = new DataRetentionsMapper(tenantId, AVAILABILITY);
            ResultSetFuture gaugeFuture = dataAccess.findDataRetentions(tenantId, GAUGE);
            ResultSetFuture availabilityFuture = dataAccess.findDataRetentions(tenantId, AVAILABILITY);
            ListenableFuture<Set<Retention>> gaugeRetentions = Futures.transform(gaugeFuture, gaugeMapper,
                    metricsTasks);
            ListenableFuture<Set<Retention>> availabilityRetentions = Futures.transform(availabilityFuture, availMapper,
                    metricsTasks);
            Futures.addCallback(gaugeRetentions,
                    new DataRetentionsLoadedCallback(tenantId, GAUGE, latch));
            Futures.addCallback(availabilityRetentions, new DataRetentionsLoadedCallback(tenantId, AVAILABILITY,
                    latch));
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
        dataPointInsertMeters = ImmutableMap.<MetricType<?>, Meter>builder()
                .put(GAUGE, metricRegistry.meter("gauge-inserts"))
                .put(AVAILABILITY, metricRegistry.meter("availability-inserts"))
                .put(COUNTER, metricRegistry.meter("counter-inserts"))
                .put(COUNTER_RATE, metricRegistry.meter("gauge-inserts"))
                .build();
        dataPointReadTimers = ImmutableMap.<MetricType<?>, Timer>builder()
                .put(GAUGE, metricRegistry.timer("gauge-read-latency"))
                .put(AVAILABILITY, metricRegistry.timer("availability-read-latency"))
                .put(COUNTER, metricRegistry.timer("counter-read-latency"))
                .build();
    }

    private static class MergeDataPointTagsFunction<T> implements
            Func1<List<Map<MetricId<T>, Set<DataPoint<T>>>>, Map<MetricId<T>, Set<DataPoint<T>>>> {

        @Override
        public Map<MetricId<T>, Set<DataPoint<T>>> call(List<Map<MetricId<T>, Set<DataPoint<T>>>> taggedDataMaps) {
            if (taggedDataMaps.isEmpty()) {
                return emptyMap();
            }
            if (taggedDataMaps.size() == 1) {
                return taggedDataMaps.get(0);
            }

            Set<MetricId<T>> ids = new HashSet<>(taggedDataMaps.get(0).keySet());
            for (int i = 1; i < taggedDataMaps.size(); ++i) {
                ids.retainAll(taggedDataMaps.get(i).keySet());
            }

            Map<MetricId<T>, Set<DataPoint<T>>> mergedDataMap = new HashMap<>();
            for (MetricId<T> id : ids) {
                TreeSet<DataPoint<T>> set = new TreeSet<>(comparingLong(DataPoint::getTimestamp));
                for (Map<MetricId<T>, Set<DataPoint<T>>> taggedDataMap : taggedDataMaps) {
                    set.addAll(taggedDataMap.get(id));
                }
                mergedDataMap.put(id, set);
            }

            return mergedDataMap;
        }
    }

    private class DataRetentionsLoadedCallback implements FutureCallback<Set<Retention>> {

        private final String tenantId;

        private final MetricType<?> type;

        private final CountDownLatch latch;

        public DataRetentionsLoadedCallback(String tenantId, MetricType<?> type, CountDownLatch latch) {
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
            log.warnDataRetentionLoadingFailure(tenantId, type, t);
            latch.countDown();
            // TODO We probably should not let initialization proceed on this error (then change log level to FATAL)
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
    public void setDataAccess(DataAccess dataAccess) {
        this.dataAccess = dataAccess;
    }

    public void setTaskScheduler(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    public void setDateTimeService(DateTimeService dateTimeService) {
        this.dateTimeService = dateTimeService;
    }

    @Override
    public Observable<Void> createTenant(final Tenant tenant) {
        return Observable.create(subscriber -> {
            Observable<Void> updates = dataAccess.insertTenant(tenant).flatMap(resultSet -> {
                if (!resultSet.wasApplied()) {
                    throw new TenantAlreadyExistsException(tenant.getId());
                }

//                Trigger trigger = new RepeatingTrigger.Builder()
//                        .withDelay(1, MINUTES)
//                        .withInterval(1, MINUTES)
//                        .build();
//                Map<String, String> params = ImmutableMap.of("tenant", tenant.getId());
//                Observable<Void> ratesScheduled = taskScheduler.scheduleTask("generate-rates", tenant.getId(),
//                        100, params, trigger).map(task -> null);

                Observable<Void> retentionUpdates = Observable.from(tenant.getRetentionSettings().entrySet())
                        .flatMap(entry -> dataAccess.updateRetentionsIndex(tenant.getId(), entry.getKey(),
                                ImmutableMap.of(makeSafe(entry.getKey().getText()), entry.getValue())))
                        .map(rs -> null);

//                return ratesScheduled.concatWith(retentionUpdates);
                return retentionUpdates;
            });
            updates.subscribe(resultSet -> {
            }, subscriber::onError, subscriber::onCompleted);
        });
    }

    @Override
    public Observable<Void> createTenants(long creationTime, Observable<String> tenantIds) {
//        return tenantIds.flatMap(tenantId -> dataAccess.insertTenant(tenantId).flatMap(resultSet -> {
//            Trigger trigger = new RepeatingTrigger.Builder()
//                    .withDelay(1, MINUTES)
//                    .withInterval(1, MINUTES)
//                    .build();
//            Map<String, String> params = ImmutableMap.of(
//                    "tenant", tenantId,
//                    "creationTime", Long.toString(creationTime));
//
//            return taskScheduler.scheduleTask("generate-rates", tenantId, 100, params, trigger).map(task -> null);
//        }));

        return tenantIds.flatMap(tenantId -> dataAccess.insertTenant(tenantId).map(resultSet -> null));
    }

    @Override
    public Observable<Tenant> getTenants() {
        return dataAccess.findAllTenantIds()
                .flatMap(Observable::from)
                .map(row -> row.getString(0))
                .distinct()
                .flatMap(id ->
                                dataAccess.findTenant(id)
                                        .flatMap(Observable::from)
                                        .map(Functions::getTenant)
                                        .switchIfEmpty(Observable.just(new Tenant(id)))
                );
    }

    private List<String> loadTenantIds() {
        Iterable<String> tenantIds = dataAccess.findAllTenantIds()
                .flatMap(Observable::from)
                .map(row -> row.getString(0))
                .distinct()
                .toBlocking()
                .toIterable();
        return ImmutableList.copyOf(tenantIds);
    }

    @Override
    public Observable<Void> createMetric(Metric<?> metric) {
        MetricType<?> metricType = metric.getId().getType();
        if (!metricType.isUserType()) {
            throw new IllegalArgumentException(metric + " cannot be created. " + metricType + " metrics are " +
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
                updates.add(dataAccess.addDataRetention(metric));
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
    public <T> Observable<Metric<T>> findMetric(final MetricId<T> id) {
        return dataAccess.findMetric(id)
                .flatMap(Observable::from)
                .map(row -> new Metric<>(id, row.getMap(2, String.class, String.class), row.getInt(3)));
    }

    @Override
    public <T> Observable<Metric<T>> findMetrics(String tenantId, MetricType<T> metricType) {
        if (metricType == null) {
            return Observable.from(MetricType.userTypes())
                    .map(type -> {
                        @SuppressWarnings("unchecked")
                        MetricType<T> t = (MetricType<T>) type;
                        return t;
                    })
                    .flatMap(type -> dataAccess.findMetricsInMetricsIndex(tenantId, type)
                            .flatMap(Observable::from)
                            .compose(new MetricsIndexRowTransformer<>(tenantId, type)));
        }
        return dataAccess.findMetricsInMetricsIndex(tenantId, metricType)
                .flatMap(Observable::from)
                .compose(new MetricsIndexRowTransformer<>(tenantId, metricType));
    }

    @Override
    public <T> Observable<Metric<T>> findMetricsWithFilters(String tenantId, Map<String, String> tagsQueries,
                                                            MetricType<T> metricType) {
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
                                    .compose(new TagsIndexRowTransformer<>(tenantId, metricType))
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
     *
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
    public Observable<Optional<Map<String, String>>> getMetricTags(MetricId<?> id) {
        Observable<ResultSet> metricTags = dataAccess.getMetricTags(id);

        return metricTags.flatMap(Observable::from).take(1).map(row -> Optional.of(row.getMap(0, String.class, String
                .class)))
                .defaultIfEmpty(Optional.empty());
    }

    // Adding/deleting metric tags currently involves writing to three tables - data,
    // metrics_idx, and metrics_tags_idx. It might make sense to refactor tag related
    // functionality into a separate class.
    @Override
    public Observable<Void> addTags(Metric<?> metric, Map<String, String> tags) {
        try {
            checkArgument(tags != null, "Missing tags");
            checkArgument(isValidTagMap(tags), "Invalid tags; tag key is required");
        } catch (Exception e) {
            return Observable.error(e);
        }

        return dataAccess.addTags(metric, tags).mergeWith(dataAccess.insertIntoMetricsTagsIndex(metric, tags))
                .toList().map(l -> null);
    }

    @Override
    public Observable<Void> deleteTags(Metric<?> metric, Map<String, String> tags) {
        return dataAccess.deleteTags(metric, tags.keySet()).mergeWith(
                dataAccess.deleteFromMetricsTagsIndex(metric, tags)).toList().map(r -> null);
    }

    @Override
    public <T> Observable<Void> addDataPoints(MetricType<T> metricType, Observable<Metric<T>> metrics) {
        checkArgument(metricType != null, "metricType is null");

        // We write to both the data and the metrics_idx tables. Each metric can have one or more data points. We
        // currently write a separate batch statement for each metric.
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

        Meter meter = getInsertMeter(metricType);
        Func2<Metric<T>, Integer, Observable<Integer>> inserter = getInserter(metricType);

        Observable<Integer> updates = metrics
                .filter(metric -> !metric.getDataPoints().isEmpty())
                .flatMap(metric -> inserter.call(metric, getTTL(metric.getId())))
                .doOnNext(meter::mark);

        Observable<Integer> tenantUpdates = updateTenantBuckets(metrics);

        Observable<Integer> indexUpdates = dataAccess.updateMetricsIndex(metrics)
                .doOnNext(batchSize -> log.tracef("Inserted %d %s metrics into metrics_idx", batchSize, metricType));
        return Observable.concat(updates, indexUpdates, tenantUpdates)
                .takeLast(1)
                .map(count -> null);
    }

    private <T> Meter getInsertMeter(MetricType<T> metricType) {
        Meter meter = dataPointInsertMeters.get(metricType);
        if (meter == null) {
            throw new UnsupportedOperationException(metricType.getText());
        }
        return meter;
    }

    @SuppressWarnings("unchecked")
    private <T> Func2<Metric<T>, Integer, Observable<Integer>> getInserter(MetricType<T> metricType) {
        Func2<Metric<T>, Integer, Observable<Integer>> inserter;
        inserter = (Func2<Metric<T>, Integer, Observable<Integer>>) dataPointInserters.get(metricType);
        if (inserter == null) {
            throw new UnsupportedOperationException(metricType.getText());
        }
        return inserter;
    }

    private Observable<Integer> updateTenantBuckets(Observable<? extends Metric<?>> metrics) {
        return metrics.flatMap(metric -> {
            return Observable.<DataPoint<?>>from(metric.getDataPoints()).map(dataPoint -> {
                return new TenantBucket(metric.getId().getTenantId(),
                        dateTimeService.getTimeSlice(dataPoint.getTimestamp(), standardMinutes(30)));
            });
        }).distinct().flatMap(tenantBucket -> {
            return dataAccess.insertTenantId(tenantBucket.getBucket(), tenantBucket.getTenant()).map(resultSet -> 0);
        });
    }

    @Override
    public <T> Observable<DataPoint<T>> findDataPoints(MetricId<T> metricId, Long start, Long end) {
        MetricType<T> metricType = metricId.getType();
        Timer timer = getDataPointFindTimer(metricType);
        Func3<MetricId<T>, Long, Long, Observable<ResultSet>> finder = getDataPointFinder(metricType);
        Func1<Row, DataPoint<T>> mapper = getDataPointMapper(metricType);
        return time(timer, () -> finder.call(metricId, start, end)
                .flatMap(Observable::from)
                .map(mapper));
    }

    private <T> Timer getDataPointFindTimer(MetricType<T> metricType) {
        Timer timer = dataPointReadTimers.get(metricType);
        if (timer == null) {
            throw new UnsupportedOperationException(metricType.getText());
        }
        return timer;
    }

    @SuppressWarnings("unchecked")
    private <T> Func3<MetricId<T>, Long, Long, Observable<ResultSet>> getDataPointFinder(MetricType<T> metricType) {
        Func3<MetricId<T>, Long, Long, Observable<ResultSet>> finder;
        finder = (Func3<MetricId<T>, Long, Long, Observable<ResultSet>>) dataPointFinders.get(metricType);
        if (finder == null) {
            throw new UnsupportedOperationException(metricType.getText());
        }
        return finder;
    }

    @SuppressWarnings("unchecked")
    private <T> Func1<Row, DataPoint<T>> getDataPointMapper(MetricType<T> metricType) {
        Func1<Row, DataPoint<T>> mapper = (Func1<Row, DataPoint<T>>) dataPointMappers.get(metricType);
        if (mapper == null) {
            throw new UnsupportedOperationException(metricType.getText());
        }
        return mapper;
    }

    @Override
    public Observable<DataPoint<Double>> findRateData(MetricId<Long> id, long start, long end) {
        return findDataPoints(id, start, end)
                .buffer(2, 1) // emit previous/next pairs
                .filter(l -> l.size() == 2) // drop last buffer
                .map(l -> {
                    DataPoint<Long> point1 = l.get(0);
                    DataPoint<Long> point2 = l.get(1);
                    long timestamp = (point1.getTimestamp() + point2.getTimestamp()) / 2;
                    long value_diff = point2.getValue() - point1.getValue();
                    double time_diff = point2.getTimestamp() - point1.getTimestamp();
                    double rate = 60_000D * value_diff / time_diff;
                    return new DataPoint<>(timestamp, rate);
                });
    }

    @Override
    public <T> Observable<T> findGaugeData(MetricId<Double> id, Long start, Long end,
                                           Func1<Observable<DataPoint<Double>>, Observable<T>>... funcs) {
        Observable<DataPoint<Double>> dataCache = findDataPoints(id, start, end).cache();
        return Observable.from(funcs).flatMap(fn -> fn.call(dataCache));
    }

    @Override
    public Observable<List<GaugeBucketPoint>> findGaugeStats(MetricId<Double> metricId, long start, long end,
                                                             Buckets buckets) {
        return findDataPoints(metricId, start, end)
                .groupBy(dataPoint -> buckets.getIndex(dataPoint.getTimestamp()))
                .flatMap(group -> group.collect(() -> new GaugeDataPointCollector(buckets, group.getKey()),
                        GaugeDataPointCollector::increment))
                .map(GaugeDataPointCollector::toBucketPoint)
                .toMap(GaugeBucketPoint::getStart)
                .map(pointMap -> GaugeBucketPoint.toList(pointMap, buckets));
    }

    @Override
    public Observable<DataPoint<AvailabilityType>> findAvailabilityData(MetricId<AvailabilityType> id, long start,
            long end, boolean distinct) {
        Observable<DataPoint<AvailabilityType>> availabilityData = findDataPoints(id, start, end);
        if (distinct) {
            return availabilityData.distinctUntilChanged(DataPoint::getValue);
        } else {
            return availabilityData;
        }
    }

    @Override
    public Observable<List<AvailabilityBucketPoint>> findAvailabilityStats(MetricId<AvailabilityType> metricId,
                                                                           long start, long end, Buckets buckets) {
        return findDataPoints(metricId, start, end)
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
    public Observable<List<long[]>> getPeriods(MetricId<Double> id, Predicate<Double> predicate, long start, long end) {
        return dataAccess.findData(new Metric<>(id), start, end, Order.ASC)
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

    private int getTTL(MetricId<?> metricId) {
        Integer ttl = dataRetentions.get(new DataRetentionKey(metricId));
        if (ttl == null) {
            ttl = dataRetentions.get(new DataRetentionKey(metricId.getTenantId(), metricId.getType()));
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

    private static class TenantBucket {
        String tenant;
        long bucket;

        public TenantBucket(String tenant, long bucket) {
            this.tenant = tenant;
            this.bucket = bucket;
        }

        public String getTenant() {
            return tenant;
        }

        public long getBucket() {
            return bucket;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TenantBucket that = (TenantBucket) o;
            return Objects.equals(bucket, that.bucket) &&
                    Objects.equals(tenant, that.tenant);
        }

        @Override public int hashCode() {
            return Objects.hash(tenant, bucket);
        }
    }

}
