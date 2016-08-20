/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.metrics.core.service;

import static org.hawkular.metrics.core.service.Functions.isValidTagMap;
import static org.hawkular.metrics.core.service.Functions.makeSafe;
import static org.hawkular.metrics.core.service.Order.ASC;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.COUNTER_RATE;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.hawkular.metrics.model.MetricType.GAUGE_RATE;
import static org.hawkular.metrics.model.MetricType.STRING;
import static org.hawkular.metrics.model.Utils.isValidTimeRange;
import static org.joda.time.Duration.standardSeconds;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.hawkular.metrics.core.service.cache.CacheService;
import org.hawkular.metrics.core.service.log.CoreLogger;
import org.hawkular.metrics.core.service.log.CoreLogging;
import org.hawkular.metrics.core.service.rollup.Rollup;
import org.hawkular.metrics.core.service.rollup.RollupService;
import org.hawkular.metrics.core.service.transformers.ItemsToSetTransformer;
import org.hawkular.metrics.core.service.transformers.MetricsIndexRowTransformer;
import org.hawkular.metrics.core.service.transformers.NumericBucketPointTransformer;
import org.hawkular.metrics.core.service.transformers.TaggedBucketPointTransformer;
import org.hawkular.metrics.core.service.transformers.TagsIndexRowTransformer;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.AvailabilityBucketPoint;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.BucketPoint;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.NamedDataPoint;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.Retention;
import org.hawkular.metrics.model.TaggedBucketPoint;
import org.hawkular.metrics.model.Tenant;
import org.hawkular.metrics.model.exception.MetricAlreadyExistsException;
import org.hawkular.metrics.model.exception.TenantAlreadyExistsException;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.TimeRange;
import org.hawkular.metrics.sysconfig.Configuration;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.functions.Func5;
import rx.observable.ListenableFutureObservable;
import rx.subjects.PublishSubject;

/**
 * @author John Sanda
 */
public class MetricsServiceImpl implements MetricsService {
    private static final CoreLogger log = CoreLogging.getCoreLogger(MetricsServiceImpl.class);

    public static final String SYSTEM_TENANT_ID = makeSafe("sysconfig");

    private static class DataRetentionKey {
        private final MetricId<?> metricId;

        public DataRetentionKey(String tenantId, MetricType<?> type) {
            metricId = new MetricId<>(tenantId, type, makeSafe(type.getText()));
        }

        public DataRetentionKey(MetricId<?> metricId) {
            this.metricId = metricId;
        }

        public DataRetentionKey(Metric<?> metric) {
            this.metricId = metric.getMetricId();
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
    private final PublishSubject<Metric<?>> insertedDataPointEvents = PublishSubject.create();

    private ListeningExecutorService metricsTasks;

    private DataAccess dataAccess;

    private ConfigurationService configurationService;

    private RollupService rollupService;

    private CacheService cacheService;

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
    private Map<MetricType<?>, Func5<? extends MetricId<?>, Long, Long,
            Integer, Order, Observable<Row>>> dataPointFinders;

    /**
     * Functions used to transform a row into a data point object.
     */
    private Map<MetricType<?>, Func1<Row, ? extends DataPoint<?>>> dataPointMappers;

    private int defaultTTL = Duration.standardDays(7).toStandardSeconds().getSeconds();

    private int maxStringSize;

    public void startUp(Session session, String keyspace, boolean resetDb, MetricRegistry metricRegistry) {
        startUp(session, keyspace, resetDb, true, metricRegistry);
    }

    public void startUp(Session session, String keyspace, boolean resetDb, boolean createSchema,
            MetricRegistry metricRegistry) {
        session.execute("USE " + keyspace);
        log.infoKeyspaceUsed(keyspace);
        metricsTasks = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));
        loadDataRetentions();

        this.metricRegistry = metricRegistry;

        dataPointInserters = ImmutableMap
                .<MetricType<?>, Func2<? extends Metric<?>, Integer,
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
                .put(STRING, (metric, ttl) -> {
                    @SuppressWarnings("unchecked")
                    Metric<String> string = (Metric<String>) metric;
                    return dataAccess.insertStringData(string, ttl, maxStringSize);
                })
                .build();

        dataPointFinders = ImmutableMap
                .<MetricType<?>, Func5<? extends MetricId<?>, Long, Long, Integer, Order,
                        Observable<Row>>>builder()
                .put(GAUGE, (metricId, start, end, limit, order) -> {
                    @SuppressWarnings("unchecked")
                    MetricId<Double> gaugeId = (MetricId<Double>) metricId;
                    return dataAccess.findGaugeData(gaugeId, start, end, limit, order);
                })
                .put(AVAILABILITY, (metricId, start, end, limit, order) -> {
                    @SuppressWarnings("unchecked")
                    MetricId<AvailabilityType> availabilityId = (MetricId<AvailabilityType>) metricId;
                    return dataAccess.findAvailabilityData(availabilityId, start, end, limit, order);
                })
                .put(COUNTER, (metricId, start, end, limit, order) -> {
                    @SuppressWarnings("unchecked")
                    MetricId<Long> counterId = (MetricId<Long>) metricId;
                    return dataAccess.findCounterData(counterId, start, end, limit, order);
                })
                .put(STRING, (metricId, start, end, limit, order) -> {
                    @SuppressWarnings("unchecked")
                    MetricId<String> stringId = (MetricId<String>) metricId;
                    return dataAccess.findStringData(stringId, start, end, limit, order);
                })
                .build();

        dataPointMappers = ImmutableMap.<MetricType<?>, Func1<Row, ? extends DataPoint<?>>> builder()
                .put(GAUGE, Functions::getGaugeDataPoint)
                .put(AVAILABILITY, Functions::getAvailabilityDataPoint)
                .put(COUNTER, Functions::getCounterDataPoint)
                .put(STRING, Functions::getStringDataPoint)
                .build();

        initStringSize(session);
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
        dataPointInsertMeters = ImmutableMap.<MetricType<?>, Meter> builder()
                .put(GAUGE, metricRegistry.meter("gauge-inserts"))
                .put(AVAILABILITY, metricRegistry.meter("availability-inserts"))
                .put(COUNTER, metricRegistry.meter("counter-inserts"))
                .put(COUNTER_RATE, metricRegistry.meter("gauge-inserts"))
                .put(STRING, metricRegistry.meter("string-inserts"))
                .build();
        dataPointReadTimers = ImmutableMap.<MetricType<?>, Timer> builder()
                .put(GAUGE, metricRegistry.timer("gauge-read-latency"))
                .put(AVAILABILITY, metricRegistry.timer("availability-read-latency"))
                .put(COUNTER, metricRegistry.timer("counter-read-latency"))
                .put(STRING, metricRegistry.timer("string-read-latency"))
                .build();
    }

    private void initStringSize(Session session) {
        Configuration configuration = configurationService.load("org.hawkular.metrics").toBlocking()
                .lastOrDefault(null);
        if (configuration == null) {
            maxStringSize = -1;  // no size limit
        } else {
            maxStringSize = Integer.parseInt(configuration.get("string-size", "2048"));
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

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setCacheService(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    public void setRollupService(RollupService rollupService) {
        this.rollupService = rollupService;
    }

    public void setDefaultTTL(int defaultTTL) {
        this.defaultTTL = Duration.standardDays(defaultTTL).toStandardSeconds().getSeconds();
    }

    @Override
    public Observable<Void> createTenant(final Tenant tenant, boolean overwrite) {
        return Observable.create(subscriber -> {
            Observable<Void> updates = dataAccess.insertTenant(tenant, overwrite).flatMap(resultSet -> {
                if (!resultSet.wasApplied()) {
                    throw new TenantAlreadyExistsException(tenant.getId());
                }

                Observable<Void> retentionUpdates = Observable.from(tenant.getRetentionSettings().entrySet())
                        .flatMap(entry -> dataAccess.updateRetentionsIndex(tenant.getId(), entry.getKey(),
                                ImmutableMap.of(makeSafe(entry.getKey().getText()), entry.getValue())))
                        .map(rs -> null);

                return retentionUpdates;
            });
            updates.subscribe(resultSet -> {
            }, subscriber::onError, subscriber::onCompleted);
        });
    }

    @Override
    public Observable<Tenant> getTenants() {
        return dataAccess.findAllTenantIds()
                .map(row -> row.getString(0))
                .distinct()
                .flatMap(id ->
                                dataAccess.findTenant(id)
                                        .map(Functions::getTenant)
                                        .switchIfEmpty(Observable.just(new Tenant(id)))
                );
    }

    private List<String> loadTenantIds() {
        Iterable<String> tenantIds = dataAccess.findAllTenantIds()
                .map(row -> row.getString(0))
                .distinct()
                .toBlocking()
                .toIterable();
        return ImmutableList.copyOf(tenantIds);
    }

    @Override
    public Observable<Void> createMetric(Metric<?> metric, boolean overwrite) {
        MetricType<?> metricType = metric.getMetricId().getType();
        if (!metricType.isUserType()) {
            throw new IllegalArgumentException(metric + " cannot be created. " + metricType + " metrics are " +
                    "internally generated metrics and cannot be created by clients.");
        }

        ResultSetFuture future = dataAccess.insertMetricInMetricsIndex(metric, overwrite);
        Observable<ResultSet> indexUpdated = ListenableFutureObservable.from(future, metricsTasks);
        return Observable.create(subscriber -> indexUpdated.subscribe(resultSet -> {
            if (!overwrite && !resultSet.wasApplied()) {
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

    private Observable<ResultSet> updateRetentionsIndex(Metric<?> metric) {
        ResultSetFuture dataRetentionFuture = dataAccess.updateRetentionsIndex(metric);
        Observable<ResultSet> dataRetentionUpdated = ListenableFutureObservable.from(dataRetentionFuture, metricsTasks);
        // TODO Shouldn't we only update dataRetentions map when the retentions index update succeeds?
        dataRetentions.put(new DataRetentionKey(metric), metric.getDataRetention());

        return dataRetentionUpdated;
    }

    @Override
    public <T> Observable<Metric<T>> findMetric(final MetricId<T> id) {
        return dataAccess.findMetric(id)
                .compose(new MetricsIndexRowTransformer<>(id.getTenantId(), id.getType(), defaultTTL));
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
                            .compose(new MetricsIndexRowTransformer<>(tenantId, type, defaultTTL)));
        }
        return dataAccess.findMetricsInMetricsIndex(tenantId, metricType)
                .compose(new MetricsIndexRowTransformer<>(tenantId, metricType, defaultTTL));
    }

    private <T> Observable<Metric<T>> findMetricsWithFilters(String tenantId, MetricType<T> metricType,
                                                            Map<String, String> tagsQueries) {
        // Fetch everything from the tagsQueries
        return Observable.from(tagsQueries.entrySet())
                .flatMap(e -> dataAccess.findMetricsByTagName(tenantId, e.getKey())
                        .filter(tagValueFilter(e.getValue(), 2))
                        .compose(new TagsIndexRowTransformer<>(tenantId, metricType))
                        .compose(new ItemsToSetTransformer<>())
                        .reduce((s1, s2) -> {
                            s1.addAll(s2);
                            return s1;
                        }))
                .reduce((s1, s2) -> {
                    s1.retainAll(s2);
                    return s1;
                })
                .flatMap(Observable::from)
                .flatMap(this::findMetric);
    }

    @Override
    public <T> Observable<Metric<T>> findMetricsWithFilters(String tenantId, MetricType<T> metricType, Map<String,
            String> tagsQueries, Func1<Metric<T>, Boolean>... filters) {
        Observable<Metric<T>> metricObservable = findMetricsWithFilters(tenantId, metricType, tagsQueries);

        for (Func1<Metric<T>, Boolean> filter : filters) {
            metricObservable = metricObservable.filter(filter);
        }

        return metricObservable;
    }

    private Func1<Row, Boolean> tagValueFilter(String regexp, int index) {
        boolean positive = (!regexp.startsWith("!"));
        Pattern p = PatternUtil.filterPattern(regexp);
        return r -> positive == p.matcher(r.getString(index)).matches(); // XNOR
    }

    public <T> Func1<Metric<T>, Boolean> idFilter(String regexp) {
        boolean positive = (!regexp.startsWith("!"));
        Pattern p = PatternUtil.filterPattern(regexp);
        return tMetric -> positive == p.matcher(tMetric.getId()).matches();
    }

    public Func1<Row, Boolean> typeFilter(MetricType<?> type) {
        return row -> {
            MetricType<?> metricType = MetricType.fromCode(row.getByte(0));
            return (type == null && metricType.isUserType()) || metricType == type;
        };
    }

    @Override
    public Observable<Map<String, Set<String>>> getTagValues(String tenantId, MetricType<?> metricType,
                                    Map<String, String> tagsQueries) {

        // Row: 0 = type, 1 = metricName, 2 = tagValue, e.getKey = tagName, e.getValue = regExp
        return Observable.from(tagsQueries.entrySet())
                .flatMap(e -> dataAccess.findMetricsByTagName(tenantId, e.getKey())
                        .filter(typeFilter(metricType))
                        .filter(tagValueFilter(e.getValue(), 2))
                        .map(row -> {
                            Map<String, Map<String, String>> idMap = new HashMap<>();
                            Map<String, String> valueMap = new HashMap<>();
                            valueMap.put(e.getKey(), row.getString(2));

                            idMap.put(row.getString(1), valueMap);
                            return idMap;
                        })
                        .switchIfEmpty(Observable.just(new HashMap<>()))
                        .reduce((map1, map2) -> {
                            map1.putAll(map2);
                            return map1;
                        }))
                .reduce((m1, m2) -> {
                    // Now try to emulate set operation of cut
                    Iterator<Map.Entry<String, Map<String, String>>> iterator = m1.entrySet().iterator();

                    while (iterator.hasNext()) {
                        Map.Entry<String, Map<String, String>> next = iterator.next();
                        if (!m2.containsKey(next.getKey())) {
                            iterator.remove();
                        } else {
                            // Combine the entries
                            Map<String, String> map2 = m2.get(next.getKey());
                            map2.forEach((k, v) -> next.getValue().put(k, v));
                        }
                    }

                    return m1;
                })
                .map(m -> {
                    Map<String, Set<String>> tagValueMap = new HashMap<>();

                    m.forEach((k, v) ->
                            v.forEach((subKey, subValue) -> {
                                if (tagValueMap.containsKey(subKey)) {
                                    tagValueMap.get(subKey).add(subValue);
                                } else {
                                    Set<String> values = new HashSet<>();
                                    values.add(subValue);
                                    tagValueMap.put(subKey, values);
                                }
                            }));

                    return tagValueMap;
                });
    }

    @Override
    public Observable<Map<String, String>> getMetricTags(MetricId<?> id) {
        return dataAccess.getMetricTags(id)
                .take(1)
                .map(row -> row.getMap(0, String.class, String.class))
                .defaultIfEmpty(new HashMap<>());
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
    public Observable<Void> deleteTags(Metric<?> metric, Set<String> tags) {
        return getMetricTags(metric.getMetricId())
                .map(loadedTags -> {
                    loadedTags.keySet().retainAll(tags);
                    return loadedTags;
                })
                .flatMap(tagsToDelete -> {
                    return dataAccess.deleteTags(metric, tagsToDelete.keySet()).mergeWith(
                            dataAccess.deleteFromMetricsTagsIndex(metric, tagsToDelete)).toList().map(r -> null);
                });
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
                .flatMap(metric -> inserter.call(metric, getTTL(metric.getMetricId()))
                        .doOnNext(i -> insertedDataPointEvents.onNext(metric)))
                .doOnNext(meter::mark);

        Observable<DataPoint<? extends Number>> cacheUpdates;
//        if (metricType == GAUGE) {
//            cacheUpdates = metrics
//                    .flatMap(metric -> Observable.from(metric.getDataPoints())
//                            .subscribeOn(Schedulers.computation())
//                            .map(dataPoint -> cacheService.put((MetricId<Double>) metric.getMetricId(),
//                                    (DataPoint<Double>) dataPoint)))
//                    .flatMap(Single::toObservable);
//        } else {
//            cacheUpdates = Observable.empty();
//        }
        cacheUpdates = Observable.empty();

        Observable<Integer> indexUpdates = dataAccess.updateMetricsIndex(metrics)
                .doOnNext(batchSize -> log.tracef("Inserted %d %s metrics into metrics_idx", batchSize, metricType));

        return Observable.merge(updates, cacheUpdates, indexUpdates).map(i -> null);
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

    @Override
    public <T> Observable<DataPoint<T>> findDataPoints(MetricId<T> metricId, long start, long end, int limit,
            Order order) {
        checkArgument(isValidTimeRange(start, end), "Invalid time range");
        MetricType<T> metricType = metricId.getType();
        Timer timer = getDataPointFindTimer(metricType);
        Func5<MetricId<T>, Long, Long, Integer, Order, Observable<Row>> finder = getDataPointFinder(metricType);
        Func1<Row, DataPoint<T>> mapper = getDataPointMapper(metricType);
        return time(timer, () -> finder.call(metricId, start, end, limit, order)
                .map(mapper));
    }

    @Override
    public <T> Observable<NamedDataPoint<T>> findDataPoints(List<MetricId<T>> metricIds, long start,
            long end, int limit, Order order) {
        return Observable.from(metricIds)
                .concatMap(id -> findDataPoints(id, start, end, limit, order)
                        .map(dataPoint -> new NamedDataPoint<>(id.getName(), dataPoint)));
    }

    private <T> Timer getDataPointFindTimer(MetricType<T> metricType) {
        Timer timer = dataPointReadTimers.get(metricType);
        if (timer == null) {
            throw new UnsupportedOperationException(metricType.getText());
        }
        return timer;
    }

    @SuppressWarnings("unchecked")
    private <T> Func5<MetricId<T>, Long, Long, Integer, Order, Observable<Row>> getDataPointFinder(
            MetricType<T> metricType) {
        Func5<MetricId<T>, Long, Long, Integer, Order, Observable<Row>> finder;
        finder = (Func5<MetricId<T>, Long, Long, Integer, Order, Observable<Row>>) dataPointFinders
                .get(metricType);
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
    public Observable<DataPoint<Double>> findRateData(MetricId<? extends Number> id, long start, long end, int limit,
                                                      Order order) {
        checkArgument(isValidTimeRange(start, end), "Invalid time range");
        checkArgument(id.getType() == COUNTER || id.getType() == GAUGE, "Unsupported metric type: %s", id.getType());
        // We can't set the limit here, because some pairs can be discarded (counter resets)
        // But since the loading is reactive, we're not going to fetch more pages than needed (see #take at the end)
        Observable<DataPoint<Double>> dataPoints = this.findDataPoints(id, start, end, 0, order)
                .buffer(2, 1) // emit previous/next pairs
                // adapt pair to the order of traversal
                .map(l -> order == ASC ? l : Lists.reverse(l))
                // Drop the last buffer
                .filter(l -> l.size() == 2)
                // Filter out counter resets
                .filter(l -> id.getType() != COUNTER
                        || l.get(1).getValue().longValue() >= l.get(0).getValue().longValue())
                .map(l -> {
                    DataPoint<? extends Number> point1 = l.get(0);
                    DataPoint<? extends Number> point2 = l.get(1);
                    long timestamp = point2.getTimestamp();
                    double value_diff = point2.getValue().doubleValue() - point1.getValue().doubleValue();
                    double time_diff = point2.getTimestamp() - point1.getTimestamp();
                    double rate = 60_000D * value_diff / time_diff;
                    return new DataPoint<>(timestamp, rate);
                });
        return limit <= 0 ? dataPoints : dataPoints.take(limit);
    }

    public Observable<NamedDataPoint<Double>> findRateData(List<MetricId<? extends Number>> ids, long start,
            long end, int limit, Order order) {
        return Observable.from(ids).concatMap(id -> findRateData(id, start, end, limit, order)
                .map(dataPoint -> new NamedDataPoint<>(id.getName(), dataPoint)));
    }

    @Override
    public Observable<List<NumericBucketPoint>> findRateStats(MetricId<? extends Number> id, long start, long end,
                                                              Buckets buckets, List<Percentile> percentiles) {
        checkArgument(isValidTimeRange(start, end), "Invalid time range");
        return findRateData(id, start, end, 0, ASC)
                .compose(new NumericBucketPointTransformer(buckets, percentiles));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Observable<T> findGaugeData(MetricId<Double> id, long start, long end,
                                           Func1<Observable<DataPoint<Double>>, Observable<T>>... funcs) {
        Observable<DataPoint<Double>> dataCache = this.findDataPoints(id, start, end, 0, Order.DESC).cache();
        return Observable.from(funcs).flatMap(fn -> fn.call(dataCache));
    }

    @Override
    public Observable<List<NumericBucketPoint>> findGaugeStats(MetricId<Double> metricId, BucketConfig bucketConfig,
                List<Percentile> percentiles) {
        TimeRange timeRange = bucketConfig.getTimeRange();
        checkArgument(isValidTimeRange(timeRange.getStart(), timeRange.getEnd()), "Invalid time range");

        if (bucketConfig.getBuckets() != null) {
            return findDataPoints(metricId, timeRange.getStart(), timeRange.getEnd(), 0, Order.DESC)
                    .compose(new NumericBucketPointTransformer(bucketConfig.getBuckets(), percentiles));
        } else if (bucketConfig.getResolution() != null) {
            return rollupService.find(metricId, timeRange.getStart(), timeRange.getEnd(), bucketConfig.getResolution())
                    .toList();
        } else {
            DateTimeComparator comparator = DateTimeComparator.getInstance();
            Duration rawTTL = standardSeconds(getTTL(metricId));
            List<Rollup> rollups = rollupService.getRollups(metricId, (int) rawTTL.getStandardSeconds())
                    .toBlocking().firstOrDefault(null);

            if (isWithinRetentionPeriod((int) rawTTL.getStandardSeconds(), timeRange.getStart())) {
                Buckets buckets = Buckets.fromCount(timeRange.getStart(), timeRange.getEnd(), 60);
                return findDataPoints(metricId, timeRange.getStart(), timeRange.getEnd(), 0, Order.DESC)
                        .compose(new NumericBucketPointTransformer(buckets, percentiles));
            }
            for (Rollup rollup : rollups) {
                if (isWithinRetentionPeriod(rollup.getDefaultTTL(), timeRange.getStart())) {
                    return rollupService.find(metricId, timeRange.getStart(), timeRange.getEnd(),
                            rollup.getResolution()).toList();
                }
            }
            return Observable.empty();
        }
    }

    private boolean isWithinRetentionPeriod(int ttl, long startTime) {
        DateTimeComparator comparator = DateTimeComparator.getInstance();
        return comparator.compare(DateTimeService.now.get().minusSeconds(ttl), new DateTime(startTime)) <= 0;
    }

    private List<Percentile> getPercentiles(Map<Float, Double> map) {
        return map.entrySet().stream().map(entry -> new Percentile(entry.getKey().toString(), entry.getValue()))
                .collect(Collectors.toList());
    }

    @Override
    public Observable<Map<String, TaggedBucketPoint>> findGaugeStats(MetricId<Double> metricId,
            Map<String, String> tags, long start, long end, List<Percentile> percentiles) {
        return findDataPoints(metricId, start, end, 0, Order.DESC)
                .compose(new TaggedBucketPointTransformer(tags, percentiles));
    }

    @Override
    public <T extends Number> Observable<List<NumericBucketPoint>> findNumericStats(String tenantId,
            MetricType<T> metricType, Map<String, String> tagFilters, long start, long end, Buckets buckets,
            List<Percentile> percentiles, boolean stacked) {

        checkArgument(isValidTimeRange(start, end), "Invalid time range");
        checkArgument(metricType == GAUGE || metricType == GAUGE_RATE
                || metricType == COUNTER || metricType == COUNTER_RATE, "Invalid metric type: %s", metricType);

        if (!stacked) {
            if (COUNTER == metricType || GAUGE == metricType) {
                return findMetricsWithFilters(tenantId, metricType, tagFilters)
                        .flatMap(metric -> findDataPoints(metric.getMetricId(), start, end, 0, Order.DESC))
                        .compose(new NumericBucketPointTransformer(buckets, percentiles));
            } else {
                MetricType<? extends Number> mtype = metricType == GAUGE_RATE ? GAUGE : COUNTER;
                return findMetricsWithFilters(tenantId, mtype, tagFilters)
                        .flatMap(metric -> findRateData(metric.getMetricId(), start, end, 0, ASC))
                        .compose(new NumericBucketPointTransformer(buckets, percentiles));
            }
        } else {
            Observable<Observable<NumericBucketPoint>> individualStats;
            if (COUNTER == metricType || GAUGE == metricType) {
                individualStats = findMetricsWithFilters(tenantId, metricType, tagFilters)
                        .map(metric -> {
                            return findDataPoints(metric.getMetricId(), start, end, 0, Order.DESC)
                                    .compose(new NumericBucketPointTransformer(buckets, percentiles))
                                    .flatMap(Observable::from);
                        });
            } else {
                MetricType<? extends Number> mtype = metricType == GAUGE_RATE ? GAUGE : COUNTER;
                individualStats = findMetricsWithFilters(tenantId, mtype, tagFilters)
                        .map(metric -> {
                            return findRateData(metric.getMetricId(), start, end, 0, ASC)
                                    .compose(new NumericBucketPointTransformer(buckets, percentiles))
                                    .flatMap(Observable::from);
                        });
            }

            return Observable.merge(individualStats)
                    .groupBy(BucketPoint::getStart)
                    .flatMap(group -> group.collect(SumNumericBucketPointCollector::new,
                            SumNumericBucketPointCollector::increment))
                    .map(SumNumericBucketPointCollector::toBucketPoint)
                    .toMap(NumericBucketPoint::getStart)
                    .map(pointMap -> NumericBucketPoint.toList(pointMap, buckets));
        }
    };

    @Override
    public <T extends Number> Observable<List<NumericBucketPoint>> findNumericStats(String tenantId,
            MetricType<T> metricType, List<String> metrics, long start, long end, Buckets buckets,
            List<Percentile> percentiles, boolean stacked) {

        checkArgument(isValidTimeRange(start, end), "Invalid time range");
        checkArgument(metricType == GAUGE || metricType == GAUGE_RATE
                || metricType == COUNTER || metricType == COUNTER_RATE, "Invalid metric type: %s", metricType);

        if (!stacked) {
            if (COUNTER == metricType || GAUGE == metricType) {
                return Observable.from(metrics)
                        .flatMap(metricName -> findMetric(new MetricId<>(tenantId, metricType, metricName)))
                        .flatMap(metric -> findDataPoints(metric.getMetricId(), start, end, 0, Order.DESC))
                        .compose(new NumericBucketPointTransformer(buckets, percentiles));
            } else {
                MetricType<? extends Number> mtype = metricType == GAUGE_RATE ? GAUGE : COUNTER;
                return Observable.from(metrics)
                        .flatMap(metricName -> findMetric(new MetricId<>(tenantId, mtype, metricName)))
                        .flatMap(metric -> findRateData(metric.getMetricId(), start, end, 0, ASC))
                        .compose(new NumericBucketPointTransformer(buckets, percentiles));
            }
        } else {
            Observable<Observable<NumericBucketPoint>> individualStats;
            if (COUNTER == metricType || GAUGE == metricType) {
                individualStats = Observable.from(metrics)
                        .flatMap(metricName -> findMetric(new MetricId<>(tenantId, metricType, metricName)))
                        .map(metric -> {
                            return findDataPoints(metric.getMetricId(), start, end, 0, Order.DESC)
                                    .compose(new NumericBucketPointTransformer(buckets, percentiles))
                                    .flatMap(Observable::from);
                        });
            } else {
                MetricType<? extends Number> mtype = metricType == GAUGE_RATE ? GAUGE : COUNTER;
                individualStats = Observable.from(metrics)
                        .flatMap(metricName -> findMetric(new MetricId<>(tenantId, mtype, metricName)))
                        .map(metric -> {
                            return findRateData(metric.getMetricId(), start, end, 0, ASC)
                                    .compose(new NumericBucketPointTransformer(buckets, percentiles))
                                    .flatMap(Observable::from);
                        });
            }

            return Observable.merge(individualStats)
                    .groupBy(BucketPoint::getStart)
                    .flatMap(group -> group.collect(SumNumericBucketPointCollector::new,
                            SumNumericBucketPointCollector::increment))
                    .map(SumNumericBucketPointCollector::toBucketPoint)
                    .toMap(NumericBucketPoint::getStart)
                    .map(pointMap -> NumericBucketPoint.toList(pointMap, buckets));
        }
    }

    @Override
    public Observable<DataPoint<AvailabilityType>> findAvailabilityData(MetricId<AvailabilityType> id, long start,
            long end, boolean distinct, int limit, Order order) {
        checkArgument(isValidTimeRange(start, end), "Invalid time range");
        if (distinct) {
            Observable<DataPoint<AvailabilityType>> availabilityData = findDataPoints(id, start, end, 0, order)
                    .distinctUntilChanged(DataPoint::getValue);
            if (limit <= 0) {
                return availabilityData;
            } else {
                return availabilityData.limit(limit);
            }
        } else {
            return findDataPoints(id, start, end, limit, order);
        }
    }

    @Override
    public Observable<List<AvailabilityBucketPoint>> findAvailabilityStats(MetricId<AvailabilityType> metricId,
            long start, long end, Buckets buckets) {
        checkArgument(isValidTimeRange(start, end), "Invalid time range");
        return this.findDataPoints(metricId, start, end, 0, ASC)
                .groupBy(dataPoint -> buckets.getIndex(dataPoint.getTimestamp()))
                .flatMap(group -> group.collect(() -> new AvailabilityDataPointCollector(buckets, group.getKey()),
                        AvailabilityDataPointCollector::increment))
                .map(AvailabilityDataPointCollector::toBucketPoint)
                .toMap(AvailabilityBucketPoint::getStart)
                .map(pointMap -> AvailabilityBucketPoint.toList(pointMap, buckets));
    }

    @Override
    public Observable<DataPoint<String>> findStringData(MetricId<String> id, long start, long end, boolean distinct,
            int limit, Order order) {
        checkArgument(isValidTimeRange(start, end));
        if (distinct) {
            return findDataPoints(id, start, end, limit, order).distinctUntilChanged(DataPoint::getValue);
        } else {
            return findDataPoints(id, start, end, limit, order);
        }
    }

    @Override
    public Observable<Boolean> idExists(final MetricId<?> metricId) {
        return this.findMetrics(metricId.getTenantId(), metricId.getType())
                .filter(m -> {
                    return metricId.getName().equals(m.getMetricId().getName());
                })
                .take(1)
                .map(m -> Boolean.TRUE)
                .defaultIfEmpty(Boolean.FALSE);
    }

    @Override
    public Observable<List<NumericBucketPoint>> findCounterStats(MetricId<Long> id, long start, long end,
            Buckets buckets, List<Percentile> percentiles) {
        checkArgument(isValidTimeRange(start, end), "Invalid time range");
        return findDataPoints(id, start, end, 0, ASC)
                .compose(new NumericBucketPointTransformer(buckets, percentiles));
    }

    @Override
    public Observable<Map<String, TaggedBucketPoint>> findCounterStats(MetricId<Long> metricId,
            Map<String, String> tags, long start, long end, List<Percentile> percentiles) {
        return findDataPoints(metricId, start, end, 0, ASC)
                .compose(new TaggedBucketPointTransformer(tags, percentiles));
    }

    @Override
    public Observable<List<long[]>> getPeriods(MetricId<Double> id, Predicate<Double> predicate, long start,
            long end) {
        checkArgument(isValidTimeRange(start, end), "Invalid time range");
        return dataAccess.findGaugeData(id, start, end, 0, ASC)
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

    @Override
    public Observable<Metric<?>> insertedDataEvents() {
        return insertedDataPointEvents;
    }

    private int getTTL(MetricId<?> metricId) {
        Integer ttl = dataRetentions.get(new DataRetentionKey(metricId));
        if (ttl == null) {
            ttl = dataRetentions.getOrDefault(new DataRetentionKey(metricId.getTenantId(), metricId.getType()),
                    defaultTTL);
        }
        return ttl;
    }

    public void shutdown() {
        insertedDataPointEvents.onCompleted();
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
