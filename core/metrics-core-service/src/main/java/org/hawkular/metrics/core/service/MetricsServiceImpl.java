/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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

import static java.time.ZoneOffset.UTC;

import static org.hawkular.metrics.core.service.Functions.isValidTagMap;
import static org.hawkular.metrics.core.service.Functions.makeSafe;
import static org.hawkular.metrics.core.service.Order.ASC;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.hawkular.metrics.model.MetricType.STRING;
import static org.hawkular.metrics.model.Utils.isValidTimeRange;

import static com.google.common.base.Preconditions.checkArgument;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.hawkular.metrics.core.dropwizard.HawkularMetricRegistry;
import org.hawkular.metrics.core.dropwizard.MetricNameService;
import org.hawkular.metrics.core.service.compress.CompressedPointContainer;
import org.hawkular.metrics.core.service.log.CoreLogger;
import org.hawkular.metrics.core.service.log.CoreLogging;
import org.hawkular.metrics.core.service.tags.ExpressionTagQueryParser;
import org.hawkular.metrics.core.service.tags.SimpleTagQueryParser;
import org.hawkular.metrics.core.service.tags.TagsConverter;
import org.hawkular.metrics.core.service.transformers.DataPointCompressTransformer;
import org.hawkular.metrics.core.service.transformers.DataPointDecompressTransformer;
import org.hawkular.metrics.core.service.transformers.MetricFromDataRowTransformer;
import org.hawkular.metrics.core.service.transformers.MetricIdentifierFromFullDataRowTransformer;
import org.hawkular.metrics.core.service.transformers.MetricsIndexRowTransformer;
import org.hawkular.metrics.core.service.transformers.NumericBucketPointTransformer;
import org.hawkular.metrics.core.service.transformers.SortedMerge;
import org.hawkular.metrics.core.service.transformers.TaggedBucketPointTransformer;
import org.hawkular.metrics.core.service.transformers.TempTableCompressTransformer;
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
import org.hawkular.metrics.model.exception.RuntimeApiError;
import org.hawkular.metrics.model.exception.TenantAlreadyExistsException;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.Tags;
import org.hawkular.metrics.model.param.TimeRange;
import org.hawkular.metrics.sysconfig.Configuration;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.joda.time.Duration;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import rx.Completable;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func6;
import rx.observable.ListenableFutureObservable;
import rx.schedulers.Schedulers;

/**
 * @author John Sanda
 */
public class MetricsServiceImpl implements MetricsService {
    private static final CoreLogger log = CoreLogging.getCoreLogger(MetricsServiceImpl.class);

    private static final long DAY_TO_MILLIS = 24 * 3600 * 1000;
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

    private ListeningExecutorService metricsTasks;

    private DataAccess dataAccess;

    private ConfigurationService configurationService;

    private MetricNameService metricNameService = new MetricNameService();

    private HawkularMetricRegistry metricRegistry;

    /**
     * Functions used to insert metric data points.
     */
    private Map<MetricType<?>, Func1<Observable<? extends Metric<?>>, Observable<Integer>>> pointsInserter;

    /**
     * Functions used to find metric data points rows.
     */
    private Map<MetricType<?>, Func6<? extends MetricId<?>, Long, Long,
                Integer, Order, Integer, Observable<Row>>> dataPointFinders;

    /**
     * Functions used to transform a row into a data point object.
     */
    private Map<MetricType<?>, Func1<Row, ? extends DataPoint<?>>> dataPointMappers;

    private Map<MetricType<?>, Func1<Row, ? extends DataPoint<?>>> tempDataPointMappers;

    /**
     * Tools that do tag query parsing and execution
     */
    private boolean disableACostOptimization;
    private SimpleTagQueryParser tagQueryParser;
    private ExpressionTagQueryParser expresssionTagQueryParser;

    private int defaultTTL = Duration.standardDays(7).toStandardSeconds().getSeconds();
    private int DEFAULT_RETENTION = (int) Duration.standardSeconds(defaultTTL).getStandardDays();

    private int maxStringSize;

    private long insertRetryMaxDelay;

    private int insertMaxRetries;

    private int defaultPageSize;

    public void startUp(Session session, String keyspace, boolean resetDb, HawkularMetricRegistry metricRegistry) {
        startUp(session, keyspace, resetDb, true, metricRegistry);
    }

    public void startUp(Session session, String keyspace, boolean resetDb, boolean createSchema,
            HawkularMetricRegistry metricRegistry) {
        session.execute("USE " + keyspace);
        log.infoKeyspaceUsed(keyspace);
        metricsTasks = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));
        loadDataRetentions();

        this.metricRegistry = metricRegistry;

        pointsInserter = ImmutableMap
                .<MetricType<?>, Func1<Observable<? extends Metric<?>>, Observable<Integer>>>builder()
                .put(GAUGE, metric -> {
                    @SuppressWarnings("unchecked")
                    Observable<Metric<Double>> gauge = (Observable<Metric<Double>>) metric;
                    return dataAccess.insertData(gauge);
                })
                .put(COUNTER, metric -> {
                    @SuppressWarnings("unchecked")
                    Observable<Metric<Long>> counter = (Observable<Metric<Long>>) metric;
                    return dataAccess.insertData(counter);
                })
                .put(AVAILABILITY, metric -> {
                    @SuppressWarnings("unchecked")
                    Observable<Metric<AvailabilityType>> avail = (Observable<Metric<AvailabilityType>>) metric;
                    return dataAccess.insertData(avail);
                })
                .put(STRING, metric -> {
                    @SuppressWarnings("unchecked")
                    Observable<Metric<String>> string = (Observable<Metric<String>>) metric;
                    return dataAccess.insertStringDatas(string, this::getTTL, maxStringSize);
                })
                .build();

        dataPointFinders = ImmutableMap
                .<MetricType<?>, Func6<? extends MetricId<?>, Long, Long, Integer, Order, Integer,
                        Observable<Row>>>builder()
                .put(STRING, (metricId, start, end, limit, order, pageSize) -> {
                    @SuppressWarnings("unchecked")
                    MetricId<String> stringId = (MetricId<String>) metricId;
                    return dataAccess.findStringData(stringId, start, end, limit, order, pageSize);
                })
                .build();

        dataPointMappers = ImmutableMap.<MetricType<?>, Func1<Row, ? extends DataPoint<?>>> builder()
                .put(GAUGE, Functions::getGaugeDataPoint)
                .put(AVAILABILITY, Functions::getAvailabilityDataPoint)
                .put(COUNTER, Functions::getCounterDataPoint)
                .put(STRING, Functions::getStringDataPoint)
                .build();

        tempDataPointMappers = ImmutableMap.<MetricType<?>, Func1<Row, ? extends DataPoint<?>>> builder()
                .put(GAUGE, Functions::getTempGaugeDataPoint)
                .put(COUNTER, Functions::getTempCounterDataPoint)
                .put(AVAILABILITY, Functions::getTempAvailabilityDataPoint)
                .build();

        initConfiguration(session);
        setDefaultTTL(session, keyspace);
        initMetrics();

        verifyAndCreateTempTables();

        tagQueryParser = new SimpleTagQueryParser(this.dataAccess, this, disableACostOptimization);
        expresssionTagQueryParser = new ExpressionTagQueryParser(this.dataAccess, this);
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
        metricRegistry.registerMetaData("DataPointsInserted", "Core", "Write");
        metricRegistry.registerMetaData("RawDataReadLatency", "Core", "Read");
        metricRegistry.registerMetaData("MetricTagsQueryLatency", "Core", "Read");
    }

    /**
     * Measurements of the throughput of inserting data points.
     */
    private Meter getDataPointsInserted() {
        return metricRegistry.meter("DataPointsInserted");
    }

    /**
     * Raw data read metrics
     */
    private Timer getRawDataReadLatency() {
        return metricRegistry.timer("RawDataReadLatency");
    }

    /**
     * Metric tag query metrics
     */
    private Timer getMetricsTagsQueryLatency() {
        return metricRegistry.timer("MetricTagsQueryLatency");
    }

    private void initConfiguration(Session session) {
        Configuration configuration = configurationService.load("org.hawkular.metrics").toBlocking()
                .lastOrDefault(null);
        String configMaxStringSize = configuration.get("string-size");
        if (configMaxStringSize == null) {
            maxStringSize = -1;  // no size limit
        } else {
            maxStringSize = Integer.parseInt(configMaxStringSize);
        }
        log.infoMaxSizeStringMetrics(this.maxStringSize);

        insertRetryMaxDelay = Long.parseLong(configuration.get("ingestion.retry.max-delay", "30000"));
        insertMaxRetries = Integer.parseInt(configuration.get("ingestion.retry.max-retries", "5"));
        log.infoInsertRetryConfig(insertMaxRetries, insertRetryMaxDelay);

        defaultPageSize = Integer.parseInt(configuration.get("page-size", "5000"));
        disableACostOptimization = Boolean.parseBoolean(configuration.get("disable.parser.optimization", "false"));
    }

    private void setDefaultTTL(Session session, String keyspace) {
        ResultSet resultSet = session.execute("select default_time_to_live from system_schema.tables where " +
                "keyspace_name = '" + keyspace + "' and table_name = 'data'");
        List<Row> rows = resultSet.all();
        if (rows.isEmpty()) {
            throw new IllegalStateException("Failed to find " + keyspace + ".data in system_schema.tables. Default " +
                    "data retention cannot be configured.");
        }
        int defaultTTL = rows.get(0).getInt(0);
        if (defaultTTL != this.defaultTTL) {
            session.execute("alter table " + keyspace + ".data with default_time_to_live = " + this.defaultTTL);
        }
        log.infoDefaultDataRetention(this.defaultTTL);
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

    public void setMetricNameService(MetricNameService metricNameService) {
        this.metricNameService = metricNameService;
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
                updates.add(dataAccess.insertIntoMetricsTagsIndex(metric, metric.getTags()));

                if (metric.getDataRetention() != null) {
                    updates.add(updateRetentionsIndex(metric));
                }

                Observable.merge(updates).subscribe(new VoidSubscriber<>(subscriber));
            }
        }));
    }

    private Observable<ResultSet> updateRetentionsIndex(Metric<?> metric) {
        return ListenableFutureObservable.from(dataAccess.updateRetentionsIndex(metric), metricsTasks)
                .doOnCompleted(() ->
                        dataRetentions.put(new DataRetentionKey(metric), metric.getDataRetention()));
    }

    @Override
    public Observable<MetricId<?>> findAllMetricIdentifiers() {
        return dataAccess.findAllMetricIdentifiersInData()
                .compose(new MetricIdentifierFromFullDataRowTransformer(defaultTTL))
                .distinct();
    }

    public <T> Observable.Transformer<MetricId<T>, Metric<T>> enrichToMetric() {
        return t -> t
                .flatMap(id -> dataAccess.findMetricInMetricsIndex(id)
                        .compose(new MetricsIndexRowTransformer<>(id.getTenantId(), id.getType(), defaultTTL))
                        .switchIfEmpty(dataAccess.findMetricInData(id) // This only verifies it exists..
                                .compose(new MetricFromDataRowTransformer<>(id.getTenantId(), id.getType(), defaultTTL))));
    }

    @Override
    public <T> Observable<Metric<T>> findMetric(final MetricId<T> id) {
        return Observable.just(id)
                .compose(enrichToMetric());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Observable<Metric<T>> findMetrics(String tenantId, MetricType<T> metricType) {
        Observable<Metric<T>> setFromMetricsIndex = null;
        Observable<Metric<T>> setFromData = dataAccess.findAllMetricIdentifiersInData()
                .doOnError(Throwable::printStackTrace)
                .filter(row -> tenantId.equals(row.getString(0)))
                .compose(new MetricIdentifierFromFullDataRowTransformer(defaultTTL))
                .distinct()
                .map(m -> new Metric(m, DEFAULT_RETENTION));

        if (metricType == null) {
            setFromMetricsIndex = Observable.from(MetricType.userTypes())
                    .map(type -> (MetricType<T>) type)
                    .flatMap(type -> dataAccess.findMetricsInMetricsIndex(tenantId, type)
                            .compose(new MetricsIndexRowTransformer<>(tenantId, type, defaultTTL)));
        } else {
            setFromMetricsIndex = dataAccess.findMetricsInMetricsIndex(tenantId, metricType)
                    .compose(new MetricsIndexRowTransformer<>(tenantId, metricType, defaultTTL));

            setFromData = setFromData.filter(m -> metricType.equals(m.getType()));
        }

        return setFromMetricsIndex.concatWith(setFromData).distinct(Metric::getMetricId);
    }

//    @SuppressWarnings("unchecked")
    @Override
    public <T> Observable<MetricId<T>> findMetricIdentifiersWithFilters(String tenantId, MetricType<T> metricType,
                                                                       String tags) {
        Timer.Context context = getMetricsTagsQueryLatency().time();
        Observable<MetricId<T>> results;
        try {
            results = expresssionTagQueryParser
                    .parse(tenantId, metricType, tags)
                    .map(tMetric -> tMetric);
        } catch (Exception e1) {
            try {
                Tags parsedSimpleTagQuery = TagsConverter.fromString(tags);
                results = tagQueryParser.findMetricIdentifiersWithFilters(tenantId, metricType, parsedSimpleTagQuery.getTags())
                        .map(m -> (MetricId<T>) m);
            } catch (Exception e2) {
                results = Observable.error(new RuntimeApiError("Unparseable tag query expression."));
            }
        }
        return results.doOnCompleted(context::stop);
    }

    public <T> Func1<MetricId<T>, Boolean> idFilter(String regexp) {
        if(Strings.isNullOrEmpty(regexp)) {
            return tMetric -> true;
        }
        boolean positive = (!regexp.startsWith("!"));
        Pattern p = PatternUtil.filterPattern(regexp);
        return tMetric -> positive == p.matcher(tMetric.getName()).matches();
    }

    @Override
    public Observable<Map<String, Set<String>>> getTagValues(String tenantId, MetricType<?> metricType,
                                    Map<String, String> tagsQueries) {
        return tagQueryParser.getTagValues(tenantId, metricType, tagsQueries);
    }

    @Override
    public Observable<Map<String, String>> getMetricTags(MetricId<?> id) {
        return dataAccess.getMetricTags(id)
                .take(1)
                .map(row -> row.getMap(0, String.class, String.class))
                .defaultIfEmpty(new HashMap<>());
    }

    @Override
    public Observable<String> getTagNames(String tenantId, MetricType<?> metricType, String filter) {
        return tagQueryParser.getTagNames(tenantId, metricType, filter);
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

        return dataAccess.insertIntoMetricsTagsIndex(metric, tags).concatWith(dataAccess.addTags(metric, tags))
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
                            dataAccess.deleteFromMetricsTagsIndex(metric.getMetricId(), tagsToDelete)).toList()
                            .map(r -> null);
                });
    }

    @Override
    public <T> Observable<Void> addDataPoints(MetricType<T> metricType, Observable<Metric<T>> metrics) {
        checkArgument(metricType != null, "metricType is null");

        return pointsInserter
                .get(metricType)
                .call(metrics
                        .filter(metric -> !metric.getDataPoints().isEmpty()))
                .doOnNext(getDataPointsInserted()::mark)
                .map(i -> null);
    }

    @Override
    public <T> Observable<DataPoint<T>> findDataPoints(MetricId<T> metricId, long start, long end, int limit,
            Order order) {
        return findDataPoints(metricId, start, end, limit, order, defaultPageSize);
    }

    @Override
    public <T> Observable<DataPoint<T>> findDataPoints(MetricId<T> metricId, long start, long end, int limit,
                                                       Order order, int pageSize) {

        Timer.Context context = getRawDataReadLatency().time();
        checkArgument(isValidTimeRange(start, end), "Invalid time range");
        Order safeOrder = (null == order) ? Order.ASC : order;
        MetricType<T> metricType = metricId.getType();
        Func1<Row, DataPoint<T>> mapper = getDataPointMapper(metricType);

        if (metricType == GAUGE || metricType == AVAILABILITY || metricType == COUNTER) {
            long sliceStart = DateTimeService.getTimeSlice(start, Duration.standardHours(2));

            Func1<Row, DataPoint<T>> tempMapper = (Func1<Row, DataPoint<T>>) tempDataPointMappers.get(metricType);

            // Calls mostly deprecated methods..
//            Observable<DataPoint<T>> uncompressedPoints = dataAccess.findOldData(metricId, start, end, limit, safeOrder,
//                    pageSize).map(mapper).doOnError(Throwable::printStackTrace);

            Observable<DataPoint<T>> compressedPoints =
                    dataAccess.findCompressedData(metricId, sliceStart, end, limit, safeOrder)
                            .compose(new DataPointDecompressTransformer(metricType, safeOrder, limit, start, end));

            Observable<DataPoint<T>> tempStoragePoints = dataAccess.findTempData(metricId, start, end, limit,
                    safeOrder, pageSize)
                    .map(tempMapper);

            Comparator<DataPoint<T>> comparator = getDataPointComparator(safeOrder);
            List<Observable<? extends DataPoint<T>>> sources = new ArrayList<>(3);
//            sources.add(uncompressedPoints);
            sources.add(compressedPoints);
            sources.add(tempStoragePoints);

            Observable<DataPoint<T>> dataPoints = SortedMerge.create(sources, comparator, false)
                    .distinctUntilChanged(
                            (tDataPoint, tDataPoint2) -> comparator.compare(tDataPoint, tDataPoint2) == 0);

            if (limit > 0) {
                dataPoints = dataPoints.take(limit);
            }

            return dataPoints;
        }
        Func6<MetricId<T>, Long, Long, Integer, Order, Integer, Observable<Row>> finder =
                getDataPointFinder(metricType);

        Observable<DataPoint<T>> results =
                finder.call(metricId, start, end, limit, safeOrder, pageSize).map(mapper);
        return results.doOnCompleted(context::stop);
    }

    private <T> Comparator<DataPoint<T>> getDataPointComparator(Order safeOrder) {
        Comparator<DataPoint<T>> comparator;

        switch(safeOrder) {
            case ASC:
                comparator = (tDataPoint, t1) -> (int) (tDataPoint.getTimestamp() - t1.getTimestamp());
                break;
            case DESC:
                comparator = (tDataPoint, t1) -> (int) (t1.getTimestamp() - tDataPoint.getTimestamp());
                break;
            default:
                throw new RuntimeException(safeOrder.toString() + " is not correct sorting order");
        }

        return comparator;
    }

    private <T> Observable.Transformer<T, T> applyRetryPolicy() {
        return tObservable -> tObservable
                .retryWhen(observable -> {
                    Observable<Integer> range = Observable.range(1, Integer.MAX_VALUE);
                    Observable<Observable<?>> zipWith = observable.zipWith(range, (t, i) -> {
                        log.debug("Attempt #" + i + " to retry the operation after Cassandra client" +
                                " exception");
                        if (t instanceof DriverException) {
                            return Observable.timer(i, TimeUnit.SECONDS).onBackpressureDrop();
                        } else {
                            return Observable.error(t);
                        }
                    });

                    return Observable.merge(zipWith);
                })
                .doOnError(t -> log.error("Failure while trying to apply compression, skipping block", t))
                .onErrorResumeNext(Observable.empty());
    }

    /**
     * Intended to be used at the startup of the MetricsServiceImpl to ensure we have enough tables for processing
     */
    public void verifyAndCreateTempTables() {
        ZonedDateTime currentBlock = ZonedDateTime.ofInstant(Instant.ofEpochMilli(DateTimeService.now.get().getMillis()), UTC)
                .with(DateTimeService.startOfPreviousEvenHour());

        ZonedDateTime lastStartupBlock = currentBlock.plus(6, ChronoUnit.HOURS);
        verifyAndCreateTempTables(currentBlock, lastStartupBlock).await();
    }

    @Override
    public Completable verifyAndCreateTempTables(ZonedDateTime startTime, ZonedDateTime endTime) {
        Set<Long> timestamps = new HashSet<>();

        while(startTime.isBefore(endTime)) {
            // Table sizes are not configurable at this point
            timestamps.add(startTime.toInstant().toEpochMilli());
            startTime = startTime.plus(2, ChronoUnit.HOURS);
        }

        return Completable.fromObservable(dataAccess.createTempTablesIfNotExists(timestamps));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Completable compressBlock(long startTimeSlice, int pageSize, int maxConcurrency) {
        return Completable.fromObservable(
                dataAccess.findAllDataFromBucket(startTimeSlice, pageSize, maxConcurrency)
                        .switchIfEmpty(Observable.empty())
                        .flatMap(rows -> rows
                                // Each time the tokenrange changes inside the query, create new window, publish allows
                                // reuse of the observable in two distinct processing phases
                                .publish(p -> p.window(
                                        p.map(Row::getPartitionKeyToken)
                                                .distinctUntilChanged()))
                                // ConcatMap so we don't mess the order as that's important in the compression job
                                .concatMap(o -> {
                                    // Cache the first key from the observable so we can use it to create a key later
                                    Observable<Row> sharedRows = o.share();
                                    Observable<CompressedPointContainer> compressed =
                                            sharedRows.compose(new TempTableCompressTransformer(startTimeSlice));
                                    Observable<Row> keyTake = sharedRows.take(1);

                                    // Merge the first row with the compressed package to be able to write to Cassandra
                                    return compressed.zipWith(keyTake, (cpc, r) -> {
                                        MetricId<?> metricId =
                                                new MetricId(r.getString(0), MetricType.fromCode(r.getByte(1)),
                                                        r.getString(2));
                                        return dataAccess.insertCompressedData(metricId, startTimeSlice, cpc,
                                                getTTL(metricId));
                                    });
                                }), maxConcurrency)
                        .flatMap(rs -> rs)
                        .doOnCompleted(() -> dataAccess.dropTempTable(startTimeSlice)
                                .compose(applyRetryPolicy())
                                .subscribeOn(Schedulers.io())
                                .subscribe())
        );
    }

    @Override
    @Deprecated
    @SuppressWarnings("unchecked")
    public Completable compressBlock(Observable<? extends MetricId<?>> metrics, long startTimeSlice,
            long endTimeSlice, int pageSize) {

        return Completable.fromObservable(metrics
                .compose(applyRetryPolicy())
                .concatMap(metricId -> findDataPoints(metricId, startTimeSlice, endTimeSlice, 0, ASC, pageSize)
                        .compose(applyRetryPolicy())
                        .compose(new DataPointCompressTransformer(metricId.getType(), startTimeSlice))
                        .concatMap(cpc -> dataAccess.deleteAndInsertCompressedGauge(metricId, startTimeSlice,
                                (CompressedPointContainer) cpc, startTimeSlice, endTimeSlice, getTTL(metricId))
                                .compose(applyRetryPolicy()))));
    }

    @Override
    public <T> Observable<NamedDataPoint<T>> findDataPoints(List<MetricId<T>> metricIds, long start,
            long end, int limit, Order order) {
        return Observable.from(metricIds)
                .concatMap(id -> findDataPoints(id, start, end, limit, order)
                        .map(dataPoint -> new NamedDataPoint<>(id.getName(), dataPoint)));
    }

    @Override
    public <T> Observable<NamedDataPoint<T>> findDataPoints(String tenantId, MetricType<T> metricType,
            String tagFilters, long start, long end, int limit, Order order) {
        return findMetricIdentifiersWithFilters(tenantId, metricType, tagFilters)
                .concatMap(id -> findDataPoints(id, start, end, limit, order)
                        .map(dataPoint -> new NamedDataPoint<>(id.getName(), dataPoint)));
    }

    @SuppressWarnings("unchecked")
    private <T> Func6<MetricId<T>, Long, Long, Integer, Order, Integer, Observable<Row>> getDataPointFinder(
            MetricType<T> metricType) {
        Func6<MetricId<T>, Long, Long, Integer, Order, Integer, Observable<Row>> finder;
        finder = (Func6<MetricId<T>, Long, Long, Integer, Order, Integer, Observable<Row>>) dataPointFinders
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

    @Override
    public <T extends Number> Observable<NamedDataPoint<Double>> findRateData(List<MetricId<T>> ids, long start,
                                                                     long end, int limit, Order order) {
        return Observable.from(ids).concatMap(id -> findRateData(id, start, end, limit, order)
                .map(dataPoint -> new NamedDataPoint<>(id.getName(), dataPoint)));
    }

    @Override
    public Observable<List<NumericBucketPoint>> findRateStats(MetricId<? extends Number> id, BucketConfig bucketConfig,
                                                              List<Percentile> percentiles) {
        TimeRange timeRange = bucketConfig.getTimeRange();
        checkArgument(isValidTimeRange(timeRange.getStart(), timeRange.getEnd()), "Invalid time range");
        return findRateData(id, timeRange.getStart(), timeRange.getEnd(), 0, ASC)
                .compose(new NumericBucketPointTransformer(bucketConfig.getBuckets(), percentiles));
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
        return findDataPoints(metricId, timeRange.getStart(), timeRange.getEnd(), 0, Order.DESC)
                .compose(new NumericBucketPointTransformer(bucketConfig.getBuckets(), percentiles));
    }

    @Override
    public Observable<Map<String, TaggedBucketPoint>> findGaugeStats(MetricId<Double> metricId,
            Map<String, String> tags, long start, long end, List<Percentile> percentiles) {
        return findDataPoints(metricId, start, end, 0, Order.DESC)
                .compose(new TaggedBucketPointTransformer(tags, percentiles));
    }

    @Override
    public <T extends Number> Observable<List<NumericBucketPoint>> findNumericStats(
            List<MetricId<T>> metrics, long start, long end, Buckets buckets, List<Percentile>
            percentiles, boolean stacked, boolean isRate) {

        // TODO Stats needs fixing to understand compressed values also..
        checkArgument(isValidTimeRange(start, end), "Invalid time range");
        if (!stacked) {
            if (!isRate) {
                return Observable.from(metrics)
                        .flatMap(metricId -> findDataPoints(metricId, start, end, 0, Order.DESC))
                        .compose(new NumericBucketPointTransformer(buckets, percentiles));
            } else {
                return Observable.from(metrics)
                        .flatMap(metricId -> findRateData(metricId, start, end, 0, ASC))
                        .compose(new NumericBucketPointTransformer(buckets, percentiles));
            }
        } else {
            Observable<Observable<NumericBucketPoint>> individualStats;
            if (!isRate) {
                individualStats = Observable.from(metrics).map(metricId -> {
                    return findDataPoints(metricId, start, end, 0, Order.DESC)
                            .compose(new NumericBucketPointTransformer(buckets, percentiles))
                            .flatMap(Observable::from);
                });
            } else {
                individualStats = Observable.from(metrics).map(metricId -> {
                    return findRateData(metricId, start, end, 0, ASC)
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
    public Observable<List<NumericBucketPoint>> findCounterStats(MetricId<Long> id, BucketConfig bucketConfig, List<Percentile>
            percentiles) {
        TimeRange timeRange = bucketConfig.getTimeRange();
        checkArgument(isValidTimeRange(timeRange.getStart(), timeRange.getEnd()), "Invalid time range");
        return findDataPoints(id, timeRange.getStart(), timeRange.getEnd(), 0, ASC)
                .doOnError(Throwable::printStackTrace)
                .compose(new NumericBucketPointTransformer(bucketConfig.getBuckets(), percentiles));
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
        return findDataPoints(id, start, end, 0, ASC)
//                .map(Functions::getGaugeDataPoint)
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
            ttl = dataRetentions.getOrDefault(new DataRetentionKey(metricId.getTenantId(), metricId.getType()),
                    defaultTTL);
        } else {
            ttl = Duration.standardDays(ttl).toStandardSeconds().getSeconds();
        }
        return ttl;
    }

    public void shutdown() {
        metricsTasks.shutdown();
        unloadDataRetentions();
//        dataAccess.shutdown();
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

    @Override
    public <T> Observable<Void> deleteMetric(MetricId<T> id) {
        //NOTE: compressed data is not deleted due to the using TWCS compaction strategy
        //      for the compressed data table.
        Observable<Void> result = dataAccess.getMetricTags(id)
                .map(row -> row.getMap(0, String.class, String.class))
                .defaultIfEmpty(new HashMap<>())
                .flatMap(map -> dataAccess.deleteFromMetricsTagsIndex(id, map))
                .map(r -> null);
        Observable<Void> indexes = Observable.merge(
                dataAccess.deleteMetricFromMetricsIndex(id),
                dataAccess.deleteMetricData(id),
                dataAccess.deleteMetricFromRetentionIndex(id))
                .map(r -> null);

        return result.concatWith(indexes);
    }

}
