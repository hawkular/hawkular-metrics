package org.rhq.metrics.impl.cassandra;

import static org.joda.time.Hours.hours;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;

import org.joda.time.Duration;
import org.joda.time.Hours;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.core.Availability;
import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricAlreadyExistsException;
import org.rhq.metrics.core.MetricData;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.MetricsThreadFactory;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric2;
import org.rhq.metrics.core.RawNumericMetric;
import org.rhq.metrics.core.Retention;
import org.rhq.metrics.core.RetentionSettings;
import org.rhq.metrics.core.SchemaManager;
import org.rhq.metrics.core.Tenant;
import org.rhq.metrics.core.TenantAlreadyExistsException;

/**
 * @author John Sanda
 */
public class MetricsServiceCassandra implements MetricsService {

    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceCassandra.class);

    public static final String REQUEST_LIMIT = "rhq.metrics.request.limit";

    public static final int DEFAULT_TTL = Duration.standardDays(7).toStandardSeconds().getSeconds();

    private static final Function<ResultSet, Void> RESULT_SET_TO_VOID = new Function<ResultSet, Void>() {
        @Override
        public Void apply(ResultSet resultSet) {
            return null;
        }
    };

    private static final Function<List<ResultSet>, Void> RESULT_SETS_TO_VOID = new Function<List<ResultSet>, Void>() {
        @Override
        public Void apply(List<ResultSet> resultSets) {
            return null;
        }
    };

    private static class DataRetentionKey {
        private String tenantId;
        private MetricId metricId;
        private MetricType type;

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
            if (type != that.type) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = tenantId.hashCode();
            result = 31 * result + metricId.hashCode();
            result = 31 * result + type.hashCode();
            return result;
        }
    }

    private RateLimiter permits = RateLimiter.create(Double.parseDouble(
        System.getProperty(REQUEST_LIMIT, "30000")), 3, TimeUnit.MINUTES);

    private Optional<Session> session;

    private DataAccess dataAccess;

    private ListeningExecutorService metricsTasks = MoreExecutors
        .listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));

    /**
     * Note that while user specifies the durations in hours, we store them in seconds.
     */
    private Map<DataRetentionKey, Integer> dataRetentions = new ConcurrentHashMap<>();

    @Override
    public void startUp(Session s) {
        // the session is managed externally
        this.session = Optional.absent();
        this.dataAccess = new DataAccessImpl(s);
        loadDataRetentions();
    }

    @Override
    public void startUp(Map<String, String> params) {

        String tmp = params.get("cqlport");
        int port = 9042;
        try {
            port = Integer.parseInt(tmp);
        } catch (NumberFormatException nfe) {
            logger.warn("Invalid context param 'cqlport', not a number. Will use a default of 9042");
        }

        String[] nodes;
        if (params.containsKey("nodes")) {
            nodes = params.get("nodes").split(",");
        } else {
            nodes = new String[] {"127.0.0.1"};
        }

        Cluster cluster = new Cluster.Builder()
            .addContactPoints(nodes)
            .withPort(port)
            .build();

        String keyspace = params.get("keyspace");
        if (keyspace==null||keyspace.isEmpty()) {
            logger.debug("No keyspace given in params, checking system properties ...");
            keyspace = System.getProperty("cassandra.keyspace");
        }

        if (keyspace==null||keyspace.isEmpty()) {
            logger.debug("No explicit keyspace given, will default to 'rhq'");
            keyspace="rhq";
        }

        logger.info("Using a key space of '" + keyspace + "'");

        session = Optional.of(cluster.connect("system"));

        if (System.getProperty("cassandra.resetdb")!=null) {
            // We want a fresh DB -- mostly used for tests
            dropKeyspace(keyspace);
        }
        // This creates/updates the keyspace + tables if needed
        updateSchemaIfNecessary(keyspace);
        session.get().execute("USE " + keyspace);

        dataAccess = new DataAccessImpl(session.get());
        loadDataRetentions();
    }

    void loadDataRetentions() {
        DataRetentionsMapper mapper = new DataRetentionsMapper();
        List<String> tenantIds = loadTenantIds();
        CountDownLatch latch = new CountDownLatch(tenantIds.size() * 2);
        for (String tenantId : tenantIds) {
            ResultSetFuture numericFuture = dataAccess.findDataRetentions(tenantId, MetricType.NUMERIC);
            ResultSetFuture availabilityFuture = dataAccess.findDataRetentions(tenantId, MetricType.AVAILABILITY);
            ListenableFuture<Set<Retention>> numericRetentions = Futures.transform(numericFuture, mapper,
                metricsTasks);
            ListenableFuture<Set<Retention>> availabilityRetentions = Futures.transform(availabilityFuture, mapper,
                metricsTasks);
            Futures.addCallback(numericRetentions, new DataRetentionsLoadedCallback(tenantId, MetricType.NUMERIC,
                    latch), metricsTasks);
            Futures.addCallback(availabilityRetentions, new DataRetentionsLoadedCallback(tenantId,
                MetricType.AVAILABILITY, latch), metricsTasks);
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

    private class DataRetentionsLoadedCallback implements FutureCallback<Set<Retention>> {

        private String tenantId;

        private MetricType type;

        private CountDownLatch latch;

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
        if(session.isPresent()) {
            Session s = session.get();
            s.close();
            s.getCluster().close();
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

    @Override
    public ListenableFuture<Void> createTenant(final Tenant tenant) {
        ResultSetFuture future = dataAccess.insertTenant(tenant);
        return Futures.transform(future, new AsyncFunction<ResultSet, Void>() {
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
                    for (MetricType type : retentionsMap.keySet()) {
                        updateRetentionFutures.add(dataAccess.updateRetentionsIndex(tenant.getId(), type,
                            retentionsMap.get(type)));
                        for (Retention r : retentionsMap.get(type)) {
                            dataRetentions.put(new DataRetentionKey(tenant.getId(), type), r.getValue());
                        }
                    }
                    ListenableFuture<List<ResultSet>> updateRetentionsFuture = Futures
                            .allAsList(updateRetentionFutures);
                    return Futures.transform(updateRetentionsFuture, RESULT_SETS_TO_VOID, metricsTasks);
                }
            }
        }, metricsTasks);
    }

    @Override
    public ListenableFuture<List<Tenant>> getTenants() {
        TenantMapper mapper = new TenantMapper();
        List<String> ids = loadTenantIds();
        List<ListenableFuture<Tenant>> tenantFutures = new ArrayList<>(ids.size());
        for (String id : ids) {
            ResultSetFuture queryFuture = dataAccess.findTenant(id);
            ListenableFuture<Tenant> tenantFuture = Futures.transform(queryFuture, mapper, metricsTasks);
            tenantFutures.add(tenantFuture);
        }
        return Futures.allAsList(tenantFutures);
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
    public ListenableFuture<Void> createMetric(final Metric metric) {
        ResultSetFuture future = dataAccess.insertMetricInMetricsIndex(metric);
        return Futures.transform(future, new AsyncFunction<ResultSet, Void>() {
            @Override
            public ListenableFuture<Void> apply(ResultSet resultSet) {
                if (!resultSet.wasApplied()) {
                    throw new MetricAlreadyExistsException(metric);
                }
                // TODO Need error handling if either of the following updates fail
                // If adding meta data fails, then we want to report the error to the
                // client. Updating the retentions_idx table could also fail. We need to
                // report that failure as well.
                ResultSetFuture metadataFuture = dataAccess.addMetadata(metric);
                if (metric.getDataRetention() == null) {
                    return Futures.transform(metadataFuture, RESULT_SET_TO_VOID);
                }
                ResultSetFuture dataRetentionFuture = dataAccess.updateRetentionsIndex(metric);
                ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(metadataFuture,
                    dataRetentionFuture);
                dataRetentions.put(new DataRetentionKey(metric), metric.getDataRetention());
                return Futures.transform(insertsFuture, RESULT_SETS_TO_VOID);
            }
        }, metricsTasks);
    }

    @Override
    public ListenableFuture<Metric> findMetric(final String tenantId, final MetricType type, final MetricId id) {
        if (type == MetricType.LOG_EVENT) {
            throw new IllegalArgumentException(MetricType.LOG_EVENT + " is not yet supported");
        }
        ResultSetFuture future = dataAccess.findMetric(tenantId, type, id, Metric.DPART);
        return Futures.transform(future, new Function<ResultSet, Metric>() {
            @Override
            public Metric apply(ResultSet resultSet) {
                if (resultSet.isExhausted()) {
                    return null;
                }
                Row row = resultSet.one();
                if (type == MetricType.NUMERIC) {
                    return new NumericMetric2(tenantId, id, row.getMap(5, String.class, String.class), row.getInt(6));
                } else {
                    return new AvailabilityMetric(tenantId, id, row.getMap(5, String.class, String.class),
                        row.getInt(6));
                }
            }
        }, metricsTasks);
    }

    @Override
    public ListenableFuture<List<Metric>> findMetrics(String tenantId, MetricType type) {
        ResultSetFuture future = dataAccess.findMetricsInMetricsIndex(tenantId, type);
        return Futures.transform(future, new MetricsIndexMapper(tenantId, type));
    }

    @Override
    public ListenableFuture<Void> updateMetadata(Metric metric, Map<String, String> metadata, Set<String> deletions) {
        ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(
            dataAccess.updateMetadata(metric, metadata, deletions),
            dataAccess.updateMetadataInMetricsIndex(metric, metadata, deletions)
        );
        return Futures.transform(insertsFuture, RESULT_SETS_TO_VOID, metricsTasks);
    }

    @Override
    public ListenableFuture<Void> addNumericData(List<NumericMetric2> metrics) {
        List<ResultSetFuture> insertFutures = new ArrayList<>(metrics.size());
        for (NumericMetric2 metric : metrics) {
            if (metric.getData().isEmpty()) {
                logger.warn("There is no data to insert for {}", metric);
            } else {
                int ttl = getTTL(metric);
                insertFutures.add(dataAccess.insertData(metric, ttl));
            }
        }
        insertFutures.add(dataAccess.updateMetricsIndex(metrics));
        ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insertFutures);
        return Futures.transform(insertsFuture, RESULT_SETS_TO_VOID, metricsTasks);
    }

    @Override
    public ListenableFuture<Void> addAvailabilityData(List<AvailabilityMetric> metrics) {
        List<ResultSetFuture> insertFutures = new ArrayList<>(metrics.size());
        for (AvailabilityMetric metric : metrics) {
            if (metric.getData().isEmpty()) {
                logger.warn("There is no data to insert for {}", metric);
            } else {
                int ttl = getTTL(metric);
                insertFutures.add(dataAccess.insertData(metric, ttl));
            }
        }
        insertFutures.add(dataAccess.updateMetricsIndex(metrics));
        ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insertFutures);
        return Futures.transform(insertsFuture, RESULT_SETS_TO_VOID, metricsTasks);
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
    public ListenableFuture<NumericMetric2> findNumericData(NumericMetric2 metric, long start, long end) {
        // When we implement date partitioning, dpart will have to be determined based on
        // the start and end params. And it is possible the the date range spans multiple
        // date partitions.
        metric.setDpart(Metric.DPART);
        ResultSetFuture queryFuture = dataAccess.findData(metric, start, end);
        return Futures.transform(queryFuture, new NumericMetricMapper(), metricsTasks);
    }

    @Override
    public ListenableFuture<AvailabilityMetric> findAvailabilityData(AvailabilityMetric metric, long start, long end) {
        ResultSetFuture queryFuture = dataAccess.findAvailabilityData(metric, start, end);
        return Futures.transform(queryFuture, new AvailabilityMetricMapper());
    }

    @Override
    public ListenableFuture<List<NumericData>> findData(NumericMetric2 metric, long start, long end) {
        ResultSetFuture future = dataAccess.findData(metric, start, end);
        return Futures.transform(future, new NumericDataMapper(), metricsTasks);
    }

    @Override
    public ListenableFuture<Boolean> idExists(final String id) {
        ResultSetFuture future = dataAccess.findAllNumericMetrics();
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
    public ListenableFuture<List<String>> listMetrics() {
        ResultSetFuture future = dataAccess.findAllNumericMetrics();
        return Futures.transform(future, new Function<ResultSet, List<String>>() {
            @Override
            public List<String> apply(ResultSet resultSet) {
                List<String> metrics = new ArrayList<>();
                for (Row row : resultSet) {
                    metrics.add(row.getString(2));
                }
                return metrics;
            }
        }, metricsTasks);
    }

    @Override
    public ListenableFuture<Boolean> deleteMetric(String id) {
        ResultSetFuture future = dataAccess.deleteNumericMetric(DEFAULT_TENANT_ID, id, Interval.NONE, Metric.DPART);
        return Futures.transform(future, new Function<ResultSet, Boolean>() {
            @Override
            public Boolean apply(ResultSet input) {
                return input.isExhausted();
            }
        });
    }


    private void dropKeyspace(String keyspace) {
        session.get().execute("DROP KEYSPACE IF EXISTS " + keyspace);
    }

    @Override
    // TODO refactor to support multiple metrics
    // Data for different metrics and for the same tag are stored within the same partition
    // in the tags table; therefore, it makes sense for the API to support tagging multiple
    // metrics since they could efficiently be inserted in a single batch statement.
    public ListenableFuture<List<NumericData>> tagNumericData(NumericMetric2 metric, final Set<String> tags, long start,
        long end) {
        ResultSetFuture queryFuture = dataAccess.findData(metric, start, end, true);
        ListenableFuture<List<NumericData>> dataFuture = Futures.transform(queryFuture, new NumericDataMapper(true),
            metricsTasks);
        int ttl = getTTL(metric);
        ListenableFuture<List<NumericData>> updatedDataFuture = Futures.transform(dataFuture,
            new ComputeTTL<NumericData>(ttl), metricsTasks);
        return Futures.transform(updatedDataFuture, new AsyncFunction<List<NumericData>, List<NumericData>>() {
            @Override
            public ListenableFuture<List<NumericData>> apply(final List<NumericData> taggedData) {
                List<ResultSetFuture> insertFutures = new ArrayList<>(tags.size());
                for (String tag : tags) {
                    insertFutures.add(dataAccess.insertNumericTag(tag, taggedData));
                }
                for (NumericData d : taggedData) {
                    insertFutures.add(dataAccess.updateDataWithTag(d, tags));
                }
                ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insertFutures);
                return Futures.transform(insertsFuture, new Function<List<ResultSet>, List<NumericData>>() {
                    @Override
                    public List<NumericData> apply(List<ResultSet> resultSets) {
                        return taggedData;
                    }
                });
            }
        }, metricsTasks);
    }

    @Override
    public ListenableFuture<List<Availability>> tagAvailabilityData(AvailabilityMetric metric, final Set<String> tags,
        long start, long end) {
        ResultSetFuture queryFuture = dataAccess.findData(metric, start, end, true);
        ListenableFuture<List<Availability>> dataFuture = Futures.transform(queryFuture,
            new AvailabilityDataMapper(true), metricsTasks);
        int ttl = getTTL(metric);
        ListenableFuture<List<Availability>> updatedDataFuture = Futures.transform(dataFuture,
            new ComputeTTL<Availability>(ttl), metricsTasks);
        return Futures.transform(updatedDataFuture, new AsyncFunction<List<Availability>, List<Availability>>() {
            @Override
            public ListenableFuture<List<Availability>> apply(final List<Availability> taggedData) throws Exception {
                List<ResultSetFuture> insertFutures = new ArrayList<>(taggedData.size());
                for (String tag : tags) {
                    insertFutures.add(dataAccess.insertAvailabilityTag(tag, taggedData));
                }
                for (Availability a : taggedData) {
                    insertFutures.add(dataAccess.updateDataWithTag(a, tags));
                }
                ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insertFutures);
                return Futures.transform(insertsFuture, new Function<List<ResultSet>, List<Availability>>() {
                    @Override
                    public List<Availability> apply(List<ResultSet> resultSets) {
                        return taggedData;
                    }
                });
            }
        });
    }

    @Override
    public ListenableFuture<List<NumericData>> tagNumericData(NumericMetric2 metric, final Set<String> tags,
        long timestamp) {
        ListenableFuture<ResultSet> queryFuture = dataAccess.findData(metric, timestamp, true);
        ListenableFuture<List<NumericData>> dataFuture = Futures.transform(queryFuture, new NumericDataMapper(true),
            metricsTasks);
        int ttl = getTTL(metric);
        ListenableFuture<List<NumericData>> updatedDataFuture = Futures.transform(dataFuture,
            new ComputeTTL<NumericData>(ttl), metricsTasks);
        return Futures.transform(updatedDataFuture, new AsyncFunction<List<NumericData>, List<NumericData>>() {
            @Override
            public ListenableFuture<List<NumericData>> apply(final List<NumericData> data) throws Exception {
                if (data.isEmpty()) {
                    List<NumericData> results = Collections.emptyList();
                    return Futures.immediateFuture(results);
                }
                List<ResultSetFuture> insertFutures = new ArrayList<>(tags.size());
                for (String tag : tags) {
                    insertFutures.add(dataAccess.insertNumericTag(tag, data));
                }
                for (NumericData d : data) {
                    insertFutures.add(dataAccess.updateDataWithTag(d, tags));
                }
                ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insertFutures);
                return Futures.transform(insertsFuture, new Function<List<ResultSet>, List<NumericData>>() {
                    @Override
                    public List<NumericData> apply(List<ResultSet> resultSets) {
                        return data;
                    }
                });
            }
        });
    }

    @Override
    public ListenableFuture<List<Availability>> tagAvailabilityData(AvailabilityMetric metric, final Set<String> tags,
        long timestamp) {
        ListenableFuture<ResultSet> queryFuture = dataAccess.findData(metric, timestamp);
        ListenableFuture<List<Availability>> dataFuture = Futures.transform(queryFuture,
            new AvailabilityDataMapper(true), metricsTasks);
        int ttl = getTTL(metric);
        ListenableFuture<List<Availability>> updatedDataFuture = Futures.transform(dataFuture,
            new ComputeTTL<Availability>(ttl), metricsTasks);
        return Futures.transform(updatedDataFuture, new AsyncFunction<List<Availability>, List<Availability>>() {
            @Override
            public ListenableFuture<List<Availability>> apply(final List<Availability> data) throws Exception {
                if (data.isEmpty()) {
                    List<Availability> results = Collections.emptyList();
                    return Futures.immediateFuture(results);
                }
                List<ResultSetFuture> insertFutures = new ArrayList<>();
                for (String tag : tags) {
                    insertFutures.add(dataAccess.insertAvailabilityTag(tag, data));
                }
                for (Availability a : data) {
                    insertFutures.add(dataAccess.updateDataWithTag(a, tags));
                }
                ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insertFutures);
                return Futures.transform(insertsFuture, new Function<List<ResultSet>, List<Availability>>() {
                    @Override
                    public List<Availability> apply(List<ResultSet> resultSets) {
                        return data;
                    }
                });
            }
        });
    }

    @Override
    public ListenableFuture<Map<MetricId, Set<NumericData>>> findNumericDataByTags(String tenantId, Set<String> tags) {
        List<ListenableFuture<Map<MetricId, Set<NumericData>>>> queryFutures = new ArrayList<>(tags.size());
        for (String tag : tags) {
            queryFutures.add(Futures.transform(dataAccess.findNumericDataByTag(tenantId, tag),
                new TaggedNumericDataMapper(), metricsTasks));
        }
        ListenableFuture<List<Map<MetricId, Set<NumericData>>>> queriesFuture = Futures.allAsList(queryFutures);
        return Futures.transform(queriesFuture,
                new Function<List<Map<MetricId, Set<NumericData>>>, Map<MetricId, Set<NumericData>>>() {
            @Override
            public Map<MetricId, Set<NumericData>> apply(List<Map<MetricId, Set<NumericData>>> taggedDataMaps) {
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

                Map<MetricId, Set<NumericData>> mergedDataMap = new HashMap<>();
                for (MetricId id : ids) {
                    TreeSet<NumericData> set = new TreeSet<>(MetricData.TIME_UUID_COMPARATOR);
                    for (Map<MetricId, Set<NumericData>> taggedDataMap : taggedDataMaps) {
                        set.addAll(taggedDataMap.get(id));
                    }
                    mergedDataMap.put(id, set);
                }

                return mergedDataMap;
            }
        }, metricsTasks);

    }

    @Override
    public ListenableFuture<Map<MetricId, Set<Availability>>> findAvailabilityByTags(String tenantId,
        Set<String> tags) {
        List<ListenableFuture<Map<MetricId, Set<Availability>>>> queryFutures = new ArrayList<>(tags.size());
        for (String tag : tags) {
            queryFutures.add(Futures.transform(dataAccess.findAvailabilityByTag(tenantId, tag),
                new TaggedAvailabilityMappper(), metricsTasks));
        }
        ListenableFuture<List<Map<MetricId, Set<Availability>>>> queriesFuture = Futures.allAsList(queryFutures);
        return Futures.transform(queriesFuture,
                new Function<List<Map<MetricId, Set<Availability>>>, Map<MetricId, Set<Availability>>>() {
            @Override
            public Map<MetricId, Set<Availability>> apply(List<Map<MetricId, Set<Availability>>> taggedDataMaps) {
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

                Map<MetricId, Set<Availability>> mergedDataMap = new HashMap<>();
                for (MetricId id : ids) {
                    TreeSet<Availability> set = new TreeSet<>(MetricData.TIME_UUID_COMPARATOR);
                    for (Map<MetricId, Set<Availability>> taggedDataMap : taggedDataMaps) {
                        set.addAll(taggedDataMap.get(id));
                    }
                    mergedDataMap.put(id, set);
                }

                return mergedDataMap;
            }
        }, metricsTasks);
    }

    private int getTTL(Metric metric) {
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

    private static class RawDataFallback implements FutureFallback<ResultSet> {

        private Map<RawNumericMetric, Throwable> errors;

        private RawNumericMetric data;

        public RawDataFallback(Map<RawNumericMetric, Throwable> errors, RawNumericMetric data) {
            this.errors = errors;
            this.data = data;
        }

        @Override
        public ListenableFuture<ResultSet> create(Throwable t) throws Exception {
            errors.put(data, t);
            return Futures.immediateFailedFuture(t);
        }
    }

}
