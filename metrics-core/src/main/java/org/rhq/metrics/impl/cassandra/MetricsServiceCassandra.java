package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.Interval;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.MetricsThreadFactory;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.RawMetricMapper;
import org.rhq.metrics.core.RawNumericMetric;
import org.rhq.metrics.core.SchemaManager;

/**
 * @author John Sanda
 */
public class MetricsServiceCassandra implements MetricsService {

    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceCassandra.class);

    public static final String REQUEST_LIMIT = "rhq.metrics.request.limit";

    private static final long DPART = 0;

    private static final int RAW_TTL = Duration.standardDays(7).toStandardSeconds().getSeconds();

    private static final Function<List<ResultSet>, Void> RESULT_SETS_TO_VOID = new Function<List<ResultSet>, Void>() {
        @Override
        public Void apply(List<ResultSet> resultSets) {
            return null;
        }
    };

    private RateLimiter permits = RateLimiter.create(Double.parseDouble(
        System.getProperty(REQUEST_LIMIT, "30000")), 3, TimeUnit.MINUTES);

    private Optional<Session> session;

    private DataAccess dataAccess;

    private NumericDataMapper numericDataMapper = new NumericDataMapper();

    private ListeningExecutorService metricsTasks = MoreExecutors
        .listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));


    @Override
    public void startUp(Session s) {
        // the session is managed externally
        this.session = Optional.absent();
        this.dataAccess = new DataAccess(s);
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

        if (System.getProperty("cassandra.resetdb")!=null) {
            // We want a fresh DB -- mostly used for tests
            dropKeyspace(cluster, keyspace);
        }

        // This creates/updates the keyspace + tables if needed
        updateSchemaIfNecessary(cluster, keyspace);

        session = Optional.of(cluster.connect(keyspace));
        dataAccess = new DataAccess(session.get());

    }

    @Override
    public void shutdown() {
        if(session.isPresent()) {
            Session s = session.get();
            s.close();
            s.getCluster().close();
        }
    }

    @Override
    public ListenableFuture<Void> addNumericData(Set<NumericData> data) {
        List<ResultSetFuture> futures = new ArrayList<>(data.size());
        for (NumericData d : data) {
            permits.acquire();
            if (d.getTimeUUID() == null) {
                Interval interval = d.getInterval() == null ? Interval.NONE : d.getInterval();
                futures.add(dataAccess.addNumericAttributes(d.getTenantId(), d.getMetric(), interval, DPART,
                    d.getAttributes()));
            } else {
                futures.add(dataAccess.insertNumericData(d));
            }
        }
        ListenableFuture<List<ResultSet>> insertsFuture = Futures.successfulAsList(futures);
        // TODO Should we include any errors in the return value?
        // By returning a void future, we swallow any errors that might occur which if
        // nothing else makes development/testing more difficult. We ought to propagate
        // errors back to the client. Doing it with Guava's futures can be limiting. Might
        // be better and easier to tackle with RxJava's Observables.
        return Futures.transform(insertsFuture, RESULT_SETS_TO_VOID);
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
    public ListenableFuture<List<NumericData>> findData(String tenantId, String id, long start, long end) {
        ResultSetFuture future = dataAccess.findNumericData(tenantId, id, Interval.NONE, DPART, start, end);
        return Futures.transform(future, numericDataMapper, metricsTasks);
    }

    @Override
    public ListenableFuture<Boolean> idExists(final String id) {
        ResultSetFuture future = dataAccess.findAllNumericMetrics();
        return Futures.transform(future, new Function<ResultSet, Boolean>() {
            @Override
            public Boolean apply(ResultSet resultSet) {
                for (Row row : resultSet) {
                    if (id.equals(row.getString(1))) {
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
                    metrics.add(row.getString(1));
                }
                return metrics;
            }
        }, metricsTasks);
    }

    @Override
    public ListenableFuture<Boolean> deleteMetric(String id) {
        ResultSetFuture future = dataAccess.deleteNumericMetric(DEFAULT_TENANT_ID, id, Interval.NONE, DPART);
        return Futures.transform(future, new Function<ResultSet, Boolean>() {
            @Override
            public Boolean apply(ResultSet input) {
                return input.isExhausted();
            }
        });
    }


    private void dropKeyspace(Cluster cluster, String keyspace) {
        try (Session session = cluster.connect("system")) {
            logger.info("Removing keyspace '" + keyspace + "'");
            session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
        }
    }

    @Override
    public ListenableFuture<List<NumericData>> tagData(String tenantId, final Set<String> tags, String metric,
        long start, long end) {
        ListenableFuture<List<NumericData>> queryFuture = findData(tenantId, metric, start, end);
        return Futures.transform(queryFuture, new AsyncFunction<List<NumericData>, List<NumericData>>() {
            @Override
            public ListenableFuture<List<NumericData>> apply(final List<NumericData> taggedData) {
                List<ResultSetFuture> insertFutures = new ArrayList<>(tags.size());
                for (String tag : tags) {
                    insertFutures.add(dataAccess.insertTag(tag, taggedData));
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
    };

    @Override
    public ListenableFuture<List<NumericData>> findDataByTags(String tenantId, Set<String> tags) {
        String tag = tags.iterator().next();
        ListenableFuture<ResultSet> queryFuture = dataAccess.findData(tenantId, tag);
        return Futures.transform(queryFuture, new TaggedDataMapper(), metricsTasks);
    }

    private void updateSchemaIfNecessary(Cluster cluster, String schemaName) {
        try (Session session = cluster.connect("system")) {
            SchemaManager schemaManager = new SchemaManager(session);
            try {
                schemaManager.createSchema(schemaName);
            } catch (Exception e) {
                logger.error("Schema update failed: " + e);
                throw new RuntimeException(e);
            }
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

    private static class MapQueryResultSet implements Function<ResultSet, List<RawNumericMetric>> {

        RawMetricMapper mapper = new RawMetricMapper();

        @Override
        public List<RawNumericMetric> apply(ResultSet resultSet) {
            return mapper.map(resultSet);
        }
    }
}
