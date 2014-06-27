package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import com.google.common.collect.ImmutableMap;
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
import org.rhq.metrics.core.DataAccess;
import org.rhq.metrics.core.DataType;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.MetricsThreadFactory;
import org.rhq.metrics.core.RawMetricMapper;
import org.rhq.metrics.core.RawNumericMetric;
import org.rhq.metrics.core.SchemaManager;

/**
 * @author John Sanda
 */
public class MetricsServiceCassandra implements MetricsService {

    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceCassandra.class);

    public static final String REQUEST_LIMIT = "rhq.metrics.request.limit";

    private static final int RAW_TTL = Duration.standardDays(7).toStandardSeconds().getSeconds();

    private static final Function<ResultSet, Void> TO_VOID = new Function<ResultSet, Void>() {
        @Override
        public Void apply(ResultSet resultSet) {
            return null;
        }
    };

    private RateLimiter permits = RateLimiter.create(Double.parseDouble(
        System.getProperty(REQUEST_LIMIT, "30000")), 3, TimeUnit.MINUTES);

    private Optional<Session> session;

    private DataAccess dataAccess;

    private MapQueryResultSet mapQueryResultSet = new MapQueryResultSet();

    private Function<ResultSet, List<Counter>> mapCounters = new Function<ResultSet, List<Counter>>() {
        @Override
        public List<Counter> apply(ResultSet resultSet) {
            List<Counter> counters = new ArrayList<>();
            for (Row row : resultSet) {
                counters.add(new Counter(row.getString(0), row.getString(1), row.getLong(2)));
            }
            return counters;
        }
    };

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

    public ListenableFuture<Void> addData(RawNumericMetric data) {
        permits.acquire();
        ResultSetFuture future = dataAccess.insertData(data.getBucket(), data.getId(), data.getTimestamp(),
            ImmutableMap.of(DataType.RAW.ordinal(), data.getValue()), RAW_TTL);
        return Futures.transform(future, TO_VOID);
    }

    @Override
    public ListenableFuture<Map<RawNumericMetric, Throwable>> addData(Set<RawNumericMetric> data) {
        final Map<RawNumericMetric, Throwable> errors = new HashMap<>();
        List<ResultSetFuture> futures = new ArrayList<>(data.size());

        for (final RawNumericMetric metric : data) {
            permits.acquire();
            ResultSetFuture future = dataAccess.insertData(metric.getBucket(), metric.getId(), metric.getTimestamp(),
                ImmutableMap.of(DataType.RAW.ordinal(), metric.getAvg()), RAW_TTL);
            Futures.withFallback(future, new RawDataFallback(errors, metric));
            futures.add(future);
        }
        ListenableFuture<List<ResultSet>> insertsFuture = Futures.successfulAsList(futures);

        return Futures.transform(insertsFuture, new Function<List<ResultSet>, Map<RawNumericMetric, Throwable>>() {
            @Override
            public Map<RawNumericMetric, Throwable> apply(List<ResultSet> resultSets) {
                return errors;
            }
        });
    }

    @Override
    public ListenableFuture<Void> updateCounter(Counter counter) {
        return Futures.transform(dataAccess.updateCounter(counter), TO_VOID);
    }

    @Override
    public ListenableFuture<Void> updateCounters(Collection<Counter> counters) {
        ResultSetFuture future = dataAccess.updateCounters(counters);
        return Futures.transform(future, TO_VOID);
    }

    @Override
    public ListenableFuture<List<Counter>> findCounters(String group) {
        ResultSetFuture future = dataAccess.findCounters(group);
        return Futures.transform(future, mapCounters, metricsTasks);
    }

    @Override
    public ListenableFuture<List<Counter>> findCounters(String group, List<String> counterNames) {
        ResultSetFuture future = dataAccess.findCounters(group, counterNames);
        return Futures.transform(future, mapCounters, metricsTasks);
    }

    @Override
    public ListenableFuture<List<RawNumericMetric>> findData(String bucket, String id, long start, long end) {
        ResultSetFuture future = dataAccess.findData(bucket, id, start, end);
        return Futures.transform(future, mapQueryResultSet, metricsTasks);
    }

    @Override
    public ListenableFuture<List<RawNumericMetric>> findData(String id, long start, long end) {
        return findData("raw", id, start, end);
    }

    @Override
    public ListenableFuture<Boolean> idExists(final String id) {
        ResultSetFuture rsf = dataAccess.listMetricNames();
        return Futures.transform(rsf, new Function<ResultSet, Boolean>() {
            @Override
            public Boolean apply(ResultSet input) {
                boolean found = false;
                for (Row row : input.all()) {
                    String name = row.getString("metric_id");
                    if (name.equals(id)) {
                        found = true;
                        break;
                    }
                }
                return found;
            }
        });
    }

    @Override
    public ListenableFuture<List<String>> listMetrics() {
        ResultSetFuture rsf = dataAccess.listMetricNames();
        return Futures.transform(rsf, new Function<ResultSet, List<String>>() {
            @Override
            public List<String> apply(ResultSet input) {
                List<String> result = new ArrayList<String>();
                for (Row row : input.all()) {
                    String name = row.getString("metric_id");
                    result.add(name);
                }
                return result;
            }
        });
    }

    @Override
    public ListenableFuture<Boolean> deleteMetric(String id) {
        ResultSetFuture future = dataAccess.removeData(id);
        return Futures.transform(future, new Function<ResultSet, Boolean>() {
            @Override
            public Boolean apply(ResultSet input) {
                return input.isExhausted();
            }
        });
    }

    private void updateSchemaIfNecessary(Cluster cluster, String schemaName) {
        try (Session session = cluster.connect("system")) {
            SchemaManager schemaManager = new SchemaManager(session);
            try {
                schemaManager.updateSchema(schemaName);
            } catch (Exception e) {
                logger.error("Schema update failed: " + e);
                throw e;
            }
        }
    }

    private void dropKeyspace(Cluster cluster, String keyspace) {
        try (Session session = cluster.connect("system")) {
            logger.info("Removing keyspace '" + keyspace + "'");
            session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
        }
    }


    private class RawDataFallback implements FutureFallback<ResultSet> {

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

    private class MapQueryResultSet implements Function<ResultSet, List<RawNumericMetric>> {

        RawMetricMapper mapper = new RawMetricMapper();

        @Override
        public List<RawNumericMetric> apply(ResultSet resultSet) {
            return mapper.map(resultSet);
        }
    }
}
