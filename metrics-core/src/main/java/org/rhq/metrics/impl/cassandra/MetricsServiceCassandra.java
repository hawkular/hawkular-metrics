package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
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

    private Session session;

    private DataAccess dataAccess;

    private MapQueryResultSet mapQueryResultSet = new MapQueryResultSet();

    private ListeningExecutorService metricsTasks = MoreExecutors
        .listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));

    // temporary until we have a better solution
    Set<String> ids = new TreeSet<>();

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

        updateSchemaIfNecessary(cluster);

        String keyspace = params.get("keyspace");
        if (keyspace==null||keyspace.isEmpty()) {
            logger.info("No explicit keyspace given, will default to 'rhq'");
            keyspace="rhq";
        }
        session = cluster.connect(keyspace);
        dataAccess = new DataAccess(session);
    }

    @Override
    public void shutdown() {
        session.close();
        session.getCluster().close();
    }

    public void setDataAccess(DataAccess dataAccess) {
        this.dataAccess = dataAccess;
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
            ids.add(metric.getId());
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
    public ListenableFuture<List<RawNumericMetric>> findData(String bucket, String id, long start, long end) {
        ResultSetFuture future = dataAccess.findData(bucket, id, start, end);
        return Futures.transform(future, mapQueryResultSet);
    }

    @Override
    public ListenableFuture<List<RawNumericMetric>> findData(String id, long start, long end) {
        return findData("raw", id, start, end);
    }

    @Override
    public boolean idExists(String id) {
        return ids.contains(id);
    }

    @Override
    public List<String> listMetrics() {
        return new ArrayList<>(ids);
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

    private void updateSchemaIfNecessary(Cluster cluster) {
        try (Session session = cluster.connect("system")) {
            SchemaManager schemaManager = new SchemaManager(session);
            schemaManager.updateSchema("rhq");
        }
    }

}
