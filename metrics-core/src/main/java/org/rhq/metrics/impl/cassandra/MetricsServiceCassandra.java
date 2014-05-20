package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
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

/**
 * @author John Sanda
 */
public class MetricsServiceCassandra implements MetricsService {

    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceCassandra.class);

    public static final String REQUEST_LIMIT = "rhq.metrics.request.limit";

    private static final int RAW_TTL = Duration.standardDays(7).toStandardSeconds().getSeconds();

    private RateLimiter permits = RateLimiter.create(Double.parseDouble(
        System.getProperty(REQUEST_LIMIT, "30000")), 3, TimeUnit.MINUTES);

    private DataAccess dataAccess;

    private MapQueryResultSet mapQueryResultSet = new MapQueryResultSet();

    private ListeningExecutorService metricsTasks = MoreExecutors
        .listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));

    public void setDataAccess(DataAccess dataAccess) {
        this.dataAccess = dataAccess;
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
        return false;  // TODO: Customise this generated block
    }

    @Override
    public List<String> listMetrics() {
        return Collections.emptyList();
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
