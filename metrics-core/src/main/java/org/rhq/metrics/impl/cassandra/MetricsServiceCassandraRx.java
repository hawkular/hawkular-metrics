package org.rhq.metrics.impl.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricsServiceRx;
import org.rhq.metrics.core.MetricsThreadFactory;
import org.rhq.metrics.core.NumericMetric2;
import org.rhq.metrics.core.Tenant;
import org.rhq.metrics.core.TenantAlreadyExistsException;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * @author John Sanda
 */
public class MetricsServiceCassandraRx implements MetricsServiceRx {

    private static final Logger logger = LoggerFactory.getLogger(MetricsServiceCassandraRx.class);

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

    private DataAccess dataAccess;

    private Map<String, Tenant> tenants = new ConcurrentHashMap<>();

    private ListeningExecutorService metricsTasks = MoreExecutors
        .listeningDecorator(Executors.newFixedThreadPool(4, new MetricsThreadFactory()));

    public MetricsServiceCassandraRx(DataAccess dataAccess) {
        this.dataAccess = dataAccess;
    }

    @Override
    public ListenableFuture<Void> createTenant(final Tenant tenant) {
        ResultSetFuture future = dataAccess.insertTenant(tenant);
        return Futures.transform(future, new Function<ResultSet, Void>() {
            @Override
            public Void apply(ResultSet resultSet) {
                if (resultSet.wasApplied()) {
                    tenants.put(tenant.getId(), tenant);
                    return null;
                }
                throw new TenantAlreadyExistsException(tenant.getId());
            }
        }, metricsTasks);
    }

    @Override
    public ListenableFuture<Void> addNumericData(List<NumericMetric2> metrics) {
        List<ResultSetFuture> insertFutures = new ArrayList<>(metrics.size());
        for (NumericMetric2 metric : metrics) {
            if (metric.getData().isEmpty()) {
                logger.warn("There is no data to insert for {}", metric);
            } else {
                insertFutures.add(dataAccess.insertData(metric));
            }
        }
        insertFutures.add(dataAccess.updateMetricsIndex(metrics));
        ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(insertFutures);
        return Futures.transform(insertsFuture, RESULT_SETS_TO_VOID, metricsTasks);
    }

    @Override
    public Observable<NumericMetric2> findNumericData(NumericMetric2 metric, long start, long end) {
        metric.setDpart(Metric.DPART);
        Observable<ResultSet> observable = dataAccess.findDataRx(metric, start, end);
        return observable.flatMap(new Func1<ResultSet, Observable<Row>>() {
            @Override
            public Observable<Row> call(final ResultSet resultSet) {
                return Observable.create(new Observable.OnSubscribe<Row>() {
                    @Override
                    public void call(Subscriber<? super Row> subscriber) {
                        for (Row row : resultSet) {
                            subscriber.onNext(row);
                        }
                        subscriber.onCompleted();
                    }
                });
            }
        }).reduce(new NumericMetric2(metric.getTenantId(), metric.getId()),
            new Func2<NumericMetric2, Row, NumericMetric2>() {
                @Override
                public NumericMetric2 call(NumericMetric2 metric, Row row) {
                    if (metric.getData().isEmpty()) {
                        metric.setMetadata(row.getMap(5, String.class, String.class));
                    }
                    metric.addData(row.getUUID(4), row.getDouble(6));
                    return metric;
                }
            });
    }
}
