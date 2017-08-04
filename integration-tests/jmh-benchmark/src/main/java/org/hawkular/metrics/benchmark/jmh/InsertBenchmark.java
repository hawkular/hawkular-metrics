/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.benchmark.jmh;

import static org.hawkular.metrics.core.service.TimeUUIDUtils.getTimeUUID;
import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.benchmark.jmh.util.LiveCassandraManager;
import org.hawkular.metrics.benchmark.jmh.util.MetricServiceManager;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.transformers.BatchStatementTransformer;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.datastax.driver.core.PreparedStatement;

import rx.Observable;
import rx.Subscriber;

/**
 * Creates benchmarks that use MetricsServiceImpl directly, without creating JSON or REST overhead. This should
 * allow testing the internal speed of our MetricsService/DataAccess implementations. The backend of Session
 * object is configurable.
 *
 * @author Michael Burman
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5) // Reduce the amount of iterations if you start to see GC interference
public class InsertBenchmark {

    @State(Scope.Benchmark)
    public static class ServiceCreator {
        private MetricsService metricsService;
        private MetricServiceManager metricsManager;

        public RxSession rxSession;

        public PreparedStatement createSingleTable;
        public PreparedStatement createMultiTable;

        public PreparedStatement truncateSingleTable;
        public PreparedStatement truncateMultiTable;

        public PreparedStatement insertPartition;
        public PreparedStatement insertMultiPartition;

        @Setup
        public void setup() {
            metricsManager = new MetricServiceManager(new LiveCassandraManager());
//            metricsManager = new MetricServiceManager(new SCassandraManager());
//            metricsManager = new MetricServiceManager(new MockCassandraManager());
            metricsService = metricsManager.getMetricsService();

            rxSession = new RxSessionImpl(metricsManager.getSession());

            metricsManager.getSession().execute("ALTER KEYSPACE benchmark WITH replication = {'class': " +
                    "'SimpleStrategy', 'replication_factor': 1" +
                    "} AND durable_writes = false");

            createSingleTable = metricsManager.getSession().prepare(
                    "CREATE TABLE IF NOT EXISTS data_bench_single (tenant_id text, type tinyint, metric text, dpart " +
                            "bigint, time " +
                            "timeuuid, n_value double, PRIMARY KEY ((tenant_id, type, dpart), time)) WITH CLUSTERING ORDER BY (time DESC)");

            createMultiTable = metricsManager.getSession().prepare(
                    "CREATE TABLE IF NOT EXISTS data_bench_multi (tenant_id text, type tinyint, metric text, dpart " +
                            "bigint, time " +
                            "timeuuid, n_value double, PRIMARY KEY ((tenant_id, type, metric, dpart), time)) WITH CLUSTERING ORDER BY (time DESC)");

            metricsManager.getSession().execute(createMultiTable.bind());
            metricsManager.getSession().execute(createSingleTable.bind());

            truncateSingleTable = metricsManager.getSession().prepare("TRUNCATE TABLE data_bench_single");
            truncateMultiTable = metricsManager.getSession().prepare("TRUNCATE TABLE data_bench_multi");

            // Temp table with a single partition
            insertPartition = metricsManager.getSession().prepare(
                    "UPDATE data_bench_single " +
                            "SET n_value = ? " +
                            "WHERE tenant_id = ? AND type = ? AND dpart = 0 AND time = ? ");

            // Current table in temp format
            insertMultiPartition = metricsManager.getSession().prepare(
                    "UPDATE data_bench_multi " +
                            "SET n_value = ? " +
                            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = 0 AND time = ? ");
        }

        @TearDown
        public void shutdown() {
            metricsManager.shutdown();
        }

        public MetricsService getMetricsService() {
            return metricsService;
        }
    }

    @State(Scope.Benchmark)
    public static class GaugeMetricCreator {

        @Param({"100000"}) // 1M caused some GC issues and unstable test results
        public int size;

//        @Param({"1", "10"})
        @Param({"1"})
        public int datapointsPerMetric;

        private List<Metric<Double>> metricList;

        @Setup(Level.Trial)
        public void setup() {
            size = size*datapointsPerMetric;
            final long timestamp = System.currentTimeMillis();

            List<Metric<Double>> metrics = new ArrayList<>(size);

            for (int i = 0; i < size; i += datapointsPerMetric) {
                List<DataPoint<Double>> points = new ArrayList<>();
                for (int j = 0; j < datapointsPerMetric; j++) {
                    points.add(new DataPoint<>(timestamp + i, (double) j));
                }
                Metric<Double> metric =
                        new Metric<>(new MetricId<>("b", GAUGE, "insert.metrics.test." + i), points);
                metrics.add(metric);
            }

            this.metricList = metrics;
        }

        public Observable<Metric<Double>> getMetricObservable() {
            return Observable.from(metricList);
        }
    }

    // Equivalent to REST-tests for inserting size-amount of metrics in one call
//    @Benchmark
    @OperationsPerInvocation(100000) // Note, this is metric amount from param size, not datapoints
    public void insertBenchmarkRxJava1(GaugeMetricCreator creator, ServiceCreator service, Blackhole bh) {
        bh.consume(service.getMetricsService().addDataPoints(GAUGE, creator.getMetricObservable())
                .toBlocking().lastOrDefault(null));
    }

    // Equivalent of REST-test for inserting for single-metric id one call
//    @Benchmark
    @OperationsPerInvocation(100000) // Note, this is metric amount from param size, not datapoints
//    @Fork(jvmArgsAppend =
//            {"-XX:+UnlockCommercialFeatures",
//                    "-XX:+FlightRecorder",
//                    "-XX:StartFlightRecording=duration=60s,filename=./profiling-data.jfr,name=profile,settings=profile",
//                    "-XX:FlightRecorderOptions=settings=/usr/lib/jvm/java-8-oracle/jre/lib/jfr/profile.jfc,samplethreads=true"
//            })
    public void insertBenchmarkSingle(GaugeMetricCreator creator, ServiceCreator service, Blackhole bh) {
        bh.consume(creator.getMetricObservable()
                .flatMap(m -> service.getMetricsService().addDataPoints(GAUGE, Observable.just(m)))
                .toBlocking().lastOrDefault(null));
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void insertToMultiplePartitions(GaugeMetricCreator creator, ServiceCreator service, Blackhole bh) {
        bh.consume(
                creator.getMetricObservable()
                .flatMap(m ->
                        Observable.from(m.getDataPoints())
                                .map(d -> service.insertMultiPartition.bind(d.getValue(), m.getTenantId(),
                                        m.getMetricId().getType().getCode(),
                                        m.getMetricId().getName(), getTimeUUID(d.getTimestamp())))
                )
                        .compose(new BatchStatementTransformer())
                        .flatMap(batch -> service.rxSession.execute(batch).map(resultSet -> batch.size()))
//                .doOnCompleted(() -> service.rxSession.execute(service.truncateMultiTable.bind()))
                .toBlocking().lastOrDefault(null)
        );
    }

//    @Benchmark
    @OperationsPerInvocation(100000)
    public void insertToSinglePartition(GaugeMetricCreator creator, ServiceCreator service, Blackhole bh) {
        bh.consume(
                creator.getMetricObservable()
                        .flatMap(m ->
                                Observable.from(m.getDataPoints())
                                        .map(d -> service.insertPartition.bind(d.getValue(), m.getTenantId(),
                                                m.getMetricId().getType().getCode(),
                                                getTimeUUID(d.getTimestamp())))
                        )
                        .compose(new BatchStatementTransformer())
                        .flatMap(batch -> service.rxSession.execute(batch).map(resultSet -> batch.size()))
//                        .doOnCompleted(() -> service.rxSession.execute(service.truncateMultiTable.bind()))
                        .toBlocking().lastOrDefault(null)
        );

    }

    @SuppressWarnings("unused")
    private static final class GenericSubscriber<T> extends Subscriber<T> {
        final Blackhole bh;
        public GenericSubscriber(long r, Blackhole bh) {
            this.bh = bh;
            request(r);
        }

        @Override
        public void onNext(T t) {
            bh.consume(t);
        }

        @Override
        public void onError(Throwable e) {
            e.printStackTrace();
        }

        @Override
        public void onCompleted() {

        }
    }
}
