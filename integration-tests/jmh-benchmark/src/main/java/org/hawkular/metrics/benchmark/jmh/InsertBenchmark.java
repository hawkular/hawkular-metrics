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
package org.hawkular.metrics.benchmark.jmh;

import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.benchmark.jmh.util.LiveCassandraManager;
import org.hawkular.metrics.benchmark.jmh.util.MetricServiceManager;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
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
@Warmup(iterations = 5)
@Measurement(iterations = 10) // Reduce the amount of iterations if you start to see GC interference
public class InsertBenchmark {

    @State(Scope.Benchmark)
    public static class ServiceCreator {
        private MetricsService metricsService;
        private MetricServiceManager metricsManager;

        @Setup
        public void setup() {
            metricsManager = new MetricServiceManager(new LiveCassandraManager());
//            metricsManager = new MetricServiceManager(new SCassandraManager());
//            metricsManager = new MetricServiceManager(new MockCassandraManager());
            metricsService = metricsManager.getMetricsService();
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

        @Param({"1", "10"})
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
//    @OperationsPerInvocation(100000) // Note, this is metric amount from param size, not datapoints
    public void insertBenchmark(GaugeMetricCreator creator, ServiceCreator service, Blackhole bh) {
        bh.consume(service.getMetricsService().addDataPoints(GAUGE, creator.getMetricObservable())
                .toBlocking().lastOrDefault(null));
    }

    // Equivalent of REST-test for inserting for single-metric id one call
    @Benchmark
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
