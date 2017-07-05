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
package org.hawkular.metrics.core.compress;

import static java.util.Arrays.asList;

import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.joda.time.DateTime.now;

import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.core.service.metrics.BaseMetricsITest;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.testng.annotations.Test;

import com.datastax.driver.core.Row;

import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.observers.TestSubscriber;

/**
 * @author Michael Burman
 */
public class CompressionTest extends BaseMetricsITest {

    @Test
    void addAndCompressData() throws Exception {
        String tenantId = "t1";
        long start = now().getMillis();

        int amountOfMetrics = 1000;
        int datapointsPerMetric = 10;

        for (int j = 0; j < datapointsPerMetric; j++) {
            final int dpAdd = j;
            Observable<Metric<Double>> metrics = Observable.create(emitter -> {
                for (int i = 0; i < amountOfMetrics; i++) {
                    String metricName = String.format("m%d", i);
                    MetricId<Double> mId = new MetricId<>(tenantId, GAUGE, metricName);
                    emitter.onNext(new Metric<>(mId, asList(new DataPoint<>(start + dpAdd, 1.1))));
                }
                emitter.onCompleted();
            }, Emitter.BackpressureMode.BUFFER);

            TestSubscriber<Void> subscriber = new TestSubscriber<>();
            Observable<Void> observable = metricsService.addDataPoints(GAUGE, metrics);
            observable.subscribe(subscriber);
            subscriber.awaitTerminalEvent(20, TimeUnit.SECONDS); // For Travis..
            for (Throwable throwable : subscriber.getOnErrorEvents()) {
                throwable.printStackTrace();
            }
            subscriber.assertNoErrors();
            subscriber.assertCompleted();
        }

        Completable completable = metricsService.compressBlock(start, 2000, 2);

        TestSubscriber<Row> tsr = new TestSubscriber<>();
        completable.subscribe(tsr);
        tsr.awaitTerminalEvent(100, TimeUnit.SECONDS); // Travis again
        for (Throwable throwable : tsr.getOnErrorEvents()) {
            throwable.printStackTrace();
        }
        tsr.assertCompleted();
        tsr.assertNoErrors();

        // This happens too fast after the compression job has finished.. preparedstatements are still there
        // while the table is gone
        for (int i = 0; i < amountOfMetrics; i++) {
            String metricName = String.format("m%d", i);
            MetricId<Double> mId = new MetricId<>(tenantId, GAUGE, metricName);

            Observable<DataPoint<Double>> dataPoints =
                    metricsService.findDataPoints(mId, start, start + datapointsPerMetric + 1, 0, Order.ASC);

            TestSubscriber<DataPoint<Double>> tsrD = new TestSubscriber<>();
            dataPoints.subscribe(tsrD);
            tsrD.awaitTerminalEvent(10, TimeUnit.SECONDS);
            tsrD.assertCompleted();
            tsrD.assertNoErrors();
            tsrD.assertValueCount(datapointsPerMetric);
        }
    }

//    @Test
//    public void addAndCompressData() throws Exception {
//        // TODO This unit test works even if compression is broken.. as long as the data is not deleted
//        DateTime dt = new DateTime(2016, 9, 2, 14, 15, DateTimeZone.UTC); // Causes writes to go to compressed and one
//        // uncompressed table
//        DateTimeUtils.setCurrentMillisFixed(dt.getMillis());
//
//        DateTime start = dt.minusMinutes(30);
//        DateTime end = start.plusMinutes(20);
//
//        MetricId<Double> mId = new MetricId<>(tenantId, GAUGE, "m1");
//
//        metricsService.createTenant(new Tenant(tenantId), false).toBlocking().lastOrDefault(null);
//
//        Metric<Double> m1 = new Metric<>(mId, asList(
//                new DataPoint<>(start.getMillis(), 1.1),
//                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
//                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
//                new DataPoint<>(end.getMillis(), 4.4)));
//
//        Observable<Void> insertObservable = metricsService.addDataPoints(GAUGE, Observable.just(m1));
//        insertObservable.toBlocking().lastOrDefault(null);
//
//        DateTime startSlice = DateTimeService.getTimeSlice(start, CompressData.DEFAULT_BLOCK_SIZE);
////        DateTime endSlice = startSlice.plus(CompressData.DEFAULT_BLOCK_SIZE);
//
//        System.out.printf("===================> Processing %d compressing %d\n", start.getMillis(), startSlice.getMillis());
//
//        Completable compressCompletable =
//                metricsService.compressBlock(startSlice.getMillis(), COMPRESSION_PAGE_SIZE)
//                .doOnError(Throwable::printStackTrace);
////                metricsService.compressBlock(Observable.just(mId), startSlice.getMillis(), endSlice.getMillis(),
////                        COMPRESSION_PAGE_SIZE, PublishSubject.create()).doOnError(Throwable::printStackTrace);
//
//        TestSubscriber<Void> testSubscriber = new TestSubscriber<>();
//        compressCompletable.subscribe(testSubscriber);
//        testSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
//        testSubscriber.assertCompleted();
//        testSubscriber.assertNoErrors();
//
//        Observable<DataPoint<Double>> observable = metricsService.findDataPoints(mId, start.getMillis(),
//                end.getMillis() + 1, 0, Order.DESC);
//        List<DataPoint<Double>> actual = toList(observable);
//        List<DataPoint<Double>> expected = asList(
//                new DataPoint<>(end.getMillis(), 4.4),
//                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
//                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
//                new DataPoint<>(start.getMillis(), 1.1));
//
//        assertEquals(actual, expected, "The data does not match the expected values");
//        assertMetricIndexMatches(tenantId, GAUGE, singletonList(new Metric<>(m1.getMetricId(), m1.getDataPoints(), 7)));
//
//        observable = metricsService.findDataPoints(mId, start.getMillis(),
//                end.getMillis(), 0, Order.DESC);
//        actual = toList(observable);
//        assertEquals(3, actual.size(), "Last datapoint should be missing (<)");
//
//        DateTimeUtils.setCurrentMillisSystem();
//    }

    @Test
    public void testNonExistantCompression() throws Exception {
        // TODO Test compressBlock with a timestamp far away in the past
        /*
        No errors, no data_0 compression
         */

        // Write data to the data_0 table
        // compress
        // assertNoErrors
        // assertNothingCompressed
        // assertData0RemainsIntact
    }
}
