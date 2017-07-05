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
import static org.junit.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.DataAccessImpl;
import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.core.service.metrics.BaseMetricsITest;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.AggregateMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.FunctionMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SchemaChangeListener;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;

import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.observers.TestSubscriber;

/**
 * Additional tests targeting the compressBlock function in the MetricsServiceImpl. For job execution tests,
 * check CompressDataJobITest
 *
 * @author Michael Burman
 */
public class CompressionTest extends BaseMetricsITest {

    private String tenantId;
    private static double startValue = 1.1;

    @BeforeMethod
    public void initTest(Method method) {
        tenantId = method.getName();
    }


    private void createAndInsertMetrics(long start, int amountOfMetrics, int datapointsPerMetric) {
        for (int j = 0; j < datapointsPerMetric; j++) {
            final int dpAdd = j;
            Observable<Metric<Double>> metrics = Observable.create(emitter -> {
                for (int i = 0; i < amountOfMetrics; i++) {
                    String metricName = String.format("m%d", i);
                    MetricId<Double> mId = new MetricId<>(tenantId, GAUGE, metricName);
                    emitter.onNext(new Metric<>(mId, asList(new DataPoint<>(start + dpAdd, startValue + dpAdd))));
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
    }

    private void compressData(long start) {
        Completable completable = metricsService.compressBlock(start, 2000, 2);

        TestSubscriber<Row> tsr = new TestSubscriber<>();
        completable.subscribe(tsr);
        tsr.awaitTerminalEvent(100, TimeUnit.SECONDS); // Travis again
        for (Throwable throwable : tsr.getOnErrorEvents()) {
            throwable.printStackTrace();
        }
        tsr.assertCompleted();
        tsr.assertNoErrors();
    }

    @Test
    void addAndCompressData() throws Exception {
        long start = now().getMillis();

        int amountOfMetrics = 1000;
        int datapointsPerMetric = 10;

        createAndInsertMetrics(start, amountOfMetrics, datapointsPerMetric);

        CountDownLatch latch = new CountDownLatch(1);
        latchingSchemaChangeListener(latch);
        compressData(start);

        // Verify the count from the data_compressed table
        TestSubscriber<Long> ts = new TestSubscriber<>();

        rxSession.executeAndFetch("SELECT COUNT(*) FROM data_compressed")
                .map(r -> r.getLong(0))
                .subscribe(ts);

        ts.awaitTerminalEvent(2, TimeUnit.SECONDS);
        ts.assertNoErrors();
        ts.assertCompleted();
        List<Long> onNextEvents = ts.getOnNextEvents();
        assertEquals(onNextEvents.size(), 1);
        assertEquals(onNextEvents.get(0).longValue(), amountOfMetrics);

        // Now read it through findDataPoints also
        assertTrue(latch.await(5, TimeUnit.SECONDS));

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

            List<DataPoint<Double>> datapoints = tsrD.getOnNextEvents();
            for(int h = 0; h < datapoints.size(); h++) {
                assertEquals(datapoints.get(h).getValue(), startValue + h);
            }
        }
    }

    @Test
    public void testNonExistantCompression() throws Exception {
        // Write to past .. should go to data_0 table
        long start = now().minusDays(2).getMillis();
        createAndInsertMetrics(start, 100, 1);
        compressData(start); // Try to compress table in the past

        // Verify that the out of order table was not dropped
        TableMetadata table = session.getCluster().getMetadata().getKeyspace(getKeyspace())
                .getTable(DataAccessImpl.OUT_OF_ORDER_TABLE_NAME);

        assertNotNull(table, "data_0 should not have been dropped");

        TestSubscriber<Long> ts = new TestSubscriber<>();

        // Verify that the out of order table was not compressed
        rxSession.executeAndFetch(String.format("SELECT COUNT(*) FROM %s", DataAccessImpl.OUT_OF_ORDER_TABLE_NAME))
                .map(r -> r.getLong(0))
                .subscribe(ts);

        ts.awaitTerminalEvent(2, TimeUnit.SECONDS);
        ts.assertNoErrors();
        ts.assertCompleted();
        List<Long> onNextEvents = ts.getOnNextEvents();
        assertEquals(onNextEvents.size(), 1);
        assertEquals(onNextEvents.get(0).longValue(), 100);
    }

    private void latchingSchemaChangeListener(CountDownLatch latch) {
        session.getCluster().register(new SchemaChangeListener() {
            @Override public void onKeyspaceAdded(KeyspaceMetadata keyspaceMetadata) {

            }

            @Override public void onKeyspaceRemoved(KeyspaceMetadata keyspaceMetadata) {

            }

            @Override
            public void onKeyspaceChanged(KeyspaceMetadata keyspaceMetadata, KeyspaceMetadata keyspaceMetadata1) {

            }

            @Override public void onTableAdded(TableMetadata tableMetadata) {

            }

            @Override public void onTableRemoved(TableMetadata tableMetadata) {
                latch.countDown();
            }

            @Override public void onTableChanged(TableMetadata tableMetadata, TableMetadata tableMetadata1) {

            }

            @Override public void onUserTypeAdded(UserType userType) {

            }

            @Override public void onUserTypeRemoved(UserType userType) {

            }

            @Override public void onUserTypeChanged(UserType userType, UserType userType1) {

            }

            @Override public void onFunctionAdded(FunctionMetadata functionMetadata) {

            }

            @Override public void onFunctionRemoved(FunctionMetadata functionMetadata) {

            }

            @Override
            public void onFunctionChanged(FunctionMetadata functionMetadata, FunctionMetadata functionMetadata1) {

            }

            @Override public void onAggregateAdded(AggregateMetadata aggregateMetadata) {

            }

            @Override public void onAggregateRemoved(AggregateMetadata aggregateMetadata) {

            }

            @Override
            public void onAggregateChanged(AggregateMetadata aggregateMetadata, AggregateMetadata aggregateMetadata1) {

            }

            @Override public void onMaterializedViewAdded(MaterializedViewMetadata materializedViewMetadata) {

            }

            @Override public void onMaterializedViewRemoved(MaterializedViewMetadata materializedViewMetadata) {

            }

            @Override public void onMaterializedViewChanged(MaterializedViewMetadata materializedViewMetadata,
                                                            MaterializedViewMetadata materializedViewMetadata1) {

            }

            @Override public void onRegister(Cluster cluster) {

            }

            @Override public void onUnregister(Cluster cluster) {

            }
        });
    }
}
