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

package org.hawkular.rx.cassandra.driver;

import static java.util.stream.Collectors.toList;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.datastax.driver.core.Row;

import rx.Observable;
import rx.Observer;
import rx.observers.TestSubscriber;

/**
 * @author Thomas Segismont
 */
public class ResultSetToRowsTransformerTest {
    private static final ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    ResultSetToRowsTransformer transformer = new ResultSetToRowsTransformer();

    @AfterClass
    public static void shutdownExecutor() {
        EXECUTOR.shutdownNow();
    }

    @Rule
    public Timeout timeout = new Timeout(1, TimeUnit.MINUTES);

    @Test
    public void testEmptyResultSet() throws Exception {
        MockResultSet resultSet = MockResultSet.createEmpty();

        Observable<Row> rows = transformer.call(Observable.just(resultSet));

        TestSubscriber<Row> subscriber = new TestSubscriber<>();
        rows.subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertNoValues();
    }

    @Test
    public void testSinglePage() throws Exception {
        int rowCount = 763;
        MockResultSet resultSet = MockResultSet.createSinglePage(rowCount);

        Observable<Row> rows = transformer.call(Observable.just(resultSet));

        TestSubscriber<Row> subscriber = new TestSubscriber<>();
        rows.subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(rowCount);
        subscriber.assertReceivedOnNext(LongStream.range(0, rowCount).mapToObj(MockRow::new).collect(toList()));
    }

    @Test
    public void testMultiPage() throws Exception {
        int rowCount = 76377;
        int pageSize = 5000;
        MockResultSet resultSet = MockResultSet.createMultiPage(rowCount, pageSize);

        Observable<Row> rows = transformer.call(Observable.just(resultSet));

        TestSubscriber<Row> subscriber = new TestSubscriber<>();
        rows.subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(rowCount);
        subscriber.assertReceivedOnNext(LongStream.range(0, rowCount).mapToObj(MockRow::new).collect(toList()));
    }

    @Test
    public void testBackPressure() throws Exception {
        int rowCount = 76377;
        int pageSize = 5000;
        MockResultSet resultSet = MockResultSet.createMultiPage(rowCount, pageSize);

        Observable<Row> rows = transformer.call(Observable.just(resultSet));

        // Backpressure must be requested on the TestSubscriber instance, not the Observer delegate
        AtomicReference<TestSubscriber<Row>> subscriberRef = new AtomicReference<>();
        int initialRequest = 750;
        int intermediateRequest = 100;
        int totalRequest = initialRequest + intermediateRequest;
        TestSubscriber<Row> subscriber = new TestSubscriber<>(new Observer<Row>() {
            final AtomicLong received = new AtomicLong();

            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
            }

            @Override
            public void onNext(Row o) {
                long r = received.incrementAndGet();
                if (r % totalRequest == 50) {
                    // After 50 request more
                    subscriberRef.get().requestMore(intermediateRequest);
                    return;
                }
                if (r / totalRequest == 1) {
                    // Wait a bit and request more again
                    EXECUTOR.schedule(() -> {
                        subscriberRef.get().requestMore(initialRequest);
                    }, 5, TimeUnit.MILLISECONDS);
                }
            }
        }, initialRequest);
        subscriberRef.set(subscriber);
        rows.subscribe(subscriber);

        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(rowCount);
        subscriber.assertReceivedOnNext(LongStream.range(0, rowCount).mapToObj(MockRow::new).collect(toList()));
    }
}