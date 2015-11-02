/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.impl;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.joda.time.DateTime;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import rx.Observable;
import rx.observers.TestSubscriber;

/**
 * @author John Sanda
 */
public class MetricsITest {

    private static final long FUTURE_TIMEOUT = 3;

    protected Session session;

    protected RxSession rxSession;

    private PreparedStatement truncateMetrics;

    private PreparedStatement truncateCounters;

    public void initSession() {
        String nodeAddresses = System.getProperty("nodes", "127.0.0.1");
        Cluster cluster = new Cluster.Builder()
            .addContactPoints(nodeAddresses.split(","))
            // Due to JAVA-500 and JAVA-509 we need to explicitly set the protocol to V3.
            // These bugs are fixed upstream and will be in version 2.1.3 of the driver.
                .withProtocolVersion(ProtocolVersion.V4)
                .withSpeculativeExecutionPolicy(NoSpeculativeExecutionPolicy.INSTANCE)
            .build();
        session = cluster.connect(getKeyspace());
        rxSession = new RxSessionImpl(session);

//        truncateMetrics = session.prepare("TRUNCATE metrics");
//        truncateCounters = session.prepare("TRUNCATE counters");
    }

    protected void resetDB() {
        session.execute(truncateMetrics.bind());
        session.execute(truncateCounters.bind());
    }

    protected String getKeyspace() {
        return System.getProperty("keyspace", "hawkulartest");
    }

    protected DateTime hour0() {
        DateTime rightNow = now();
        return rightNow.hourOfDay().roundFloorCopy().minusHours(
                rightNow.hourOfDay().roundFloorCopy().hourOfDay().get());
    }

    protected DateTime hour(int hourOfDay) {
        return hour0().plusHours(hourOfDay);
    }

    protected <V> V getUninterruptibly(ListenableFuture<V> future) throws ExecutionException, TimeoutException {
        return Uninterruptibles.getUninterruptibly(future, FUTURE_TIMEOUT, TimeUnit.SECONDS);
    }

    /**
     * This method take a function that produces an Observable that has side effects, like
     * inserting rows into the database. A {@link TestSubscriber} is subscribed to the
     * Observable. The subscriber blocks up to five seconds waiting for a terminal event
     * from the Observable.
     *
     * @param fn A function that produces an Observable with side effects
     */
    protected void doAction(Supplier<Observable<Void>> fn) {
        TestSubscriber<Void> subscriber = new TestSubscriber<>();
        Observable<Void> observable = fn.get();
        observable.subscribe(subscriber);
        subscriber.awaitTerminalEvent(5, TimeUnit.SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertCompleted();
    }

    /**
     * This method takes a function that produces an Observable. The method blocks up to
     * five seconds until the Observable emits a terminal event. The items that the
     * Observable emits are then returned.
     *
     * @param fn A function that produces an Observable
     * @param <T> The type of items emitted by the Observable
     * @return A list of the items emitted by the Observable
     */
    protected <T> List<T> getOnNextEvents(Supplier<Observable<T>> fn) {
        TestSubscriber<T> subscriber = new TestSubscriber<>();
        Observable<T> observable = fn.get();
        observable.subscribe(subscriber);
        subscriber.awaitTerminalEvent(5, SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertCompleted();

        return subscriber.getOnNextEvents();
    }

    protected double calculateRate(double value, DateTime startTime, DateTime endTime) {
        return (value / (endTime.getMillis() - startTime.getMillis())) * 60000;
    }

    protected void assertIsEmpty(String msg, Observable<ResultSet> observable) {
        List<ResultSet> resultSets = getOnNextEvents(() -> observable);
        assertEquals(resultSets.size(), 1, msg + ": Failed to obtain result set");
        assertTrue(resultSets.get(0).isExhausted(), msg);
    }
}
