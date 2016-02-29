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

import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import rx.Observable;
import rx.Observable.Transformer;
import rx.Producer;
import rx.Scheduler;
import rx.Scheduler.Worker;
import rx.Subscriber;
import rx.internal.operators.BackpressureUtils;
import rx.schedulers.Schedulers;

/**
 * Transforms an {@link Observable} of {@link ResultSet} into an {@link Observable} of {@link Row}.
 *
 * Use this transformer together with {@link Observable#compose(Transformer)} instead of:
 * <pre>
 * Observable&lt;ResultSet&gt; resultSetObservable = ...;
 * Observable&lt;Row&gt; rowObservable = resultSetObservable.flatMap(resultSet -> {
 *   // ResultSet is an Iterable&lt;Row&gt;
 *   return Observable.from(resultSet);
 * })
 * </pre>
 * The code above may <strong>block</strong> the operating thread if the {@link ResultSet} is not fully fetched.
 *
 * @author Thomas Segismont
 */
public class ResultSetToRowsTransformer implements Transformer<ResultSet, Row> {
    private final Scheduler scheduler;

    /**
     * Creates a new transformer operating on the {@link Schedulers#computation()} scheduler.
     */
    public ResultSetToRowsTransformer() {
        this(Schedulers.computation());
    }

    /**
     * Creates a new transformer operating on the specified scheduler.
     *
     * @param scheduler the scheduler on which this transformer must operate
     */
    public ResultSetToRowsTransformer(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public Observable<Row> call(Observable<ResultSet> resultSetObservable) {
        return resultSetObservable.flatMap(resultSet -> {
            return Observable.create(subscriber -> {
                subscriber.setProducer(new RowProducer(resultSet, subscriber, scheduler));
            });
        });
    }

    private static class RowProducer implements Producer {
        final ResultSet resultSet;
        final Subscriber<? super Row> subscriber;
        final Scheduler scheduler;
        final AtomicLong requested = new AtomicLong();

        RowProducer(ResultSet resultSet, Subscriber<? super Row> subscriber, Scheduler scheduler) {
            this.resultSet = resultSet;
            this.subscriber = subscriber;
            this.scheduler = scheduler;
        }

        @Override
        public void request(long n) {
            if (n < 0) {
                throw new IllegalArgumentException();
            }
            if (n == 0) {
                return;
            }
            if (BackpressureUtils.getAndAddRequest(requested, n) != 0) {
                return;
            }
            execute(this::produce);
        }

        void produce() {
            long r = requested.get();
            long a = resultSet.getAvailableWithoutFetching();
            for (; ; ) {
                long e = Math.min(r, a);
                for (long i = 0; i < e; i++) {
                    subscriber.onNext(resultSet.one());
                    if (subscriber.isUnsubscribed()) {
                        return;
                    }
                }

                r = requested.addAndGet(-e);
                if (r == 0) {
                    return;
                }

                a = resultSet.getAvailableWithoutFetching();
                if (a == 0) {
                    break;
                }
            }

            if (resultSet.isFullyFetched()) {
                subscriber.onCompleted();
                return;
            }

            Futures.addCallback(resultSet.fetchMoreResults(), new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(ResultSet result) {
                    if (!subscriber.isUnsubscribed()) {
                        produce();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    if (!subscriber.isUnsubscribed()) {
                        subscriber.onError(t);
                    }
                }
            }, this::execute);
        }

        void execute(Runnable command) {
            Worker worker = scheduler.createWorker();
            worker.schedule(() -> {
                try {
                    command.run();
                } finally {
                    worker.unsubscribe();
                }
            });
        }
    }
}
