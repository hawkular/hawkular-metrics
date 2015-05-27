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
package org.hawkular.rx.cassandra.driver;

import java.util.concurrent.Executor;

import com.google.common.util.concurrent.ListenableFuture;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action0;

/**
 * The functions in this class are copied directly from https://github.com/ReactiveX/RxJavaGuava at least for the time
 * being since the artifact(s) are not available in maven central.
 * <br/><br/>
 * This should be an internal, help class only. For now though it is exposed as we transition away from
 * ListenableFuture.
 *
 * @author jsanda
 */
public class RxUtil {

    private RxUtil() {
    }

    /**
     * Converts from {@link ListenableFuture} to {@link rx.Observable}.
     *
     * @param future  the {@link ListenableFuture} to register a listener on.
     * @param scheduler  the {@link Scheduler} where the callback will be executed.  The will be where the
     * {@link Observer#onNext(Object)} call from.
     * @return an {@link Observable} that emits the one value when the future completes.
     */
    public static <T> Observable<T> from(final ListenableFuture<T> future, final Scheduler scheduler) {
        final Scheduler.Worker worker = scheduler.createWorker();
        return from(future, new Executor() {
            @Override
            public void execute(final Runnable command) {
                worker.schedule(new Action0() {
                    @Override
                    public void call() {
                        command.run();
                    }
                });
            }
        });
    }

    /**
     * Converts from {@link ListenableFuture} to {@link rx.Observable}.
     *
     * @param future  the {@link ListenableFuture} to register a listener on.
     * @param executor  the {@link Executor} where the callback will be executed.  The will be where the
     * {@link Observer#onNext(Object)} call from.
     * @return an {@link Observable} that emits the one value when the future completes.
     */
    public static <T> Observable<T> from(final ListenableFuture<T> future, final Executor executor) {
        return Observable.create(new Observable.OnSubscribe<T>() {
            @Override
            public void call(final Subscriber<? super T> subscriber) {
                future.addListener(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            T t = future.get();
                            subscriber.onNext(t);
                            subscriber.onCompleted();
                        } catch (Exception e) {
                            subscriber.onError(e);
                        }
                    }
                }, executor);
            }
        });
    }

}
