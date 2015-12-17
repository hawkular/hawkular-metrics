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
package org.hawkular.metrics.core.service;

import rx.Subscriber;

/**
 * A subscriber that is intended for use with an Observable that does not emit any items. A common use case for this
 * is inserting data into the database. When inserting rows into Cassandra for example, the driver returns a result set
 * which has no rows. In general we are only interested in whether or not the query completed successfully. Upon
 * completion of the insert request, {@link #onCompleted()} will be invoked. If there is an time out or other error
 * performing the operation, then {@link #onError(Throwable)} is invoked.
 *
 * @author jsanda
 */
public class VoidSubscriber<T> extends Subscriber<T> {

    private Subscriber<? super Void> subscriber;

    /**
     * @param subscriber The original Subscriber to which calls are delegated.
     */
    public VoidSubscriber(Subscriber<? super Void> subscriber) {
        this.subscriber = subscriber;
    }

    @Override
    public void onCompleted() {
        subscriber.onCompleted();
    }

    @Override
    public void onError(Throwable e) {
        subscriber.onError(e);
    }

    @Override
    public void onNext(T t) {
        // Only the onComplete event is interesting
    }
}
