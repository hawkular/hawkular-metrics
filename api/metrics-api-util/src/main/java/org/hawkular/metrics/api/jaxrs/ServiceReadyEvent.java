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
package org.hawkular.metrics.api.jaxrs;

import org.hawkular.metrics.model.Metric;

import rx.Observable;

/**
 * A ServiceReadyEvent is fired when the core metrics service is fully initialized. This event class contains an
 * Observable that emits metrics as data points are inserted. The purpose of this class is to provide a way to expose
 * that observable to the metrics bus component in order to publish data points onto the bus for consumption by other
 * hawkular components.
 *
 * @author jsanda
 */
public class ServiceReadyEvent {

    private Observable<Metric<?>> insertedData;

    public ServiceReadyEvent(Observable<Metric<?>> insertedData) {
        this.insertedData = insertedData;
    }

    public Observable<Metric<?>> getInsertedData() {
        return insertedData;
    }
}
