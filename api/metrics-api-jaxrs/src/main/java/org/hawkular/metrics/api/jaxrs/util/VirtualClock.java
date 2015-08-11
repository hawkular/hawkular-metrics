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
package org.hawkular.metrics.api.jaxrs.util;

import rx.schedulers.TestScheduler;

import java.util.concurrent.TimeUnit;

/**
 * @author jsanda
 */
public class VirtualClock {

    private TestScheduler scheduler;

    public VirtualClock() {
    }

    public VirtualClock(TestScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public long now() {
        return scheduler.now();
    }

    public void advanceTimeTo(long time) {
        scheduler.advanceTimeTo(time, TimeUnit.MILLISECONDS);
    }

    public void advanceTimeBy(long duration, TimeUnit timeUnit) {
        scheduler.advanceTimeBy(duration, timeUnit);
    }

}
