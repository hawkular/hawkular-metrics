/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.impl.SchedulerImpl;
import org.hawkular.rx.cassandra.driver.RxSession;

/**
 * This class is essentially a test hook. For REST API integration tests it is replaced with a version that returns
 * a {@link org.hawkular.metrics.scheduler.impl.TestScheduler TestScheduler}.
 *
 * @author jsanda
 */
public class JobSchedulerFactory {

    private String metricsHostname;

    public Scheduler getJobScheduler(RxSession session) {
        try {
            if (metricsHostname == null) {
                metricsHostname = InetAddress.getLocalHost().getHostName();
            }
            return new SchedulerImpl(session, metricsHostname);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

}
