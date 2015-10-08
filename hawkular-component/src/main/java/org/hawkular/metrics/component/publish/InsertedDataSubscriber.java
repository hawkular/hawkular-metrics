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

package org.hawkular.metrics.component.publish;

import static javax.ejb.ConcurrencyManagementType.BEAN;
import static javax.ejb.TransactionAttributeType.NEVER;

import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.TransactionAttribute;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.hawkular.metrics.api.jaxrs.ServiceReady;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricsService;
import org.jboss.logging.Logger;

import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * Subscribes to {@link MetricsService#insertedDataEvents()} and relays metrics to a bus publisher.
 *
 * @author Thomas Segismont
 */
@Startup
@Singleton
@TransactionAttribute(NEVER)
@ConcurrencyManagement(BEAN)
public class InsertedDataSubscriber {
    private static final Logger log = Logger.getLogger(InsertedDataSubscriber.class);

    @Resource
    ManagedExecutorService executor;
    @Inject
    MetricDataPublisher metricDataPublisher;
    @Inject
    AvailDataPublisher availDataPublisher;


    private Scheduler scheduler;
    private Subscription subscription;

    @PostConstruct
    void init() {
        scheduler = Schedulers.from(executor);
    }

    @SuppressWarnings("unused")
    public void onMetricsServiceReady(@Observes @ServiceReady MetricsService metricsService) {
        Observable<Metric<?>> events = metricsService.insertedDataEvents().observeOn(scheduler);
        // We may want to buffer events in the future for better performance
        // events.buffer(50, TimeUnit.MILLISECONDS, 10, scheduler);
        subscription = events.subscribe(this::onInsertedData);
    }

    private void onInsertedData(Metric<?> metric) {
        log.tracef("Inserted metric: %s", metric);
        if (metric.getId().getType() == AVAILABILITY) {
            @SuppressWarnings("unchecked")
            Metric<AvailabilityType> avail = (Metric<AvailabilityType>) metric;
            availDataPublisher.publish(avail);
        } else {
            @SuppressWarnings("unchecked")
            Metric<? extends Number> numeric = (Metric<? extends Number>) metric;
            metricDataPublisher.publish(numeric);
        }
    }

    @PreDestroy
    void shutdown() {
        if (subscription != null) {
            subscription.unsubscribe();
        }
    }
}
