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

package org.hawkular.metrics.alerting;

import static java.util.stream.Collectors.toList;

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.DISABLE_METRICS_FORWARDING;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.DISABLE_PUBLISH_FILTERING;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.METRICS_PUBLISH_PERIOD;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.hawkular.alerts.api.model.data.Data;
import org.hawkular.alerts.api.services.AlertsService;
import org.hawkular.metrics.api.jaxrs.ServiceReady;
import org.hawkular.metrics.api.jaxrs.ServiceReadyEvent;
import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.jboss.logging.Logger;

import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * @author Thomas Segismont
 * @author Jay Shaughnessy
 */
@ApplicationScoped
public class InsertedDataSubscriber {
    private static final Logger log = Logger.getLogger(InsertedDataSubscriber.class);

    private Subscription subscription;

    @Inject
    private AlertsService alertsService;

    @Inject
    private PublishCommandTable publish;

    @Inject
    @Configurable
    @ConfigurationProperty(METRICS_PUBLISH_PERIOD)
    private String publishPeriodProperty;
    private int publishPeriod;

    @Inject
    @Configurable
    @ConfigurationProperty(DISABLE_METRICS_FORWARDING)
    private String disableMetricsForwarding;

    @Inject
    @Configurable
    @ConfigurationProperty(DISABLE_PUBLISH_FILTERING)
    private String disablePublishFiltering;

    public void onMetricsServiceReady(@Observes @ServiceReady ServiceReadyEvent event) {
        if (!Boolean.parseBoolean(disableMetricsForwarding)) {
            publishPeriod = getPublishPeriod();
            Observable<List<Metric<?>>> events;
            if (Boolean.parseBoolean(disablePublishFiltering)) {
                events = event.getInsertedData()
                        .buffer(publishPeriod, TimeUnit.MILLISECONDS, 100)
                        .filter(list -> !list.isEmpty());
            } else {
                events = event.getInsertedData()
                        .filter(m -> m.getType() != MetricType.STRING)
                        .filter(m -> publish.isPublished(m.getMetricId()))
                        .buffer(publishPeriod, TimeUnit.MILLISECONDS, 100)
                        .filter(list -> !list.isEmpty());
            }
            events = events.onBackpressureBuffer()
                    .observeOn(Schedulers.io());
            subscription = events.subscribe(list -> list.forEach(this::onInsertedData));
        }
    }

    private void onInsertedData(Metric<?> metric) {
        if (metric.getMetricId().getType() == AVAILABILITY) {
            @SuppressWarnings("unchecked")
            Metric<AvailabilityType> avail = (Metric<AvailabilityType>) metric;
            publishAvailablility(avail);
        } else {
            @SuppressWarnings("unchecked")
            Metric<? extends Number> numeric = (Metric<? extends Number>) metric;
            publishNumeric(numeric);
        }
    }

    private void publishNumeric(Metric<? extends Number> numeric) {
        MetricId<? extends Number> numericId = numeric.getMetricId();
        String tenantId = numericId.getTenantId();
        String dataId = publish.getPublishingName(numericId);
        List<Data> data = numeric.getDataPoints().stream()
                .map(dataPoint -> Data.forNumeric(tenantId, dataId, dataPoint.getTimestamp(),
                        dataPoint.getValue().doubleValue()))
                .collect(toList());
        try {
            log.tracef("Publish numeric data: %s", data);
            alertsService.sendData(data);
        } catch (Exception e) {
            log.warnf("Failed to send numeric alerting data.", e);
        }
    }

    private void publishAvailablility(Metric<AvailabilityType> avail) {
        MetricId<AvailabilityType> availId = avail.getMetricId();
        String tenantId = availId.getTenantId();
        String dataId = publish.getPublishingName(availId);
        List<Data> data = avail.getDataPoints().stream()
                .map(dataPoint -> Data.forAvailability(tenantId, dataId, dataPoint.getTimestamp(),
                        toAlertingAvail(dataPoint.getValue())))
                .collect(toList());
        try {
            log.tracef("Publish avail data: %s", data);
            alertsService.sendData(data);
        } catch (Exception e) {
            log.warnf("Failed to send availability alerting data.", e);
        }
    }

    private org.hawkular.alerts.api.model.data.AvailabilityType toAlertingAvail(
            AvailabilityType metricAvailType) {
        switch (metricAvailType) {
            case UP:
                return org.hawkular.alerts.api.model.data.AvailabilityType.UP;
            case DOWN:
                return org.hawkular.alerts.api.model.data.AvailabilityType.DOWN;
            default:
                return org.hawkular.alerts.api.model.data.AvailabilityType.UNAVAILABLE;
        }
    }

    @PreDestroy
    void shutdown() {
        if (subscription != null) {
            subscription.unsubscribe();
        }
    }

    private int getPublishPeriod() {
        try {
            return Integer.parseInt(publishPeriodProperty);
        } catch (NumberFormatException e) {
            log.warnf("Invalid publish period. Setting default value %s", METRICS_PUBLISH_PERIOD.defaultValue());
            return Integer.parseInt(METRICS_PUBLISH_PERIOD.defaultValue());
        }
    }
}
