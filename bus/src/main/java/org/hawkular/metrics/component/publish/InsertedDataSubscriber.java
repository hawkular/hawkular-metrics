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

package org.hawkular.metrics.component.publish;

import static java.util.stream.Collectors.toList;

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.METRICS_PUBLISH_PERIOD;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.jms.Topic;

import org.hawkular.bus.Bus;
import org.hawkular.bus.common.BasicMessage;
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
 */
@ApplicationScoped
public class InsertedDataSubscriber {
    private static final Logger log = Logger.getLogger(InsertedDataSubscriber.class);

    @Resource(mappedName = "java:/topic/HawkularMetricData")
    private Topic numericTopic;

    @Resource(mappedName = "java:/topic/HawkularAvailData")
    private Topic availabilityTopic;

    @Inject
    private Bus bus;

    private Subscription subscription;

    @Inject
    private PublishCommandTable publish;

    @Inject
    @Configurable
    @ConfigurationProperty(METRICS_PUBLISH_PERIOD)
    private String publishPeriodProperty;
    private int publishPeriod;

    public void onMetricsServiceReady(@Observes @ServiceReady ServiceReadyEvent event) {
        publishPeriod = getPublishPeriod();
        Observable<List<Metric<?>>> events = event.getInsertedData()
                .filter(m -> m.getType() != MetricType.STRING)
                .filter(m -> publish.isPublished(m.getMetricId()))
                .buffer(publishPeriod, TimeUnit.MILLISECONDS, 100)
                .filter(list -> !list.isEmpty())
                .onBackpressureBuffer()
                .observeOn(Schedulers.io());
        subscription = events.subscribe(list -> list.forEach(this::onInsertedData));
    }

    private void onInsertedData(Metric<?> metric) {
        log.tracef("Inserted metric: %s", metric);
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

    private void publishNumeric(Metric<? extends Number> metric) {
        BasicMessage message = createNumericMessage(metric);
        bus.send(numericTopic, message).subscribe(
                msg -> log.tracef("Sent message %s", msg),
                t -> log.warnf(t, "Failed to send message %s", message)
        );
    }

    private BasicMessage createNumericMessage(Metric<? extends Number> numeric) {
        MetricId<?> numericId = numeric.getMetricId();
        List<MetricDataMessage.SingleMetric> numericList = numeric.getDataPoints().stream()
                .map(dataPoint -> new MetricDataMessage.SingleMetric(numericId.getType().getText(), numericId.getName(),
                        dataPoint.getTimestamp(), dataPoint.getValue().doubleValue()))
                .collect(toList());
        MetricDataMessage.MetricData metricData = new MetricDataMessage.MetricData();
        metricData.setTenantId(numericId.getTenantId());
        metricData.setData(numericList);
        return new MetricDataMessage(metricData);

    }

    private void publishAvailablility(Metric<AvailabilityType> metric) {
        BasicMessage message = createAvailMessage(metric);
        bus.send(availabilityTopic, message).subscribe(
                msg -> log.tracef("Sent message %s", msg),
                t -> log.warnf(t, "Failed to send message %s", message)
        );
    }

    private BasicMessage createAvailMessage(Metric<AvailabilityType> avail) {
        MetricId<AvailabilityType> availId = avail.getMetricId();
        List<AvailDataMessage.SingleAvail> availList = avail.getDataPoints().stream()
                .map(dataPoint -> new AvailDataMessage.SingleAvail(availId.getTenantId(), availId.getName(),
                        dataPoint.getTimestamp(),
                        dataPoint.getValue().getText().toUpperCase()))
                .collect(toList());
        AvailDataMessage.AvailData metricData = new AvailDataMessage.AvailData();
        metricData.setData(availList);
        return new AvailDataMessage(metricData);
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
