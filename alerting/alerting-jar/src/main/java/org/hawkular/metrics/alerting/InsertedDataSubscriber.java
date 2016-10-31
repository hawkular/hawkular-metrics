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
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.METRICS_PUBLISH_BUFFER_SIZE;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.METRICS_PUBLISH_PERIOD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.hawkular.alerts.api.model.data.Data;
import org.hawkular.alerts.api.services.AlertsService;
import org.hawkular.alerts.filter.CacheClient;
import org.hawkular.alerts.filter.CacheKey;
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

    // Hmetrics MetricId uniqueness is tenantId+metricType+name.  Halerting Data uniqueness is tenantId+dataId. As
    // such, we have introduced this prefix convention when performing alerting on metrics.  The alerting dataId
    // includes a prefix indicating its metric type.  So, prior to sending metric data to alerting, we must apply
    // the proper prefix to the metricId name, this giving us the expected dataId.
    static final Map<MetricType<?>, String> prefixMap = new HashMap<>();
    static {
        prefixMap.put(MetricType.AVAILABILITY, "hm_a_");
        prefixMap.put(MetricType.COUNTER, "hm_c_");
        prefixMap.put(MetricType.COUNTER_RATE, "hm_cr_");
        prefixMap.put(MetricType.GAUGE, "hm_g_");
        prefixMap.put(MetricType.GAUGE_RATE, "hm_gr_");
        prefixMap.put(MetricType.STRING, "hm_s_");
    }

    private Subscription subscription;

    @Inject
    private AlertsService alertsService;

    @Inject
    private CacheClient dataIdCache;

    @Inject
    @Configurable
    @ConfigurationProperty(METRICS_PUBLISH_PERIOD)
    private String publishPeriodProperty;
    private int publishPeriod;

    @Inject
    @Configurable
    @ConfigurationProperty(METRICS_PUBLISH_BUFFER_SIZE)
    private String publishBufferSizeProperty;
    private int publishBufferSize;

    @Inject
    @Configurable
    @ConfigurationProperty(DISABLE_METRICS_FORWARDING)
    private String disableMetricsForwardingProperty;

    @Inject
    @Configurable
    @ConfigurationProperty(DISABLE_PUBLISH_FILTERING)
    private String disablePublishFilteringProperty;
    private boolean disablePublishFiltering;

    public void onMetricsServiceReady(@Observes @ServiceReady ServiceReadyEvent event) {
        if (!Boolean.parseBoolean(disableMetricsForwardingProperty)) {
            publishPeriod = getPublishPeriod();
            publishBufferSize = getPublishBufferSize();
            disablePublishFiltering = Boolean.parseBoolean(disablePublishFilteringProperty);
            Observable<List<Metric<?>>> events;
            events = event.getInsertedData()
                    .buffer(publishPeriod, TimeUnit.MILLISECONDS, publishBufferSize)
                    .filter(list -> !list.isEmpty());
            events = events.onBackpressureBuffer()
                    .observeOn(Schedulers.io());
            subscription = events.subscribe(list -> onInsertedData(list));
        }
    }

    private void onInsertedData(List<Metric<?>> metrics) {
        List<Data> dataToSend = new ArrayList<>(metrics.size());
        CacheKey reusableKey = new CacheKey("", ""); // avoid generating a bunch of throw-away key objects
        metrics.stream().forEach(m -> addToData(m, dataToSend, reusableKey));
        if (!dataToSend.isEmpty()) {
            try {
                alertsService.sendData(dataToSend, true);

            } catch (Exception e) {
                log.warnf("Failed to send alerting data.", e);
            }
        }
    }

    private boolean containsKey(String tenantId, String dataId, CacheKey reusableKey) {
        reusableKey.setTenantId(tenantId);
        reusableKey.setDataId(dataId);
        return dataIdCache.containsKey(reusableKey);
    }

    @SuppressWarnings("unchecked")
    private void addToData(Metric<?> metric, List<Data> dataToSend, CacheKey reusableKey) {
        MetricId<?> metricId = metric.getMetricId();
        MetricType<?> metricType = metricId.getType();
        if (metricType == MetricType.UNDEFINED) {
            return;
        }

        String tenantId = metricId.getTenantId();
        String dataId = prefixMap.get(metricType) + metricId.getName();
        if (!disablePublishFiltering && !containsKey(tenantId, dataId, reusableKey)) {
            return;
        }

        if (metricType == MetricType.AVAILABILITY) {
            Metric<AvailabilityType> avail = (Metric<AvailabilityType>) metric;
            List<Data> availData = avail.getDataPoints().stream()
                    .map(dataPoint -> Data.forAvailability(tenantId, dataId, dataPoint.getTimestamp(),
                            toAlertingAvail(dataPoint.getValue())))
                    .collect(toList());
            dataToSend.addAll(availData);

        } else if (metricType == MetricType.STRING) {
            Metric<String> string = (Metric<String>) metric;
            List<Data> stringData = string.getDataPoints().stream()
                    .map(dataPoint -> Data.forString(tenantId, dataId, dataPoint.getTimestamp(), dataPoint.getValue()))
                    .collect(toList());
            dataToSend.addAll(stringData);

        } else {
            Metric<? extends Number> numeric = (Metric<? extends Number>) metric;
            List<Data> numericData = numeric.getDataPoints().stream()
                    .map(dataPoint -> Data.forNumeric(tenantId, dataId, dataPoint.getTimestamp(),
                            dataPoint.getValue().doubleValue()))
                    .collect(toList());
            dataToSend.addAll(numericData);
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

    private int getPublishBufferSize() {
        try {
            return Integer.parseInt(publishBufferSizeProperty);
        } catch (NumberFormatException e) {
            log.warnf("Invalid publish buffer size. Setting default value %s",
                    METRICS_PUBLISH_BUFFER_SIZE.defaultValue());
            return Integer.parseInt(METRICS_PUBLISH_BUFFER_SIZE.defaultValue());
        }
    }

}