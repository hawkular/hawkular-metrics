/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.dropwizard;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.util.List;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.jboss.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Timer;

import rx.Observable;

/**
 * A listener that receives notifications about registered metrics and persists the metric tags. Only metrics having
 * meta data will be persisted. A warning is logged if no meta data is found for a metric.
 *
 * @author jsanda
 */
public class HawkularMetricsRegistryListener extends MetricRegistryListener.Base {

    private static Logger logger = Logger.getLogger(HawkularMetricsRegistryListener.class);

    private MetricsService metricsService;

    private HawkularMetricRegistry metricRegistry;

    private MetricNameService metricNameService;

    public void setMetricsService(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    public void setMetricRegistry(HawkularMetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public void setMetricNameService(MetricNameService metricNameService) {
        this.metricNameService = metricNameService;
    }

    @Override
    public void onMeterAdded(String name, Meter meter) {
        MetaData metaData = metricRegistry.getMetaData(name);
        if (metaData == null) {
            logger.warnf("Did not find meta data for meter %s that has been added to the registry", name);
            return;
        }

        String tenantId = metricNameService.getTenantId();
        String metricName = metricNameService.createMetricName(metaData);

        createMetrics(metricName, asList(
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-5min"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-15min"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-mean"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, COUNTER, metricName), metaData.getTags())
        ));
     }

    @Override
    public void onMeterRemoved(String name) {
        MetaData metaData = metricRegistry.removeMetaData(name);
        if (metaData == null) {
            logger.warnf("Did not find meta data for meter %s that has been removed from the registry", name);
            return;
        }

        String tenantId = metricNameService.getTenantId();
        String metricName = metricNameService.createMetricName(metaData);

        deleteMetrics(metricName, asList(
                new MetricId<>(tenantId, GAUGE, metricName + "-5min"),
                new MetricId<>(tenantId, GAUGE, metricName + "-15min"),
                new MetricId<>(tenantId, GAUGE, metricName + "-mean"),
                new MetricId<>(tenantId, COUNTER, metricName)
        ));
    }

    @Override
    public void onTimerAdded(String name, Timer timer) {
        MetaData metaData = metricRegistry.getMetaData(name);
        if (metaData == null) {
            logger.warnf("Did not find meta data for timer %s that has been added to the registry", name);
            return;
        }

        String tenantId = metricNameService.getTenantId();
        String metricName = metricNameService.createMetricName(metaData);

        createMetrics(metricName, asList(
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-5min"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-15min"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-mean"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, COUNTER, metricName), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-median"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-max"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-min"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-75p"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-95p"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-99p"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-999p"), metaData.getTags())

        ));
    }

    @Override
    public void onTimerRemoved(String name) {
        MetaData metaData = metricRegistry.removeMetaData(name);
        if (metaData == null) {
            logger.warnf("Did not find meta data for timer %s that has been removed from the registry");
            return;
        }

        String tenantId = metricNameService.getTenantId();
        String metricName = metricNameService.createMetricName(metaData);

        deleteMetrics(metricName, asList(
                new MetricId<>(tenantId, GAUGE, metricName + "-5min"),
                new MetricId<>(tenantId, GAUGE, metricName + "-15min"),
                new MetricId<>(tenantId, GAUGE, metricName + "-mean"),
                new MetricId<>(tenantId, COUNTER, metricName),
                new MetricId<>(tenantId, GAUGE, metricName + "-median"),
                new MetricId<>(tenantId, GAUGE, metricName + "-max"),
                new MetricId<>(tenantId, GAUGE, metricName + "-min"),
                new MetricId<>(tenantId, GAUGE, metricName + "-75p"),
                new MetricId<>(tenantId, GAUGE, metricName + "-95p"),
                new MetricId<>(tenantId, GAUGE, metricName + "-99p"),
                new MetricId<>(tenantId, GAUGE, metricName + "-999p")
        ));
    }

    @Override
    public void onHistogramAdded(String name, Histogram histogram) {
        MetaData metaData = metricRegistry.getMetaData(name);
        if (metaData == null) {
            logger.warnf("Did not find meta data for histogram %s that has been added to the registry", name);
            return;
        }

        String tenantId = metricNameService.getTenantId();
        String metricName = metricNameService.createMetricName(metaData);

        createMetrics(metricName, asList(
                new Metric<>(new MetricId<>(tenantId, COUNTER, metricName), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-mean"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-median"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-max"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-min"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-75p"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-95p"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-99p"), metaData.getTags()),
                new Metric<>(new MetricId<>(tenantId, GAUGE, metricName + "-999p"), metaData.getTags())

        ));
    }

    @Override
    public void onHistogramRemoved(String name) {
        MetaData metaData = metricRegistry.removeMetaData(name);
        if (metaData == null) {
            logger.warnf("Did not find meta data for histogram %s that has been removed from the registry");
            return;
        }

        String tenantId = metricNameService.getTenantId();
        String metricName = metricNameService.createMetricName(metaData);

        deleteMetrics(metricName, asList(
                new MetricId<>(tenantId, GAUGE, metricName + "-mean"),
                new MetricId<>(tenantId, COUNTER, metricName),
                new MetricId<>(tenantId, GAUGE, metricName + "-median"),
                new MetricId<>(tenantId, GAUGE, metricName + "-max"),
                new MetricId<>(tenantId, GAUGE, metricName + "-min"),
                new MetricId<>(tenantId, GAUGE, metricName + "-75p"),
                new MetricId<>(tenantId, GAUGE, metricName + "-95p"),
                new MetricId<>(tenantId, GAUGE, metricName + "-99p"),
                new MetricId<>(tenantId, GAUGE, metricName + "-999p")
        ));
    }

    @Override
    public void onGaugeAdded(String name, Gauge<?> gauge) {
        MetaData metaData = metricRegistry.getMetaData(name);
        if (metaData == null) {
            logger.warnf("Did not find meta data for gauge %s that has been added to the registry", name);
            return;
        }

        String tenantId = metricNameService.getTenantId();
        String metricName = metricNameService.createMetricName(metaData);

        createMetrics(metricName, singletonList(new Metric<>(new MetricId<>(tenantId, GAUGE, metricName),
                metaData.getTags())));
    }

    @Override
    public void onGaugeRemoved(String name) {
        MetaData metaData = metricRegistry.removeMetaData(name);
        if (metaData == null) {
            logger.warnf("Did not find meta data for gauge %s that has been removed from the registry");
            return;
        }

        String tenantId = metricNameService.getTenantId();
        String metricName = metricNameService.createMetricName(metaData);

        deleteMetrics(metricName, singletonList(new MetricId<>(tenantId, GAUGE, metricName)));
    }

    @Override
    public void onCounterAdded(String name, Counter counter) {
        MetaData metaData = metricRegistry.getMetaData(name);
        if (metaData == null) {
            logger.warnf("Did not find meta data for counter %s that has been added to the registry", name);
            return;
        }

        String tenantId = metricNameService.getTenantId();
        String metricName = metricNameService.createMetricName(metaData);

        createMetrics(metricName, singletonList(new Metric<>(new MetricId<>(tenantId, COUNTER, metricName),
                metaData.getTags())));
    }

    @Override
    public void onCounterRemoved(String name) {
        MetaData metaData = metricRegistry.removeMetaData(name);
        if (metaData == null) {
            logger.warnf("Did not find meta data for counter %s that has been removed from the registry");
            return;
        }

        String tenantId = metricNameService.getTenantId();
        String metricName = metricNameService.createMetricName(metaData);

        deleteMetrics(metricName, singletonList(new MetricId<>(tenantId, COUNTER, metricName)));
    }

    private void createMetrics(String metricName, List<Metric<?>> metrics) {
        Observable.from(metrics)
                .flatMap(metric -> metricsService.createMetric(metric, true)
                        .doOnNext(aVoid -> logger.debugf("Created %s", metric)))
                .toCompletable()
                .subscribe(
                        () -> logger.debugf("Finished creating metrics for %s", metricName),
                        t -> logger.warnf(t, "Failed to create metrics for %s", metricName)
                );
    }

    private void deleteMetrics(String metricName, List<MetricId<?>> metrics) {
        Observable.from(metrics)
                .flatMap(metricId -> metricsService.deleteMetric(metricId)
                        .doOnNext(aVoid -> logger.debugf("Deleted %s", metricId)))
                .toCompletable()
                .subscribe(
                        () -> logger.debugf("Finished deleting metrics for %s", metricName),
                        t -> logger.warnf(t, "Failed to delete metrics for %s", metricName)
                );
    }

}
