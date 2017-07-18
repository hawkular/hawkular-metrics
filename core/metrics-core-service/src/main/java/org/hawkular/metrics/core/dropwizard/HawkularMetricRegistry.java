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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

/**
 * A customer metric registry that stores {@link MetaData} for metrics. Note that you must register meta data
 * for a metric in order for it to get persisted.
 *
 * @author jsanda
 */
public class HawkularMetricRegistry extends MetricRegistry {

    private Map<String, MetaData> metaDataMap = new ConcurrentHashMap<>();

    private MetricNameService metricNameService;

    public void setMetricNameService(MetricNameService metricNameService) {
        this.metricNameService = metricNameService;
    }

    /**
     * Registering meta data does not create a metric. The reason there are two distinct steps for registering meta
     * data and for registering a metric is to allow metrics to be registered lazily. Registering a metric results in
     * metric tags and data points getting persisted whereas the meta data is just stored in memory. This two step
     * approach allows us to persist internal metrics that are actually being used.
     */
    public void registerMetaData(String name, String scope, String type) {
        metaDataMap.put(name, new MetaData(name, scope, type, metricNameService.getHostName()));
    }

    /**
     * @see #registerMetaData(String, String, String)
     */
    public void registerMetaData(MetaData metaData) {
        metaDataMap.put(metaData.getName(), metaData);
    }

    /**
     * Registers the metric along with its meta data.
     */
    public <T extends Metric> T register(String name, String scope, String type, T metric) {
        metaDataMap.put(name, new MetaData(name, scope, type, metricNameService.getHostName()));
        return super.register(name, metric);
    }

    public MetaData getMetaData(String metric) {
        return metaDataMap.get(metric);
    }

    public MetaData removeMetaData(String metric) {
        return metaDataMap.remove(metric);
    }

}
