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

import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * A customer DropWizard metric registry that provides support for creating and registering metrics with
 * {@link MetaData}.
 *
 * @author jsanda
 */
public class HawkularMetricRegistry extends MetricRegistry {

    private Map<String, MetaData> metaDataMap = new HashMap<>();

    private MetricNameService metricNameService;

    public void setMetricNameService(MetricNameService metricNameService) {
        this.metricNameService = metricNameService;
    }

    public Meter meter(String name, String scope, String type) {
        metaDataMap.put(name, new MetaData(name, scope, type, metricNameService.getHostName()));
        Meter meter = meter(name);
        return meter;
    }

    public Meter meter(MetaData metaData) {
        metaDataMap.put(metaData.getName(), metaData);
        Meter meter = meter(metaData.getName());
        return meter;
    }

    public Timer timer(String name, String scope, String type) {
        metaDataMap.put(name, new MetaData(name, scope, type, metricNameService.getHostName()));
        Timer timer = timer(name);
        return timer;
    }

    public Timer timer(MetaData metaData) {
        metaDataMap.put(metaData.getName(), metaData);
        Timer timer = timer(metaData.getName());
        return timer;
    }

    public <T extends Metric> T register(String name, String scope, String type, T metric) {
        metaDataMap.put(name, new MetaData(name, scope, type, metricNameService.getHostName()));
        T registered = register(name, metric);
        return registered;
    }

    @Override
    public boolean remove(String name) {
        boolean removed = super.remove(name);
        if (removed) {
            metaDataMap.remove(name);
        }
        return removed;
    }

    public MetaData getMetaData(String metric) {
        return metaDataMap.get(metric);
    }
}
