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
package org.hawkular.metrics.api.jaxrs.dropwizard;

import javax.enterprise.context.ApplicationScoped;

import org.hawkular.metrics.api.jaxrs.util.MetricRegistryProvider;
import org.hawkular.metrics.core.dropwizard.HawkularMetricRegistry;
import org.hawkular.metrics.core.dropwizard.MetricNameService;

import com.codahale.metrics.Timer;

/**
 * Registers metric meta data for REST endpoints. The actual metrics are registered when the end points are invoked for
 * the first time. The metrics here are based on the URI paths with parameters, not the path variables. A client for
 * example might send GET /hawkular/metrics/gauges/MyMetric/raw. The matching metric will be
 * GET gauges/id/raw. The base path of /hawkular/metrics is omitted.
 *
 * @author jsanda
 */
@ApplicationScoped
public class RESTMetrics {

    private MetricNameService metricNameService;

    public void setMetricNameService(MetricNameService metricNameService) {
        this.metricNameService = metricNameService;
    }

    public void initMetrics() {
        String hostname = metricNameService.getHostName();

        register(RESTMetaData.forRead(HTTPMethod.GET, "status", hostname));

        register(RESTMetaData.forWrite(HTTPMethod.POST, "gauges", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.DELETE, "gauges/id", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges/tags/tags", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.PUT, "gauges/id/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.DELETE, "gauges/id/tags/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.POST, "gauges/id/raw", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.POST, "gauges/raw", hostname));
        register(RESTMetaData.forRead(HTTPMethod.POST, "gauges/raw/query", hostname));
        register(RESTMetaData.forRead(HTTPMethod.POST, "gauges/rate/query", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/raw", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/stats", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges/stats", hostname));
        register(RESTMetaData.forRead(HTTPMethod.POST, "gauges/stats/query", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/stats/tags/tags", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/periods", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/rate", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/rate/stats", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "gauges/rate/stats", hostname));

        register(RESTMetaData.forWrite(HTTPMethod.POST, "counters", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "counters", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "counters/id", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.DELETE, "counters/id", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "counters/tags/tags", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.PUT, "counters/id/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.DELETE, "counters/id/tags/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.POST, "counters/raw", hostname));
        register(RESTMetaData.forRead(HTTPMethod.POST, "counters/raw/query", hostname));
        register(RESTMetaData.forRead(HTTPMethod.POST, "counters/rate/query", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.POST, "counters/id/raw", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/raw", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/stats", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/rate", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/rate/stats", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "counters/stats", hostname));
        register(RESTMetaData.forRead(HTTPMethod.POST, "counters/stats/query", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "counters/rate/stats", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/stats/tags/tags", hostname));

        register(RESTMetaData.forWrite(HTTPMethod.POST, "metrics", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "metrics", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "metrics/tags", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "metrics/tags/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.POST, "metrics/raw", hostname));
        register(RESTMetaData.forRead(HTTPMethod.POST, "metrics/stats/query", hostname));
        register(RESTMetaData.forRead(HTTPMethod.POST, "metrics/stats/batch/query", hostname));

        register(RESTMetaData.forWrite(HTTPMethod.POST, "strings", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "strings", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "strings/id", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.DELETE, "strings/id", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "strings/tags/tags", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "strings/id/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.PUT, "strings/id/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.DELETE, "strings/tags/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.POST, "strings/id/raw", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.POST, "strings/raw", hostname));
        register(RESTMetaData.forRead(HTTPMethod.POST, "strings/raw/query", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "strings/id/raw", hostname));

        register(RESTMetaData.forWrite(HTTPMethod.POST, "availability", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "availability", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "availability/id", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.DELETE, "availability/id", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "availability/tags/tags", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "availability/id/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.PUT, "availability/id/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.DELETE, "availability/id/tags/tags", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.POST, "availability/id/raw", hostname));
        register(RESTMetaData.forWrite(HTTPMethod.POST, "availability/raw", hostname));
        register(RESTMetaData.forRead(HTTPMethod.POST, "availability/raw/query", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "availability/id/raw", hostname));
        register(RESTMetaData.forRead(HTTPMethod.GET, "availability/id/stats", hostname));
    }

    private void register(RESTMetaData metaData) {
        HawkularMetricRegistry registry = MetricRegistryProvider.INSTANCE.getMetricRegistry();
        registry.registerMetaData(metaData);
    }

    public Timer getTimer(String metricName) {
        return MetricRegistryProvider.INSTANCE.getMetricRegistry().timer(metricName);
    }

}
