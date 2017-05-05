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

import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;

import org.hawkular.metrics.api.jaxrs.util.MetricRegistryProvider;
import org.hawkular.metrics.core.dropwizard.HawkularMetricRegistry;

import com.codahale.metrics.Timer;

/**
 * Registers metrics for REST endpoints. The metrics here are based on the URI paths with parameters, not the path
 * variables. A client for example might send GET /hawkular/metrics/gauges/MyMetric/raw. The matching metric will be
 * GET gauges/id/raw. The base path of /hawkular/metrics is omitted.
 *
 * @author jsanda
 */
@ApplicationScoped
public class RESTMetrics {

    private Map<RESTMetricName, Timer> timers = new HashMap<>();

    public RESTMetrics() {
        add(RESTMetaData.forWrite(HTTPMethod.POST, "gauges"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id"));
        add(RESTMetaData.forWrite(HTTPMethod.DELETE, "gauges/id"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges/tags/tags"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.PUT, "gauges/id/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.DELETE, "gauges/id/tags/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.POST, "gauges/id/raw"));
        add(RESTMetaData.forWrite(HTTPMethod.POST, "gauges/raw"));
        add(RESTMetaData.forRead(HTTPMethod.POST, "gauges/raw/query"));
        add(RESTMetaData.forRead(HTTPMethod.POST, "gauges/rate/query"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/raw"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/stats"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges/stats"));
        add(RESTMetaData.forRead(HTTPMethod.POST, "gauges/stats/query"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/stats/tags/tags"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/periods"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/rate"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges/id/rate/stats"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "gauges/rate/stats"));

        add(RESTMetaData.forWrite(HTTPMethod.POST, "counters"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "counters"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "counters/id"));
        add(RESTMetaData.forWrite(HTTPMethod.DELETE, "counters/id"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "counters/tags/tags"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.PUT, "counters/id/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.DELETE, "counters/id/tags/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.POST, "counters/raw"));
        add(RESTMetaData.forRead(HTTPMethod.POST, "counters/raw/query"));
        add(RESTMetaData.forRead(HTTPMethod.POST, "counters/rate/query"));
        add(RESTMetaData.forWrite(HTTPMethod.POST, "counters/id/raw"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/raw"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/stats"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/rate"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/rate/stats"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "counters/stats"));
        add(RESTMetaData.forRead(HTTPMethod.POST, "counters/stats/query"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "counters/rate/stats"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "counters/id/stats/tags/tags"));

        add(RESTMetaData.forWrite(HTTPMethod.POST, "metrics"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "metrics"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "metrics/tags"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "metrics/tags/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.POST, "metrics/raw"));
        add(RESTMetaData.forRead(HTTPMethod.POST, "metrics/stats/query"));
        add(RESTMetaData.forRead(HTTPMethod.POST, "metrics/stats/batch/query"));

        // TODO Should we make string and availability metrics optional since they are not used in openshift?

        add(RESTMetaData.forWrite(HTTPMethod.POST, "strings"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "strings"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "strings/id"));
        add(RESTMetaData.forWrite(HTTPMethod.DELETE, "strings/id"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "strings/tags/tags"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "strings/id/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.PUT, "strings/id/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.DELETE, "strings/tags/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.POST, "strings/id/raw"));
        add(RESTMetaData.forWrite(HTTPMethod.POST, "strings/raw"));
        add(RESTMetaData.forRead(HTTPMethod.POST, "strings/raw/query"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "strings/id/raw"));

        add(RESTMetaData.forWrite(HTTPMethod.POST, "availability"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "availability"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "availability/id"));
        add(RESTMetaData.forWrite(HTTPMethod.DELETE, "availability/id"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "availability/tags/tags"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "availability/id/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.PUT, "availability/id/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.DELETE, "availability/id/tags/tags"));
        add(RESTMetaData.forWrite(HTTPMethod.POST, "availability/id/raw"));
        add(RESTMetaData.forWrite(HTTPMethod.POST, "availability/raw"));
        add(RESTMetaData.forRead(HTTPMethod.POST, "availability/raw/query"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "availability/id/raw"));
        add(RESTMetaData.forRead(HTTPMethod.GET, "availability/id/stats"));
    }

    private void add(RESTMetaData metaData) {
        HawkularMetricRegistry metricRegistry = MetricRegistryProvider.INSTANCE.getMetricRegistry();
        Timer timer = metricRegistry.timer(metaData);
        timers.put(metaData.getRESTMetricName(), timer);
    }

    public Map<RESTMetricName, Timer> getTimers() {
        return timers;
    }

}
