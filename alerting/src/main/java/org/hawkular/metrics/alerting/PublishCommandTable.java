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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;

import org.hawkular.alerts.api.model.data.CacheKey;
import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.infinispan.Cache;

/**
 * A table to store a cache of published metrics on the bus under hawkular-services context.
 * Implemented as a shared ISPN cache.
 *
 * @author Lucas Ponce
 */
@ApplicationScoped
@Eager
public class PublishCommandTable {

    private static final String AVAILABILITY = "hm_a_";
    private static final String COUNTER = "hm_c_";
    private static final String COUNTER_RATE = "hm_cr_";
    private static final String GAUGE = "hm_g_";
    private static final String GAUGE_RATE = "hm_gr_";
    private static final String STRING = "hm_s_";
    private static final String UNDEFINED = "hm_u_";

    // This just prevents us from having to create a new object on every cache lookup...
    // Note, not thread safe, lookups must be atomic
    Map<MetricType<?>, CacheKey> map = new HashMap<>();

    @Resource(lookup = "java:jboss/infinispan/cache/hawkular-alerts/publish")
    private Cache<CacheKey, String> publishCache;

    public boolean isPublished(MetricId<?> id) {
        return publishCache.containsKey(getCacheKey(id));
    }

    private CacheKey getCacheKey(MetricId<?> id) {
        CacheKey cacheKey = map.get(id.getType());
        if (null == cacheKey) {
            cacheKey = cacheKeyFrom(id);
            map.put(id.getType(), cacheKey);
        }
        cacheKey.setTenantId(id.getTenantId());
        cacheKey.setDataIdSuffix(id.getName());
        return cacheKey;
    }

    private CacheKey cacheKeyFrom(MetricId<?> id) {
        return new CacheKey(id.getTenantId(), getPrefix(id.getType()), id.getName());
    }

    private String getPrefix(MetricType<?> metricType) {
        if (MetricType.AVAILABILITY == metricType) {
            return AVAILABILITY;
        }
        if (MetricType.COUNTER == metricType) {
            return COUNTER;
        }
        if (MetricType.COUNTER_RATE == metricType) {
            return COUNTER_RATE;
        }
        if (MetricType.GAUGE == metricType) {
            return GAUGE;
        }
        if (MetricType.GAUGE_RATE == metricType) {
            return GAUGE_RATE;
        }
        if (MetricType.STRING == metricType) {
            return STRING;
        }
        if (MetricType.UNDEFINED == metricType) {
            return UNDEFINED;
        }
        throw new IllegalArgumentException("Unhandled MetricType: " + metricType.getText());
    }

    public String getPublishingName(MetricId<?> id) {
        String name = publishCache.get(getCacheKey(id));
        return null != name ? name : id.getName();
    }

    /*
        This add() method is added as a helper for PublishDataPointsTest scenarios.
        Entries on PublishCommandTable.publishCache are handled externally.
     */
    public synchronized void add(List<MetricId<?>> ids) {
        if (ids != null) {
            publishCache.putAll(ids.stream().collect(Collectors.toMap(i -> cacheKeyFrom(i), MetricId::getName)));
        }
    }
}
