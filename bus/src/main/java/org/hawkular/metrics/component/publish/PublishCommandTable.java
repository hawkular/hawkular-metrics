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

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;

import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.model.MetricId;
import org.infinispan.Cache;
import org.jboss.logging.Logger;

/**
 * A table to store a cache of published metrics on the bus under hawkular-services context.
 * Implemented as a shared ISPN cache.
 *
 * @author Lucas Ponce
 */
@ApplicationScoped
@Eager
public class PublishCommandTable {
    private static final Logger LOG = Logger.getLogger(PublishCommandTable.class);

    @Resource(lookup = "java:jboss/infinispan/cache/hawkular-metrics/publish")
    private Cache publishCache;

    public boolean isPublished(MetricId id) {
        if (id == null) {
            return false;
        }
        boolean isPublished = publishCache.containsKey(convert(id));
        LOG.debugf("isPublished( %s ) = %s Publish Cache size: %s", id, isPublished, publishCache.size());
        return isPublished;
    }

    /*
        This add() method is added as a helper for PublishDataPointsTest scenarios.
        Entries on PublishCommandTable.publishCache are handled externally.
     */
    public synchronized void add(List<MetricId> ids) {
        if (ids != null) {
            publishCache.putAll(ids.stream().collect(Collectors.toMap(id -> convert(id), id -> convert(id))));
        }
    }

    private String convert(MetricId id) {
        return new StringBuilder(id.getTenantId() == null ? "" : id.getTenantId()).append("-")
                .append((id.getType() == null ? "" : id.getType().getText())).append("-")
                .append((id.getName() == null ? "" : id.getName())).toString();
    }
}
