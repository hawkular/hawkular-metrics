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

import java.util.HashSet;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import org.hawkular.metrics.api.jaxrs.util.Eager;

/**
 * A table to store a cache of published metrics on the bus under hawkular-services context.
 * This is a PoC and this cache is only valid for single node sceanarios.
 * Clustered scenarios requires to transform the local HashMap into a ISPN cache.
 *
 * @author Lucas Ponce
 */
@ApplicationScoped
@Eager
public class PublishCommandTable {

    private HashSet<PublishKey> published = new HashSet<>();

    public boolean isPublished(String tenantId, String id) {
        return published.contains(new PublishKey(tenantId, id));
    }

    public synchronized void add(String tenantId, List<String> ids) {
        if (tenantId != null && ids != null) {
            ids.stream().forEach(id -> published.add(new PublishKey(tenantId, id)));
        }
    }

    public synchronized void remove(String tenantId, List<String> ids) {
        if (tenantId != null && ids != null) {
            ids.stream().forEach(id -> published.remove(new PublishKey(tenantId, id)));
        }
    }

    private class PublishKey {
        private String tenantId;
        private String id;

        public PublishKey(String tenantId, String id) {
            this.tenantId = tenantId;
            this.id = id;
        }

        public String getTenantId() {
            return tenantId;
        }

        public void setTenantId(String tenantId) {
            this.tenantId = tenantId;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PublishKey that = (PublishKey) o;

            if (tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null) return false;
            return id != null ? id.equals(that.id) : that.id == null;

        }

        @Override
        public int hashCode() {
            int result = tenantId != null ? tenantId.hashCode() : 0;
            result = 31 * result + (id != null ? id.hashCode() : 0);
            return result;
        }
    }
}
