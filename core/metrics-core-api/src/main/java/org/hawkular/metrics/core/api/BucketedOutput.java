/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author John Sanda
 */
public class BucketedOutput<POINT> {

    private String tenantId;
    private String id;
    private Map<String, String> metadata = new HashMap<>();
    private List<POINT> data = new ArrayList<>();

    @SuppressWarnings("unused")
    public BucketedOutput() {
    }

    public BucketedOutput(String tenantId, String id, Map<String, String> metadata) {
        this.tenantId = tenantId;
        this.id = id;
        this.metadata = metadata;
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

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public List<POINT> getData() {
        return data;
    }

    public void setData(List<POINT> data) {
        this.data = data;
    }

    public void add(POINT d) {
        data.add(d);
    }
}
