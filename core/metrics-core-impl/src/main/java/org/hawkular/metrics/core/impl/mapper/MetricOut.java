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
package org.hawkular.metrics.core.impl.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.wordnik.swagger.annotations.ApiModel;


/**
 * @author John Sanda
 */
@ApiModel
public class MetricOut {

    private String tenantId;

    private String name;

    @JsonInclude(Include.NON_EMPTY)
    private Map<String, String> tags = new HashMap<>();

    @JsonInclude(Include.NON_EMPTY)
    private List<DataPointOut> data = new ArrayList<>();

    @JsonInclude(Include.NON_EMPTY)
    private Integer dataRetention;

    public MetricOut() {
    }

    public MetricOut(String tenantId, String name, Map<String, String> tags) {
        this.tenantId = tenantId;
        this.name = name;
        this.tags = tags;
    }

    public MetricOut(String tenantId, String name, Map<String, String> tags, Integer dataRetention) {
        this.tenantId = tenantId;
        this.name = name;
        this.tags = tags;
        this.dataRetention = dataRetention;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public List<DataPointOut> getData() {
        return data;
    }

    public void setData(List<DataPointOut> data) {
        this.data = data;
    }

    public Integer getDataRetention() {
        return dataRetention;
    }

    public void setDataRetention(Integer dataRetention) {
        this.dataRetention = dataRetention;
    }
}
