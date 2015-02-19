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
package org.hawkular.metrics.api.jaxrs;

import java.util.HashMap;
import java.util.Map;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * @author John Sanda
 */
@ApiModel(description = "Metric definition model")
public class MetricParams {

    private String name;

    private Map<String, String> tags = new HashMap<>();

    private Integer dataRetention;

    public String getName() {
        return name;
    }

    @ApiModelProperty(value = "Identifier of the metric, mandatory.")
    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @ApiModelProperty(value = "Arbitary key/value definitions for this metric")
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    @ApiModelProperty(value = "Overrides the data retention setting at the tenant level")
    public Integer getDataRetention() {
        return dataRetention;
    }

    public void setDataRetention(Integer dataRetention) {
        this.dataRetention = dataRetention;
    }
}
