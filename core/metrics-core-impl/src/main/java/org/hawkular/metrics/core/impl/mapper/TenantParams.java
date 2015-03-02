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

import java.util.HashMap;
import java.util.Map;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * @author John Sanda
 */
// TODO rename class to better reflect it is used for input and output
@ApiModel
public class TenantParams {

    private String id;

    private Map<String, Integer> retentions = new HashMap<>();

    public TenantParams() {
    }

    public TenantParams(String id, Map<String, Integer> retentions) {
        this.id = id;
        this.retentions = retentions;
    }

    @ApiModelProperty(required = true, value = "Tenant's unique identifier.")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty(required = false, allowableValues = "[numeric, availability]", value = "Retention periods for "
            + "different metric types.")
    public Map<String, Integer> getRetentions() {
        return retentions;
    }

    public void setRetentions(Map<String, Integer> retentions) {
        this.retentions = retentions;
    }
}
