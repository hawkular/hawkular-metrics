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

import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * A data point with an Id
 * @author Heiko W. Rupp
 */
@ApiModel(value = "One data point for a metric with id, timestamp and value. Inherits from DataPoint.")
@XmlRootElement
public class IdDataPoint extends DataPoint {

    private String id;

    public IdDataPoint() {
    }

    public IdDataPoint(long timestamp, double value, String id) {
        super(timestamp, value);
        this.id = id;
    }

    @ApiModelProperty(value = "Id of the metric")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
