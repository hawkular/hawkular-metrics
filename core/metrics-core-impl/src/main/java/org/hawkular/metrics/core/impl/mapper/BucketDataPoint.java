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
 * A point in time with some data for min/avg/max to express
 * that at this point in time multiple values were recorded.
 * @author Heiko W. Rupp
 */
@ApiModel(value = "A bucket is a time range with multiple data items represented by min/avg/max values" +
    "for that time span.")
@XmlRootElement
public class BucketDataPoint extends IdDataPoint {

    private double min;
    private double max;
    private double avg;

    public BucketDataPoint() {
    }

    public BucketDataPoint(String id, long timestamp, double min, double avg, double max) {
        super();
        this.setId(id);
        this.setTimestamp(timestamp);
        this.min = min;
        this.max = max;
        this.avg = avg;
    }

    @ApiModelProperty(value = "Minimum value during the time span of the bucket.")
    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    @ApiModelProperty(value = "Maximum value during the time span of the bucket.")
    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @ApiModelProperty(value = "Average value during the time span of the bucket.")
    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public boolean isEmpty() {
        return Double.isNaN(avg) || Double.isNaN(max) || Double.isNaN(min);
    }

    @Override
    public String toString() {
        return "BucketDataPoint{" +
            "min=" + min +
            ", max=" + max +
            ", avg=" + avg +
            '}';
    }
}
