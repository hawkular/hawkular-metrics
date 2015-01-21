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
package org.rhq.metrics.restServlet;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class NumericDataParams extends MetricDataParams {

    private Long timestamp;

    private Double value;

    private List<NumericDataPoint> data = new ArrayList<>();

    public NumericDataParams() {
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public List<NumericDataPoint> getData() {
        return data;
    }

    public void setData(List<NumericDataPoint> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("tenantId", tenantId)
            .add("name", name)
            .add("metadata", metadata)
            .add("timestamp", timestamp)
            .add("value", value)
            .add("data", data)
            .toString();
    }
}
