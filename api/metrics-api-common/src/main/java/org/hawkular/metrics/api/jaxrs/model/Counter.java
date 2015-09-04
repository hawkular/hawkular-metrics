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
package org.hawkular.metrics.api.jaxrs.model;

import static java.util.Collections.unmodifiableList;

import java.util.List;
import java.util.Objects;

import org.hawkular.metrics.api.jaxrs.validation.Validate;
import org.hawkular.metrics.api.jaxrs.validation.Validator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wordnik.swagger.annotations.ApiModel;

/**
 * A container for data points for a single counter metric.
 *
 * @author jsanda
 */
@ApiModel(description = "An counter metric with one or more data points")
public class Counter implements Validator {

    @JsonProperty
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY)
    private String id;

    @JsonProperty
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY)
    private List<CounterDataPoint> data;

    public String getId() {
        return id;
    }

    public List<CounterDataPoint> getData() {
        return unmodifiableList(data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Counter gauge = (Counter) o;
        return Objects.equals(id, gauge.id) &&
                Objects.equals(data, gauge.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, data);
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("id", id)
                .add("data", data)
                .toString();
    }

    @Override
    @JsonIgnore
    @org.codehaus.jackson.annotate.JsonIgnore
    public boolean isValid() {
        return Validate.validate(this.getData()).toBlocking().lastOrDefault(false);
    }

}
