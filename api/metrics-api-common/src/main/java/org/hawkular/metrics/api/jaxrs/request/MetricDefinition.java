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
package org.hawkular.metrics.api.jaxrs.request;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

import java.util.Map;
import java.util.Objects;

import org.hawkular.metrics.api.jaxrs.jackson.MetricTypeDeserializer;
import org.hawkular.metrics.api.jaxrs.jackson.MetricTypeSerializer;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.wordnik.swagger.annotations.ApiModel;

/**
 * @author jsanda
 */
@ApiModel(description = "The definition of a metric to create")
public class MetricDefinition<T> {

    // TODO Do we need this?
    @JsonProperty
    @org.codehaus.jackson.annotate.JsonProperty
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY)
    private String tenantId;

    @JsonProperty
    @org.codehaus.jackson.annotate.JsonProperty
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY)
    private String id;

    @JsonProperty
    @org.codehaus.jackson.annotate.JsonProperty
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY)
    private Map<String, String> tags;

    @JsonProperty
    @org.codehaus.jackson.annotate.JsonProperty
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY)
    private Integer dataRetention;

    @JsonProperty
    @org.codehaus.jackson.annotate.JsonProperty
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY)
    @JsonSerialize(using = MetricTypeSerializer.class)
    @JsonDeserialize(using = MetricTypeDeserializer.class)
    private MetricType<T> type;

    public MetricDefinition() {
    }

    @JsonCreator
    public MetricDefinition(

            @JsonProperty("id") String id,
            @JsonProperty(value = "tags") Map<String, String> tags,
            @JsonProperty("dataRetention") Integer dataRetention,
            @JsonProperty("type") MetricType<T> type) {
        this.id = id;
        this.tags = tags == null ? emptyMap() : unmodifiableMap(tags);
        this.dataRetention = dataRetention;
        this.type = type;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getId() {
        return id;
    }

    public Map<String, String> getTags() {
        if (tags == null) {
            return emptyMap();
        }
        return tags;
    }

    public Integer getDataRetention() {
        return dataRetention;
    }

    public MetricType<T> getType() {
        return type;
    }

    @SuppressWarnings("unchecked")
    public MetricDefinition(Metric metric) {
        this.tenantId = metric.getId().getTenantId();
        this.id = metric.getId().getName();
        this.type = metric.getId().getType();
        this.tags = metric.getTags();
        this.dataRetention = metric.getDataRetention();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricDefinition<?> gauge = (MetricDefinition<?>) o;
        return Objects.equals(id, gauge.getId()) && Objects.equals(type, gauge.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "MetricDefinition{" +
                "tenantId='" + tenantId + '\'' +
                ", id='" + id + '\'' +
                ", tags=" + tags +
                ", dataRetention=" + dataRetention +
                ", type=" + type.toString() +
                '}';
    }
}
