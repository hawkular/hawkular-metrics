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

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.Objects;

import org.hawkular.metrics.api.jaxrs.fasterxml.jackson.MetricTypeDeserializer;
import org.hawkular.metrics.api.jaxrs.fasterxml.jackson.MetricTypeSerializer;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Inclusion;
import com.wordnik.swagger.annotations.ApiModel;

/**
 * @author John Sanda
 */
@ApiModel(description = "The definition of a metric to create")
public class MetricDefinition<T> {
    private final String tenantId;
    private final String id;
    private final Map<String, String> tags;
    private final Integer dataRetention;
    private final MetricType<T> type;

    @JsonCreator(mode = Mode.PROPERTIES)
    @org.codehaus.jackson.annotate.JsonCreator
    @SuppressWarnings("unused")
    public MetricDefinition(
            @JsonProperty("id")
            @org.codehaus.jackson.annotate.JsonProperty("id")
            String id,
            @JsonProperty(value = "tags")
            @org.codehaus.jackson.annotate.JsonProperty("tags")
            Map<String, String> tags,
            @JsonProperty("dataRetention")
            @org.codehaus.jackson.annotate.JsonProperty("dataRetention")
            Integer dataRetention,
            @JsonProperty("type")
            @org.codehaus.jackson.annotate.JsonProperty("type")
            @JsonDeserialize(using = MetricTypeDeserializer.class)
            @org.codehaus.jackson.map.annotate.JsonDeserialize(
                    using = org.hawkular.metrics.api.jaxrs.codehaus.jackson.MetricTypeDeserializer.class
            )
            MetricType<T> type
    ) {
        checkArgument(id != null, "Metric id is null");
        this.tenantId = null;
        this.id = id;
        this.tags = tags == null ? emptyMap() : unmodifiableMap(tags);
        this.dataRetention = dataRetention;
        this.type = type;
    }

    public MetricDefinition(Metric<T> metric) {
        this.tenantId = metric.getId().getTenantId();
        this.id = metric.getId().getName();
        this.type = metric.getId().getType();
        this.tags = metric.getTags();
        this.dataRetention = metric.getDataRetention();
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getId() {
        return id;
    }

    @JsonSerialize(include = Inclusion.NON_EMPTY)
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY
    )
    public Map<String, String> getTags() {
        return tags;
    }

    public Integer getDataRetention() {
        return dataRetention;
    }

    @JsonSerialize(using = MetricTypeSerializer.class)
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            using = org.hawkular.metrics.api.jaxrs.codehaus.jackson.MetricTypeSerializer.class
    )
    public MetricType<T> getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetricDefinition<?> gauge = (MetricDefinition<?>) o;
        return Objects.equals(id, gauge.getId()) && Objects.equals(type, gauge.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("tenantId", tenantId)
                .add("id", id)
                .add("tags", tags)
                .add("dataRetention", dataRetention)
                .add("type", type)
                .omitNullValues()
                .toString();
    }
}
