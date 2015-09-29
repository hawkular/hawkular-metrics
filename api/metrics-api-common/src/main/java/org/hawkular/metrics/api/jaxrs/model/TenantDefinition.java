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

import org.hawkular.metrics.api.jaxrs.fasterxml.jackson.MetricTypeKeyDeserializer;
import org.hawkular.metrics.api.jaxrs.fasterxml.jackson.MetricTypeKeySerializer;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Tenant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Inclusion;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * @author John Sanda
 */
@ApiModel(value = "Tenant", description = "The definition of a tenant")
public class TenantDefinition {
    private final String id;
    private final Map<MetricType<?>, Integer> retentionSettings;

    @JsonCreator(mode = Mode.PROPERTIES)
    @org.codehaus.jackson.annotate.JsonCreator
    public TenantDefinition(
            @JsonProperty("id")
            @org.codehaus.jackson.annotate.JsonProperty("id")
            String id,
            @JsonProperty("retentions")
            @JsonDeserialize(keyUsing = MetricTypeKeyDeserializer.class)
            @org.codehaus.jackson.annotate.JsonProperty("retentions")
            Map<MetricType<?>, Integer> retentionSettings) {
        checkArgument(id != null, "Tenant id is null");
        this.id = id;
        this.retentionSettings = retentionSettings == null ? emptyMap() : unmodifiableMap(retentionSettings);
    }

    public TenantDefinition(Tenant tenant) {
        id = tenant.getId();
        retentionSettings = tenant.getRetentionSettings();
    }

    @ApiModelProperty(value = "Identifier of the tenant", required = true)
    public String getId() {
        return id;
    }

    @ApiModelProperty("Retention settings for metrics, expressed in days")
    @JsonProperty("retentions")
    @org.codehaus.jackson.annotate.JsonProperty("retentions")
    @JsonSerialize(include = Inclusion.NON_EMPTY, keyUsing = MetricTypeKeySerializer.class)
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY,
            keyUsing = org.hawkular.metrics.api.jaxrs.codehaus.jackson.MetricTypeKeySerializer.class
    )
    // Codehaus Jackson wants @JsonDeserialize here
    @org.codehaus.jackson.map.annotate.JsonDeserialize(
            keyUsing = org.hawkular.metrics.api.jaxrs.codehaus.jackson.MetricTypeKeyDeserializer.class
    )
    public Map<MetricType<?>, Integer> getRetentionSettings() {
        return retentionSettings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TenantDefinition that = (TenantDefinition) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("id", id)
                .omitNullValues()
                .toString();
    }

    public Tenant toTenant() {
        return new Tenant(id, retentionSettings);
    }
}
