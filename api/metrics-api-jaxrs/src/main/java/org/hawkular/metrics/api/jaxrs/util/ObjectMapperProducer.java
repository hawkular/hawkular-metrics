/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.api.jaxrs.util;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.fasterxml.jackson.AvailabilityTypeDeserializer;
import org.hawkular.metrics.model.fasterxml.jackson.AvailabilityTypeKeySerializer;
import org.hawkular.metrics.model.fasterxml.jackson.AvailabilityTypeSerializer;
import org.hawkular.metrics.model.fasterxml.jackson.MetricTypeDeserializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * @author jsanda
 */

@ApplicationScoped
public class ObjectMapperProducer {

    private ObjectMapper mapper;

    @PostConstruct
    public void initMapper() {
        mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(AvailabilityType.class, new AvailabilityTypeDeserializer());
        module.addDeserializer(MetricType.class, new MetricTypeDeserializer());
        module.addSerializer(AvailabilityType.class, new AvailabilityTypeSerializer());
        module.addKeySerializer(AvailabilityType.class, new AvailabilityTypeKeySerializer());
        mapper.registerModule(module);
    }

    @Produces
    public ObjectMapper getMapper() {
        return mapper;
    }

}
