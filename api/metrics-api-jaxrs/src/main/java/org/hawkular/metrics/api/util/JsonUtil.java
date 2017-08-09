/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.api.util;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Json serialization/deserialization utility for Alerts (or Events) using Jackson implementation.
 *
 * @author Lucas Ponce
 */
public class JsonUtil {

    private static JsonUtil instance = new JsonUtil();
    private ObjectMapper mapper;
    private ObjectMapper mapperThin;

    private JsonUtil() {
        mapper = new ObjectMapper();

        SimpleModule simpleModule = new SimpleModule();
        //simpleModule.setDeserializerModifier(new JacksonDeserializer.AlertThinDeserializer());
        mapperThin = new ObjectMapper();
        mapperThin.registerModule(simpleModule);
    }

    public static String toJson(Object resource) {
        try {
            return instance.mapper.writeValueAsString(resource);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        return fromJson(json, clazz, false);
    }

    public static <T> Collection<T> collectionFromJson(String json, Class<T> clazz) {
        try {
            return instance.mapper.readValue(json,
                    instance.mapper.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz, boolean thin) {
        try {
            return thin ? instance.mapperThin.readValue(json, clazz) : instance.mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> getMap(Object o) {
        return instance.mapper.convertValue(o, Map.class);
    }

    public static ObjectMapper getMapper() {
        return instance.mapper;
    }
}