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
package org.hawkular.metrics.sysconfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jsanda
 */
public class Configuration {

    private String id;

    private Map<String, String> properties;

    public Configuration(String id) {
        this.id = id;
        properties = new HashMap<>();
    }

    public Configuration(String id, Map<String, String> properties) {
        this.id = id;
        this.properties = properties;
    }

    public String getId() {
        return id;
    }

    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public String get(String name) {
        return properties.get(name);
    }

    public String get(String name, String defaultValue) {
        return properties.getOrDefault(name, defaultValue);
    }

    public void set(String name, String value) {
        properties.put(name, value);
    }

    public void delete(String name) {
        properties.remove(name);
    }
}
