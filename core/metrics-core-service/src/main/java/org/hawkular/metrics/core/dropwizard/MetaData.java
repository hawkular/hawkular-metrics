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
package org.hawkular.metrics.core.dropwizard;

import static java.util.stream.Collectors.toMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Each metric that we collect store a standard set of tags:
 *
 * <ul>
 *     <li>hostname</li>
 *     <li>namespace</li>
 *     <li>name</li>
 *     <li>scope</li>
 *     <li>type</li>
 * </ul>
 *
 * Additional tags can be supplied as well. For more details on the standard meta data please see
 * https://issues.jboss.org/browse/HWKMETRICS-668.
 *
 * @author jsanda
 */
public class MetaData {

    private static final String HAWKULAR_METRICS_NAMESPACE = "org.hawkular.metrics";

    private static final String NAMESPACE_PROPERTY = "namespace";

    private static final String NAME_PROPERTY = "name";

    private static final String SCOPE_PROPERTY = "scope";

    private static final String TYPE_PROPERTY = "type";

    private static final String HOSTNAME_PROPERTY = "hostname";

    private static final Set<String> REQUIRED_PROPERTIES = ImmutableSet.of(NAME_PROPERTY, SCOPE_PROPERTY,
            NAMESPACE_PROPERTY, TYPE_PROPERTY, HOSTNAME_PROPERTY);

    private Map<String, String> tags = new HashMap<>();

    public MetaData(String name, String scope, String type, String hostname) {
        this(name, scope, type, hostname, Collections.emptyMap());
    }

    public MetaData(String name, String scope, String type, String hostname, Map<String, String> optional) {
        tags.put(NAMESPACE_PROPERTY, HAWKULAR_METRICS_NAMESPACE);
        tags.put(NAME_PROPERTY, name);
        tags.put(SCOPE_PROPERTY, scope);
        tags.put(TYPE_PROPERTY, type);
        tags.put(HOSTNAME_PROPERTY, hostname);
        tags.putAll(optional);
    }

    public String getNamespace() {
        return tags.get(NAMESPACE_PROPERTY);
    }

    public String getName() {
        return tags.get(NAME_PROPERTY);
    }

    public String getScope() {
        return tags.get(SCOPE_PROPERTY);
    }

    public String getType() {
        return tags.get(TYPE_PROPERTY);
    }

    public String getHostname() {
        return tags.get(HOSTNAME_PROPERTY);
    }

    public Map<String, String> getTags() {
        return ImmutableMap.copyOf(tags);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetaData metaData = (MetaData) o;
        return Objects.equals(tags, metaData.tags);

    }

    @Override
    public int hashCode() {
        return Objects.hash(tags);
    }

    @Override public
    String toString() {
        Map<String, String> optional = tags.entrySet().stream()
                .filter(e -> !REQUIRED_PROPERTIES.contains(e.getKey()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        return MoreObjects.toStringHelper(this)
                .add("namespace", getNamespace())
                .add("hostname", getHostname())
                .add("name", getName())
                .add("scope", getScope())
                .add("type", getType())
                .add("optional", optional)
                .toString();
    }
}
