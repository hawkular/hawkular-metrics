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
package org.hawkular.metrics.model.param;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * Tag names holder. This class is meant to be used only as a JAX−RS method parameter.
 *
 * @author Thomas Segismont
 */
public class TagNames {
    private final Set<String> names;

    /**
     * Null or blank names are not permitted.
     */
    public TagNames(Set<String> names) {
        checkArgument(names != null, "names is null");
        checkArgument(names.stream().allMatch(Tags::isValid), "Invalid tag name: %s", names);
        this.names = ImmutableSet.copyOf(names);
    }

    @SuppressWarnings("unused")
    private static boolean isValid(Map.Entry<String, String> tag) {
        return isValid(tag.getKey()) && isValid(tag.getValue());
    }

    private static boolean isValid(String s) {
        return s != null && !s.trim().isEmpty();
    }

    /**
     * @return tag names as a {@link Map}
     */
    public Set<String> getNames() {
        return names;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TagNames tagNames = (TagNames) o;
        return names.equals(tagNames.names);

    }

    @Override
    public int hashCode() {
        return names.hashCode();
    }

    @Override
    public String toString() {
        return "TagNames{" +
                "names=" + names +
                '}';
    }
}
