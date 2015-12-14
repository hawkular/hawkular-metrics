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
package org.hawkular.metrics.core.api.param;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

/**
 * Tags holder. This class is meant to be used only as a JAX−RS method parameter.
 *
 * @author Thomas Segismont
 */
@org.codehaus.jackson.map.annotate.JsonSerialize(
        include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY)
public class Tags {
    public static final String LIST_DELIMITER = ",";
    public static final String TAG_DELIMITER = ":";

    private final Map<String, String> tags;

    /**
     * Null or blank names or values are not permitted. Names and values can't have
     *
     * @param tags values as a {@link Map}
     */
    public Tags(Map<String, String> tags) {
        checkArgument(tags != null, "tags is null");
        Stream<Map.Entry<String, String>> entryStream = tags.entrySet().stream();
        checkArgument(entryStream.allMatch(Tags::isValid), "Invalid tag name or value: %s", tags);
        this.tags = ImmutableMap.copyOf(tags);
    }

    private static boolean isValid(Map.Entry<String, String> tag) {
        return isValid(tag.getKey()) && isValid(tag.getValue());
    }

    private static boolean isValid(String s) {
        return s != null && !s.trim().isEmpty() && !s.contains(LIST_DELIMITER) && !s.contains(TAG_DELIMITER);
    }

    /**
     * @return tag values as a {@link Map}
     */
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tags tags1 = (Tags) o;
        return tags.equals(tags1.tags);

    }

    @Override
    public int hashCode() {
        return tags.hashCode();
    }

    @Override
    public String toString() {
        return "Tags[" +
               "tags=" + tags +
               ']';
    }
}
