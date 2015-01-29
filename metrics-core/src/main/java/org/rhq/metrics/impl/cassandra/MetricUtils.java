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
package org.rhq.metrics.impl.cassandra;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * @author John Sanda
 */
public class MetricUtils {

    public static Map<String, Optional<String>> getTags(Map<String, String> map) {
        return map.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, e -> getOptional(e.getValue())));
    }

    public static Map<String, String> flattenTags(Map<String, Optional<String>> tags) {
        return tags.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().orElse("")));
    }

    public static Map<String, Optional<String>> decodeTags(String tags) {
        return Arrays.stream(tags.split(","))
            .map(s -> s.split(":"))
            .collect(toMap(a -> a[0], a -> getOptional(a[1])));
    }

    private static Optional<String> getOptional(String s) {
        return s == null || s.isEmpty() ? empty() : of(s);
    }

}
