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
package org.hawkular.metrics.core.impl.cassandra;

import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.Map;

/**
 * @author John Sanda
 */
public class MetricUtils {

    public static Map<String, String> decodeTags(String tags) {
        return Arrays.stream(tags.split(","))
            .map(s -> s.split(":"))
            .collect(toMap(a -> a[0], a -> a[1]));
    }
}
