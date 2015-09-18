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
package org.hawkular.metrics.core.impl;

import java.util.HashSet;
import java.util.Set;

import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * @author Stefan Negrea
 */
public class TaggedMetricMapper {

    public static <T> Set<MetricId<T>> apply(ResultSet resultSet, MetricType<T> type) {
        Set<MetricId<T>> taggedMetrics = new HashSet<>();
        for (Row row : resultSet) {
            taggedMetrics.add(new MetricId<T>(row.getString(0), type, row.getString(2),
                    Interval.parse(row.getString(3))));
        }
        return taggedMetrics;
    }
}
