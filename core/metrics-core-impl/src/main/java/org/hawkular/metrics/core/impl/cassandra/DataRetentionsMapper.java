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

import java.util.HashSet;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.Retention;

/**
 * @author John Sanda
 */
public class DataRetentionsMapper implements Function<ResultSet, Set<Retention>> {

    @Override
    public Set<Retention> apply(ResultSet resultSet) {
        Set<Retention> dataRetentions = new HashSet<>();
        for (Row row : resultSet) {
            dataRetentions.add(new Retention(new MetricId(row.getString(3), Interval.parse(row.getString(2))),
                row.getInt(4)));
        }
        return dataRetentions;
    }
}
