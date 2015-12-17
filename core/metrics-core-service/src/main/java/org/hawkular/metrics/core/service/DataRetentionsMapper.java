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
package org.hawkular.metrics.core.service;

import java.util.HashSet;
import java.util.Set;

import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.Retention;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

/**
 * @author John Sanda
 */
public class DataRetentionsMapper implements Function<ResultSet, Set<Retention>> {
    private final String tenantId;
    private final MetricType<?> type;

    public DataRetentionsMapper(String tenantId, MetricType<?> type) {
        this.tenantId = tenantId;
        this.type = type;
    }

    @Override
    public Set<Retention> apply(ResultSet resultSet) {
        Set<Retention> dataRetentions = new HashSet<>();
        for (Row row : resultSet) {
            dataRetentions.add(new Retention(new MetricId<>(tenantId, type, row.getString(2)), row.getInt(3)));
        }
        return dataRetentions;
    }
}
