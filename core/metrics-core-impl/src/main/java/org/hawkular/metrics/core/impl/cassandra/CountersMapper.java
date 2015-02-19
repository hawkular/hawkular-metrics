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

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;

import org.hawkular.metrics.core.api.Counter;

/**
 * @author John Sanda
 */
public class CountersMapper implements Function<ResultSet, List<Counter>> {

    @Override
    public List<Counter> apply(ResultSet resultSet) {
        List<Counter> counters = new ArrayList<>();
        for (Row row : resultSet) {
            counters.add(new Counter(row.getString(0), row.getString(1), row.getString(2), row.getLong(3)));
        }
        return counters;
    }
}
