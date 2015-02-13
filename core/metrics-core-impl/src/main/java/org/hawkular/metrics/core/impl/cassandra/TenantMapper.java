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

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.google.common.base.Function;

import org.hawkular.metrics.core.api.AggregationTemplate;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Tenant;

/**
 * @author John Sanda
 */
public class TenantMapper implements Function<ResultSet, Tenant> {

    @Override
    public Tenant apply(ResultSet resultSet) {
        if (resultSet.isExhausted()) {
            return null;
        }
        Row row = resultSet.one();
        Tenant tenant = new Tenant().setId(row.getString(0));
        Map<TupleValue, Integer> retentions = row.getMap(1, TupleValue.class, Integer.class);
        for (Map.Entry<TupleValue, Integer> entry : retentions.entrySet()) {
            MetricType metricType = MetricType.fromCode(entry.getKey().getInt(0));
            if (entry.getKey().isNull(1)) {
                tenant.setRetention(metricType, entry.getValue());
            } else {
                Interval interval = Interval.parse(entry.getKey().getString(1));
                tenant.setRetention(metricType, interval, entry.getValue());
            }
        }

        List<UDTValue> templateValues = row.getList(2, UDTValue.class);
        for (UDTValue value : templateValues) {
            tenant.addAggregationTemplate(new AggregationTemplate()
                .setType(MetricType.fromCode(value.getInt("type")))
                .setInterval(Interval.parse(value.getString("interval")))
                .setFunctions(value.getSet("fns", String.class)));
        }

        return tenant;
    }
}
