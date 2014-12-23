/*
 * Copyright 2014 Red Hat, Inc.
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

package org.rhq.metrics.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * @author John Sanda
 */
public class RawMetricMapper {

    public RawNumericMetric map(Row row) {
        Map<Integer, Double> map = row.getMap(2, Integer.class, Double.class);
        return new RawNumericMetric(row.getString(0), map.get(DataType.RAW.ordinal()), row.getDate(1).getTime());
    }

    public List<RawNumericMetric> map(ResultSet resultSet) {
        List<RawNumericMetric> metrics = new ArrayList<RawNumericMetric>();
        for (Row row : resultSet) {
            metrics.add(map(row));
        }
        return metrics;
    }

}
