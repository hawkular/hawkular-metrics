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
package org.rhq.metrics.restServlet.influx.write.validation;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.rhq.metrics.restServlet.influx.InfluxObject;

/**
 * @author Thomas Segismont
 */
public class SupportedColumnsRule implements InfluxObjectValidationRule {
    @Override
    public void checkInfluxObject(InfluxObject influxObject) throws InvalidObjectException {
        List<String> columns = influxObject.getColumns();
        if (columns == null || columns.isEmpty()) {
            throw new InvalidObjectException("Object has empty columns attribute");
        }
        if (columns.size() == 1) {
            if (!columns.contains("value")) {
                throw new InvalidObjectException("Object has no 'value' column");
            }
        } else if (columns.size() == 2) {
            if (!columns.containsAll(ImmutableList.of("time", "value"))) {
                throw new InvalidObjectException("Object has columns other than 'time' or 'value'");
            }
        } else {
            throw new InvalidObjectException("Object has more than two columns");
        }
    }
}
