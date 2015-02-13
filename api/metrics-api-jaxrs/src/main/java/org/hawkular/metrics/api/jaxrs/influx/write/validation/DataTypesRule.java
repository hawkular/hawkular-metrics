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
package org.hawkular.metrics.api.jaxrs.influx.write.validation;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.or;

import java.math.BigDecimal;
import java.util.List;
import java.util.ListIterator;

import com.google.common.base.Predicate;

import org.hawkular.metrics.api.jaxrs.influx.InfluxObject;

/**
 * @author Thomas Segismont
 */
public class DataTypesRule implements InfluxObjectValidationRule {
    private static final Predicate<Object> IS_INTEGRAL = //
    and( //
        instanceOf(Number.class), //
        not( //
            or( //
                instanceOf(BigDecimal.class), //
                instanceOf(Double.class), //
                instanceOf(Float.class) //
            ) //
        ) //
    );

    @Override
    public void checkInfluxObject(InfluxObject influxObject) throws InvalidObjectException {
        List<List<?>> points = influxObject.getPoints();
        if (points == null) {
            return;
        }
        List<String> columns = influxObject.getColumns();
        for (List<?> point : points) {
            if (point.size() < columns.size()) {
                throw new InvalidObjectException("Object has not enough data in point to match columns");
            }
            if (columns.size() == 1) {
                if (!(point.get(0) instanceof Number)) {
                    throw new InvalidObjectException("Point 'value' is not numerical");
                }
            } else {
                int valueIndex = columns.indexOf("value");
                for (ListIterator<?> dataIterator = point.listIterator(); dataIterator.hasNext();) {
                    Object data = dataIterator.next();
                    if (valueIndex == dataIterator.nextIndex() - 1) {
                        if (!(data instanceof Number)) {
                            throw new InvalidObjectException("Point 'value' is not numerical");
                        }
                    } else {
                        if (!IS_INTEGRAL.apply(data)) {
                            throw new InvalidObjectException("Point 'time' is not integral");
                        }
                    }
                }
            }
        }
    }
}
