/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.metrics.api.jaxrs.influx.param;

import javax.ws.rs.ext.ParamConverter;

import org.hawkular.metrics.api.jaxrs.influx.InfluxTimeUnit;

/**
 * A JAX-RS {@link ParamConverter} for {@link InfluxTimeUnit} parameters.
 *
 * @author Thomas Segismont
 * @deprecated as of 0.17
 */
@Deprecated
public class InfluxTimeUnitConverter implements ParamConverter<InfluxTimeUnit> {

    @Override
    public InfluxTimeUnit fromString(String value) {
        InfluxTimeUnit timeUnit = InfluxTimeUnit.findById(value);
        if (timeUnit == null) {
            throw new IllegalArgumentException(value + "does not represent a time unit");
        }
        return timeUnit;
    }

    @Override
    public String toString(InfluxTimeUnit value) {
        return String.valueOf(value.getId());
    }
}
