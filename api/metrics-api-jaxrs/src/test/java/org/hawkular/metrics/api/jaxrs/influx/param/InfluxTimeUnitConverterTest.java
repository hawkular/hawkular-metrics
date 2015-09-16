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

package org.hawkular.metrics.api.jaxrs.influx.param;

import static java.util.stream.Collectors.joining;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.hawkular.metrics.api.jaxrs.influx.InfluxTimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author Thomas Segismont
 */
public class InfluxTimeUnitConverterTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final InfluxTimeUnitConverter converter = new InfluxTimeUnitConverter();

    @Test
    public void shouldFailIfTextIsAnInvalidUnit() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        converter.fromString(Arrays.stream(InfluxTimeUnit.values()).map(unit -> unit.getId()).collect(joining(",")));
    }

    @Test
    public void shouldConvertValidTimeUnits() throws Exception {
        for (InfluxTimeUnit timeUnit : InfluxTimeUnit.values()) {
            assertEquals(timeUnit, converter.fromString(String.valueOf(timeUnit.getId())));
        }
    }
}