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
package org.hawkular.metrics.api.jaxrs.influx.query.parse.definition;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class InfluxTimeUnitTest {

    @Test
    public void testConvertTo() throws Exception {
        assertEquals(5, InfluxTimeUnit.MINUTES.convertTo(TimeUnit.DAYS, 60 * 24 * 5));
        assertEquals(5, InfluxTimeUnit.MINUTES.convertTo(TimeUnit.DAYS, 60 * 24 * 5));
        assertEquals(5, InfluxTimeUnit.MINUTES.convertTo(TimeUnit.DAYS, 60 * 24 * 5));
        assertEquals(5, InfluxTimeUnit.MINUTES.convertTo(TimeUnit.DAYS, 60 * 24 * 5));
        assertEquals(7, InfluxTimeUnit.SECONDS.convertTo(TimeUnit.HOURS, 3600 * 7));
        assertEquals(13 * 1000, InfluxTimeUnit.MICROSECONDS.convertTo(TimeUnit.NANOSECONDS, 13));
    }
}
