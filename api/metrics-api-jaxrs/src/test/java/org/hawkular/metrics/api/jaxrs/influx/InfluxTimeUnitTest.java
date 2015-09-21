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
package org.hawkular.metrics.api.jaxrs.influx;

import static org.hawkular.metrics.api.jaxrs.influx.InfluxTimeUnit.DAYS;
import static org.hawkular.metrics.api.jaxrs.influx.InfluxTimeUnit.HOURS;
import static org.hawkular.metrics.api.jaxrs.influx.InfluxTimeUnit.MICROSECONDS;
import static org.hawkular.metrics.api.jaxrs.influx.InfluxTimeUnit.MINUTES;
import static org.hawkular.metrics.api.jaxrs.influx.InfluxTimeUnit.SECONDS;
import static org.hawkular.metrics.api.jaxrs.influx.InfluxTimeUnit.WEEKS;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class InfluxTimeUnitTest {

    @Test
    public void testConvertTo() throws Exception {
        assertEquals(5 * 7 * 24, WEEKS.convertTo(TimeUnit.HOURS, 5));
        assertEquals(48, DAYS.convertTo(TimeUnit.HOURS, 2));
        assertEquals(4 * 3600, HOURS.convertTo(TimeUnit.SECONDS, 4));
        assertEquals(5, MINUTES.convertTo(TimeUnit.DAYS, 60 * 24 * 5));
        assertEquals(7, SECONDS.convertTo(TimeUnit.HOURS, 3600 * 7));
        assertEquals(13 * 1000, MICROSECONDS.convertTo(TimeUnit.NANOSECONDS, 13));
    }

    @Test
    public void testConvert() throws Exception {
        assertEquals(5 * 7 * 24, HOURS.convert(5, WEEKS));
        assertEquals(48, HOURS.convert(2, DAYS));
        assertEquals(4 * 3600, SECONDS.convert(4, HOURS));
        assertEquals(5, DAYS.convert(60 * 24 * 5, MINUTES));
        assertEquals(7, HOURS.convert(3600 * 7, SECONDS));
        assertEquals(42, WEEKS.convert(42 * 7 * 24 * 3600, SECONDS));
    }
}
