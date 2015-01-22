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
package org.rhq.metrics.restServlet.influx.query.parse.definition;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class InfluxTimeUnitTest {

    @Test
    public void testConvertTo() throws Exception {
        assertThat(InfluxTimeUnit.WEEKS.convertTo(TimeUnit.HOURS, 5)).isEqualTo(5 * 7 * 24);
        assertThat(InfluxTimeUnit.DAYS.convertTo(TimeUnit.HOURS, 2)).isEqualTo(48);
        assertThat(InfluxTimeUnit.HOURS.convertTo(TimeUnit.SECONDS, 4)).isEqualTo(4 * 3600);
        assertThat(InfluxTimeUnit.MINUTES.convertTo(TimeUnit.DAYS, 60 * 24 * 5)).isEqualTo(5);
        assertThat(InfluxTimeUnit.SECONDS.convertTo(TimeUnit.HOURS, 3600 * 7)).isEqualTo(7);
        assertThat(InfluxTimeUnit.MICROSECONDS.convertTo(TimeUnit.NANOSECONDS, 13)).isEqualTo(13 * 1000);
    }
}
