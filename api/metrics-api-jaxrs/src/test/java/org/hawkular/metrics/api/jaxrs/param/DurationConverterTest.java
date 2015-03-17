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
package org.hawkular.metrics.api.jaxrs.param;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * @author Thomas Segismont
 */
@RunWith(Parameterized.class)
public class DurationConverterTest {

    @Parameters(name = "{0}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
                new Object[][]{
                        {"1ms", new Duration(1, MILLISECONDS)},
                        {"22s", new Duration(22, SECONDS)},
                        {"333mn", new Duration(333, MINUTES)},
                        {"4444h", new Duration(4444, HOURS)},
                        {"55555d", new Duration(55555, DAYS)},
                }
        );
    }

    private String value;
    private Duration duration;

    public DurationConverterTest(String value, Duration duration) {
        this.value = value;
        this.duration = duration;
    }

    @Test
    public void testFromString() throws Exception {
        assertEquals(duration, new DurationConverter().fromString(value));
    }

    @Test
    public void testToString() throws Exception {
        assertEquals(value, new DurationConverter().toString(duration));
    }
}