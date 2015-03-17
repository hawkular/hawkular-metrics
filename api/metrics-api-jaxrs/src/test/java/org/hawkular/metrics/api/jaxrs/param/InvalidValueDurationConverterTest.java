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

import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * @author Thomas Segismont
 */
@RunWith(Parameterized.class)
public class InvalidValueDurationConverterTest {

    @Parameters(name = "{0}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
                new Object[][]{
                        {"jdklsqj 0484d8sq"},
                        {"1u"},
                        {"7du"},
                        {"45640894809"},
                        {"ms"},
                }
        );
    }

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private String value;

    public InvalidValueDurationConverterTest(String value) {
        this.value = value;
    }


    @Test
    public void testFromString() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        //noinspection ResultOfMethodCallIgnored
        new DurationConverter().fromString(value);
    }
}