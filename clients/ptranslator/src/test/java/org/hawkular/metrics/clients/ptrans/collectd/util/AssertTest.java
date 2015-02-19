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
package org.hawkular.metrics.clients.ptrans.collectd.util;

import static org.hamcrest.core.IsEqual.equalTo;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AssertTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void notNullShouldThrowIllegalArgumentException() {
        String msg = "expected message";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo(msg));
        Assert.assertNotNull(null, msg);
    }

    @Test
    public void notNullShouldThrowIllegalArgumentExceptionWithFormattedMessage() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("expected message"));
        Assert.assertNotNull(null, "%s %s", "expected", "message");
    }

    @Test
    public void notNullShouldThrowNothing() {
        Assert.assertNotNull(new Object(), "should pass");
    }

    @Test
    public void equalsShouldThrowIllegalArgumentException() {
        String msg = "expected message";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo(msg));
        Assert.assertEquals(1, 2, msg);
    }

    @Test
    public void equalsShouldThrowIllegalArgumentExceptionWithFormattedMessage() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("expected message"));
        Assert.assertEquals(1, 2, "%s %s", "expected", "message");
    }

    @Test
    public void equalsShouldThrowNothing() {
        Assert.assertEquals(1, 1, "should pass");
    }
}