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
package org.hawkular.metrics.clients.ptrans.util;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hawkular.metrics.clients.ptrans.util.Arguments.checkArgument;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ArgumentsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldThrowIllegalArgumentException() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("expected message: 3"));
        checkArgument(false, "expected message: 3");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWithFormattedMessage() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("expected message: 3"));
        checkArgument(false, "%s %s: %d", "expected", "message", 3);
    }

    @Test
    public void shouldNotThrowIllegalArgumentException() {
        checkArgument(true, "should pass");
    }
}
