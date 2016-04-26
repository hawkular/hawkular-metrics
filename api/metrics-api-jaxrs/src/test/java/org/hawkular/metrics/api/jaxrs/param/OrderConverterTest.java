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
package org.hawkular.metrics.api.jaxrs.param;

import static java.util.stream.Collectors.joining;

import static org.hawkular.metrics.core.service.Order.ASC;
import static org.hawkular.metrics.core.service.Order.DESC;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.hawkular.metrics.core.service.Order;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author Thomas Segismont
 */
public class OrderConverterTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private OrderConverter orderConverter = new OrderConverter();

    @Test
    public void shouldIgnoreCase() {
        assertEquals(ASC, orderConverter.fromString("aSc"));
        assertEquals(DESC, orderConverter.fromString("DEsc"));
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWithInvalidText() throws Exception {
        expectedException.expect(IllegalArgumentException.class);

        String invalidText = Arrays.stream(Order.values()).map(Order::toString).collect(joining("."));
        orderConverter.fromString(invalidText);
    }
}