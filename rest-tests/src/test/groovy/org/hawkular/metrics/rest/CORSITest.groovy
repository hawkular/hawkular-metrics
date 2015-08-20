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
package org.hawkular.metrics.rest

import static org.junit.Assert.assertEquals

import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_CREDENTIALS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_HEADERS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_METHODS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_MAX_AGE;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ACCESS_CONTROL_REQUEST_METHOD;
import static org.hawkular.metrics.api.jaxrs.util.Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS;
import static org.hawkular.metrics.api.jaxrs.util.Headers.ORIGIN;

import org.junit.Test

class CORSITest extends RESTTest {

    @Test
    void testOptionsWithOrigin() {
        def testOrigin = "TestOrigin123";

        def response = hawkularMetrics.options(path: "gauges/test/data",
         headers: [
            ACCESS_CONTROL_REQUEST_METHOD: "POST",
            ORIGIN: testOrigin
         ]);

         //print headers
         println("==== Request Headers = Start  ====");
         response.headers.each { println "${it.name} : ${it.value}" }
         println("==== Request Headers = End    ====");

         //Expected a 200 because this is be a pre-flight call that should never reach the resource router
         assertEquals(200, response.status)
         assertEquals(DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS, response.headers[ACCESS_CONTROL_ALLOW_METHODS].value);
         assertEquals(DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS, response.headers[ACCESS_CONTROL_ALLOW_HEADERS].value);
         assertEquals(testOrigin, response.headers[ACCESS_CONTROL_ALLOW_ORIGIN].value);
         assertEquals("true", response.headers[ACCESS_CONTROL_ALLOW_CREDENTIALS].value);
         assertEquals((72 * 60 * 60)+"", response.headers[ACCESS_CONTROL_MAX_AGE].value);
    }

    @Test
    void testOptionsWithoutOrigin() {
        def response = hawkularMetrics.options(path: "gauges/test/data",
         headers: [
            ACCESS_CONTROL_REQUEST_METHOD: "GET",
         ]);

         //print headers
         println("==== Request Headers = Start  ====");
         response.headers.each { println "${it.name} : ${it.value}" }
         println("==== Request Headers = End    ====");

         //Expected a 200 because this is be a pre-flight call that should never reach the resource router
         assertEquals(200, response.status)
         assertEquals(DEFAULT_CORS_ACCESS_CONTROL_ALLOW_METHODS, response.headers[ACCESS_CONTROL_ALLOW_METHODS].value);
         assertEquals(DEFAULT_CORS_ACCESS_CONTROL_ALLOW_HEADERS, response.headers[ACCESS_CONTROL_ALLOW_HEADERS].value);
         assertEquals("*", response.headers[ACCESS_CONTROL_ALLOW_ORIGIN].value);
         assertEquals("true", response.headers[ACCESS_CONTROL_ALLOW_CREDENTIALS].value);
         assertEquals((72 * 60 * 60)+"", response.headers[ACCESS_CONTROL_MAX_AGE].value);
    }
}
